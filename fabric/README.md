# Fabric items

Workspace items in the folder format used by Fabric Git integration (`NAME.Type/` with the
item's definition files). The university tenant does not allow Git integration with GitHub, so
they are deployed and exported with [`tools/fabric_sync.py`](../tools/fabric_sync.py).

Ids are written as placeholders (`{{workspace}}`, `{{item:lh_bronze.Lakehouse}}`,
`{{connection:nyc_tlc_cloudfront}}`) and resolved on deploy, so the same files can be deployed
to any workspace named in `config/fabric.json`.

| Item | What it does |
|---|---|
| `connections/nyc_tlc_cloudfront.json` | anonymous HTTP connection to the NYC TLC file server |
| `pl_ingest_taxi.DataPipeline` | reads `Files/config/sources.json`, runs `pl_copy_taxi_file` for every (dataset, month) pair, max 4 in parallel, and refreshes the zone reference files |
| `pl_copy_taxi_file.DataPipeline` | copies one monthly Parquet file to `Files/taxi/{dataset}/year=YYYY/month=MM/`; skips the copy when the file already exists, so re-runs do not duplicate data |
| `df_ecb_fx.Dataflow` | ECB daily USD/EUR rate (CSV, full history) → table `bronze_ecb_fx`, all columns kept as published |
| `df_worldbank_gdp.Dataflow` | World Bank yearly GDP in USD (JSON, all pages) → table `bronze_worldbank_gdp` |
| `df_openaq.Dataflow` | OpenAQ v3: sensors in the NYC bounding box → `bronze_openaq_locations`; daily PM2.5 / NO2 / O3 summaries per sensor, paginated (1000 rows per page) and throttled to stay under 60 requests/min → `bronze_openaq_daily` |

| `nb_silver_taxi.Notebook` | all monthly taxi files → `silver_taxi_trips` (one schema, deduplicated, enriched, `dq_flags` / `is_valid`) and `silver_taxi_zones` |
| `nb_silver_air.Notebook` | OpenAQ → `silver_air_locations` (mapped to taxi zones) and `silver_air_quality_daily` (units normalised, flagged) |
| `nb_silver_econ.Notebook` | `silver_fx_daily` (every day, gaps filled with the last rate) and `silver_gdp_yearly` (with growth) |
| `nb_dim_date.Notebook` | `silver_dim_date`: calendar 2015–2027 with weekends, seasons and US / NY holidays |

Every Bronze table also has `_ingested_at` (UTC load time) and `_source`. Every Silver notebook
writes its row counts and check results to `silver_dq_log`. Column descriptions are in
[docs/DATA_DICTIONARY.md](../docs/DATA_DICTIONARY.md).

Notebooks reach both lakehouses through OneLake paths built from the placeholders, so they need no
default lakehouse. Run one with `fabric_sync.py run nb_silver_taxi.Notebook --wait`; the latest
metrics are in `lh_silver/Files/dq/` (`fabric_sync.py cat lh_silver.Lakehouse Files/dq/<file>.json`). Dataflow destinations use
the automatic (replace) mode, so a refresh reloads the table and never duplicates rows.

## Secrets

The OpenAQ API key is never in Git. It lives in `.env` in the project root (ignored by Git):

```
OPENAQ_API_KEY=...
```

`mashup.pq` contains the placeholder `{{secret:OPENAQ_API_KEY}}`; `fabric_sync.py deploy` fills the
value in, `export` puts the placeholder back and refuses to write a file that still contains the
key. `python tools/local_secrets.py install-hook` installs a pre-commit hook that blocks any commit
containing a value from `.env`. In Fabric the key is a hidden dataflow parameter, so only members
of the workspace can see it.

## Connections

`nyc_tlc_cloudfront` is created from `connections/` with `fabric_sync.py connect`. The dataflow
connections (`ecb_data_api`, `worldbank_api`, `openaq_api`: anonymous Web; `fabric_lakehouse`: the
signed-in user) are created once in the dataflow editor ("Configure connection"), because a
Lakehouse connection needs the user's own sign-in.

## Setup from scratch

```powershell
python -m venv .venv
.venv\Scripts\pip install -r tools\requirements.txt
.venv\Scripts\python tools\fabric_sync.py login --device-code
.venv\Scripts\python tools\fabric_sync.py connect
.venv\Scripts\python tools\fabric_sync.py upload config\sources.json lh_bronze.Lakehouse Files/config/sources.json
.venv\Scripts\python tools\fabric_sync.py deploy fabric\pl_copy_taxi_file.DataPipeline fabric\pl_ingest_taxi.DataPipeline
.venv\Scripts\python tools\fabric_sync.py run pl_ingest_taxi --wait
.venv\Scripts\python tools\local_secrets.py install-hook
.venv\Scripts\python tools\fabric_sync.py deploy fabric\df_ecb_fx.Dataflow fabric\df_worldbank_gdp.Dataflow fabric\df_openaq.Dataflow
.venv\Scripts\python tools\fabric_sync.py run df_openaq.Dataflow --wait
.venv\Scripts\python tools\fabric_sync.py tables lh_bronze.Lakehouse
```

The child pipeline is deployed first because the parent references its id.

## Adding a month or a dataset

Edit `taxi_trips` in `config/sources.json` (`end_month`, or e.g. `"datasets": ["yellow", "green"]`),
upload it again and run `pl_ingest_taxi`. Only the missing files are copied.
