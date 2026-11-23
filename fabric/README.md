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

## Setup from scratch

```powershell
python -m venv .venv
.venv\Scripts\pip install -r tools\requirements.txt
.venv\Scripts\python tools\fabric_sync.py login --device-code
.venv\Scripts\python tools\fabric_sync.py connect
.venv\Scripts\python tools\fabric_sync.py upload config\sources.json lh_bronze.Lakehouse Files/config/sources.json
.venv\Scripts\python tools\fabric_sync.py deploy fabric\pl_copy_taxi_file.DataPipeline fabric\pl_ingest_taxi.DataPipeline
.venv\Scripts\python tools\fabric_sync.py run pl_ingest_taxi --wait
```

The child pipeline is deployed first because the parent references its id.

## Adding a month or a dataset

Edit `taxi_trips` in `config/sources.json` (`end_month`, or e.g. `"datasets": ["yellow", "green"]`),
upload it again and run `pl_ingest_taxi`. Only the missing files are copied.
