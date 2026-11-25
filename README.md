# NYC Urban Analytics on Microsoft Fabric

Course project (Itransition, Data Engineering group): a unified analytics platform on
Microsoft Fabric that combines urban mobility (NYC taxi trips), air quality (OpenAQ) and
macro-economic data (World Bank GDP, ECB USD/EUR rates) in a medallion lakehouse and a
star-schema warehouse, with Power BI dashboards.

## Architecture

```mermaid
flowchart LR
    subgraph Sources
        T[NYC TLC<br/>monthly Parquet]
        A[OpenAQ API<br/>JSON, paginated]
        G[World Bank API<br/>GDP JSON]
        F[ECB API<br/>FX CSV]
    end
    subgraph Fabric workspace
        B[(lh_bronze<br/>raw files & tables)]
        S[(lh_silver<br/>clean Delta tables)]
        W[(wh_gold<br/>star schema)]
        R[Power BI<br/>Direct Lake report]
    end
    T -- pipeline --> B
    A -- Dataflow Gen2 --> B
    G -- Dataflow Gen2 --> B
    F -- Dataflow Gen2 --> B
    B -- PySpark notebooks --> S
    S -- T-SQL --> W
    W --> R
```

## Repository layout

| Path | Content |
|---|---|
| `config/sources.json` | every data source in one place (months, countries, pollutants, URLs) |
| `config/fabric.json` | target Fabric workspace |
| `fabric/` | workspace items (pipelines, dataflows, notebooks) and connections, see [fabric/README.md](fabric/README.md) |
| `tools/fabric_sync.py` | deploys `fabric/` to the workspace and exports it back through the Fabric REST API |
| `sql/` | warehouse DDL, load scripts and analytical queries |
| `docs/PLAN.md` | project plan split into segments, with progress |
| `docs/DATA_DICTIONARY.md` | every Bronze and Silver table and column |
| `docs/` | lineage, governance |

## Status

See [docs/PLAN.md](docs/PLAN.md).
