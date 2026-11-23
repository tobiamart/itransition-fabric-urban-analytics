# Project plan

Work is split into segments. Each segment ends with a "done when" check, so progress is
measurable at any moment. Segments are listed in the recommended order of work: the core path
first, then the parts that make the solution complete, then extras.

Status: `[ ]` not started · `[~]` in progress · `[x]` done

## Priority path

| Order | Segment | Why |
|---|---|---|
| 1 | S0 Foundation | nothing works without the workspace and Git |
| 2 | S1 Bronze: taxi ✅ | largest dataset, core of every question |
| 3 | S3 Silver: taxi + DimDate | cleaned data is the base for the model |
| 4 | S4 Gold: taxi star schema | first end-to-end result (minimum worth presenting) |
| 5 | S5 Mobility report page | first visible answer |
| 6 | S2 Bronze: World Bank + ECB ✅ | small and easy, unlocks USD/EUR questions |
| 7 | S3/S4 economy part | DimFX, DimGDP, revenue in EUR |
| 8 | S2 Bronze: OpenAQ ✅ | needs an API key and pagination |
| 9 | S3/S4/S5 air quality part | completes the cross-domain questions |
| 10 | S6 Orchestration & governance | automated refresh, lineage |
| 11 | S7 Documentation | required deliverable |
| 12 | S8 Extra integrations | graded separately, only after the core is complete |
| 13 | S9 Defense preparation | always, at the end |

---

## S0 Foundation
- [x] GitHub repository with this plan
- [x] Fabric workspace `nyc-urban-analytics` on the Trial capacity
- [x] Lakehouses `lh_bronze`, `lh_silver`, `lh_gold` and Warehouse `wh_gold`
- [x] Items kept in `fabric/` and synced with `tools/fabric_sync.py` (the university tenant blocks
      Fabric Git integration with GitHub, so the REST API is used instead)
- [x] OpenAQ account and API key (kept outside Git)
- [x] `config/sources.json` uploaded to `lh_bronze/Files/config/`

Done when: every workspace item can be recreated from the repository with `fabric_sync.py deploy`.

## S1 Bronze: NYC taxi
- [x] Pipeline `pl_ingest_taxi`: Lookup reads the month range and datasets from `sources.json`,
      ForEach (4 in parallel) runs `pl_copy_taxi_file` per (dataset, month): HTTP binary copy →
      `lh_bronze/Files/taxi/yellow/year=YYYY/month=MM/`
- [x] Months that already exist are skipped (Get Metadata `exists` + If Condition)
- [x] Zone lookup CSV and zone shapefile copied to `lh_bronze/Files/taxi/reference/`

Done when: 12 months of yellow taxi Parquet files are in Bronze and a second run copies nothing.
Result: 12 files (2025-10 … 2026-09, ~800 MB) loaded in 3 min; the second run took 1 min and left
every Parquet file unchanged (only the two small reference files are refreshed by design).

## S2 Bronze: APIs
- [x] Dataflow Gen2 `df_ecb_fx`: daily USD/EUR → table `bronze_ecb_fx` (7,204 rows since 1999)
- [x] Dataflow Gen2 `df_worldbank_gdp`: yearly GDP (USD) → table `bronze_worldbank_gdp` (66 years)
- [x] Dataflow Gen2 `df_openaq`: NYC locations (bounding box) → sensors for PM2.5 / NO2 / O3 →
      daily values, paginated with `page`/`limit` (max 1000), throttled to 60 requests/min →
      tables `bronze_openaq_locations` (288 sensors, 76 locations) and `bronze_openaq_daily` (15,168 rows)
- [x] Every Bronze table keeps the raw values plus `_ingested_at` and `_source`
- [x] OpenAQ key only in `.env`; placeholder in Git, pre-commit hook blocks leaks

Done when: the tables exist and a refresh does not duplicate rows (destinations replace the table;
verified: a second `df_ecb_fx` run left 7,204 rows).

## S3 Silver: cleaning and conforming (PySpark notebooks)
- [ ] `nb_silver_taxi`: snake_case names, proper types, duplicates removed, derived columns
      (`pickup_date`, `pickup_hour`, `trip_duration_min`, `is_airport_trip`), zone names joined.
      Suspicious rows are **flagged, not dropped** (`dq_flags`: zero distance, negative fare, trip
      longer than 24 h, pickup outside the file's month) so they can be analysed separately
- [ ] `nb_silver_air`: one row per location, day and pollutant; units normalised; location mapped
      to taxi zone / borough (point-in-polygon with the zone shapefile)
- [ ] `nb_silver_econ`: FX filled for weekends and holidays (last known rate), GDP per year
- [ ] `nb_dim_date`: calendar table (date, weekday, month, quarter, year, is_weekend, US holidays)
- [ ] Row counts per step written to `silver_dq_log` (in, out, flagged)

Done when: Silver Delta tables exist, partitioned by year/month where large, and the DQ log explains
every difference in row counts.

## S4 Gold: star schema in `wh_gold`
- [ ] Dimensions: `DimDate`, `DimZone`, `DimFX`, `DimGDP`
- [ ] Facts: `FactTaxiDaily` (date, pickup zone: trips, passengers, revenue USD/EUR, avg fare, avg
      distance, avg duration), `FactAirQualityDaily` (date, zone/borough, pollutant: avg, max)
- [ ] T-SQL scripts kept in `sql/` (DDL and load), loads are repeatable
- [ ] `sql/analysis/` answers the four key questions from the requirements

Done when: the four analytical questions are answered by saved queries on the star schema.

## S5 Analytics & visualisation (Power BI, Direct Lake on `wh_gold`)
- [ ] Mobility: trips per day, average fare, busiest pickup zones, peak hours
- [ ] Air quality: PM2.5 / NO2 trends by day and location
- [ ] Mobility vs air quality: trips overlaid with pollution peaks, correlation
- [ ] Economic impact: revenue USD vs EUR, FX rate, GDP context

Done when: the report is published in the workspace and every page answers one question.

## S6 Orchestration & governance
- [ ] `pl_master`: ingest → silver → gold, scheduled refresh, failure notification
- [ ] Lineage view of the workspace exported to `docs/`
- [ ] Optional: row-level security role in the semantic model, Purview lineage

Done when: one scheduled run refreshes the whole chain without manual steps.

## S7 Documentation
- [ ] Architecture diagram and run instructions (README)
- [ ] Data dictionary for every Silver and Gold table
- [ ] Lineage diagram and governance policies (naming, access, data quality, retention)
- [ ] "How to add a new source" (extendability)

## S8 Extra integrations (separate grade, only after the core)
- [ ] Weather data → periodic job copying enriched weather data into a time-series database →
      Grafana dashboard
- [ ] Great Expectations checks (distributions, nulls…) with a Telegram/Discord bot command that
      produces a data quality report

## S9 Defense preparation
- [ ] Demo script: what to show and in which order
- [ ] Be ready to change things live: add a month, add a pollutant, change an aggregation
- [ ] Known limitations and next steps listed honestly
