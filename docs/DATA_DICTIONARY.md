# Data dictionary

All tables are Delta tables in OneLake, schema `dbo`. Every table is rebuilt by its notebook or
dataflow (overwrite / replace), so re-running never duplicates rows.

## Bronze (`lh_bronze`): raw data as published

| Table / folder | Loaded by | Content |
|---|---|---|
| `Files/taxi/yellow/year=YYYY/month=MM/*.parquet` | `pl_ingest_taxi` | NYC TLC yellow taxi trips, one file per month, unchanged |
| `Files/taxi/reference/taxi_zone_lookup.csv`, `taxi_zones.zip` | `pl_ingest_taxi` | 265 taxi zones (id, borough, name) and their polygons (EPSG:2263) |
| `Files/config/sources.json` | `fabric_sync.py upload` | source configuration read by the pipelines |
| `bronze_ecb_fx` | `df_ecb_fx` | ECB CSV columns as text (`TIME_PERIOD`, `OBS_VALUE`, `OBS_STATUS`, …) |
| `bronze_worldbank_gdp` | `df_worldbank_gdp` | `country_code`, `country_name`, `indicator_id`, `year` (text), `value` (USD) |
| `bronze_openaq_locations` | `df_openaq` | one row per sensor: location, provider, coordinates, parameter, units |
| `bronze_openaq_daily` | `df_openaq` | daily summary per sensor: average, min, max, median, coverage |

Every Bronze table also has `_ingested_at` (UTC load time) and `_source`.

## Silver (`lh_silver`): cleaned and conformed

### `silver_taxi_trips` (`nb_silver_taxi`, partitioned by `year`, `month`)

| Column | Type | Description |
|---|---|---|
| `pickup_datetime`, `dropoff_datetime` | timestamp_ntz | local NYC time of pickup / drop-off |
| `pickup_date`, `pickup_hour` | date, int | derived from the pickup time |
| `trip_duration_min` | double | drop-off minus pickup, minutes |
| `vendor_id`, `rate_code_id`, `payment_type`, `store_and_fwd_flag` | int / string | TLC codes |
| `passenger_count`, `trip_distance` | int, double | passengers, miles |
| `pickup_location_id`, `pickup_borough`, `pickup_zone` | int, string | pickup taxi zone (joined from the lookup) |
| `dropoff_location_id`, `dropoff_borough`, `dropoff_zone` | int, string | drop-off taxi zone |
| `is_airport_trip` | boolean | pickup or drop-off at Newark (1), JFK (132) or LaGuardia (138) |
| `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `congestion_surcharge`, `airport_fee`, `cbd_congestion_fee`, `total_amount` | double | amounts in USD |
| `dq_flags` | array<string> | failed checks (errors and warnings), empty when the row is clean |
| `is_valid` | boolean | `false` when the row fails an error check; Gold uses valid rows only |
| `year`, `month` | int | month of the source file |

Error checks (row becomes invalid): `zero_distance`, `distance_over_200mi`, `negative_amount`,
`non_positive_duration`, `duration_over_24h`, `outside_file_month` (pickup not in the file's month).
Warning checks (row stays valid): `missing_passenger_count` (about a quarter of 2025–2026 trips
have no passenger count), `unknown_zone` (zones 264/265 or no match).
Exact duplicate rows are removed before the checks.

### `silver_taxi_zones` (`nb_silver_taxi`)

`location_id` int, `borough`, `zone`, `service_zone`: the TLC zone lookup.

### `silver_air_locations` (`nb_silver_air`)

| Column | Type | Description |
|---|---|---|
| `location_id`, `location_name`, `locality`, `provider` | int, string | OpenAQ location |
| `is_monitor` | boolean | `true` for reference monitors (AirNow), `false` for low-cost sensors |
| `latitude`, `longitude` | double | WGS84 |
| `taxi_zone_id`, `borough`, `zone` | int, string | taxi zone containing the location (point-in-polygon), null outside NYC |
| `in_nyc` | boolean | the location lies inside a NYC taxi zone |
| `datetime_first_utc`, `datetime_last_utc` | string | first / last measurement |

### `silver_air_quality_daily` (`nb_silver_air`)

| Column | Type | Description |
|---|---|---|
| `date` | date | local calendar day (New York) |
| `location_id`, `sensor_id` | int | OpenAQ ids |
| `pollutant` | string | `pm25`, `no2`, `o3` |
| `unit` | string | standard unit: `ug/m3` for PM2.5, `ppb` for NO2 and O3 |
| `value_avg`, `value_min`, `value_max` | double | daily values in the standard unit |
| `observed_count`, `percent_complete` | int, double | hourly measurements received, % of the expected 24 |
| `taxi_zone_id`, `borough`, `zone`, `in_nyc`, `is_monitor`, `provider` | | from `silver_air_locations` |
| `source_unit` | string | unit as published by OpenAQ |
| `dq_flags`, `is_valid` | array<string>, boolean | checks: `unknown_unit`, `missing_value`, `negative_value`, `low_coverage` (< 75 %), `missing_date` |

Unit conversion: 1 ppm = 1000 ppb; µg/m³ → ppb = µg/m³ × 24.45 / molar mass (NO2 46.0055, O3 47.9982).

### `silver_fx_daily` (`nb_silver_econ`)

| Column | Type | Description |
|---|---|---|
| `date` | date | every calendar day from 1999-01-04 |
| `usd_per_eur` | double | ECB reference rate: USD for 1 EUR |
| `eur_per_usd` | double | `1 / usd_per_eur`, used to convert USD revenue to EUR |
| `is_filled` | boolean | `true` on weekends / holidays: the last published rate is carried forward |
| `rate_date` | date | day the rate was published |

### `silver_gdp_yearly` (`nb_silver_econ`)

`country_code`, `country_name`, `year` int, `gdp_usd` double, `gdp_growth_pct` (change versus the
previous year, %), `is_valid` (GDP present).

### `silver_dim_date` (`nb_dim_date`)

`date_key` (yyyymmdd), `date`, `year`, `quarter`, `month`, `month_name`, `day`, `day_of_week`
(1 = Monday), `day_name`, `iso_week`, `is_weekend`, `season`, `holiday_name`, `is_holiday`
(US federal and New York State holidays), 2015-01-01 to 2027-12-31.

### `silver_dq_log` (all Silver notebooks)

One row per run, table and metric: `run_id`, `step` (notebook), `table_name`, `metric`
(`rows_in`, `duplicates_removed`, `rows_out`, `rows_flagged`, `check:<name>`, …), `value`,
`logged_at`. The latest run of each step is also written to `lh_silver/Files/dq/<step>_<table>.json`.
