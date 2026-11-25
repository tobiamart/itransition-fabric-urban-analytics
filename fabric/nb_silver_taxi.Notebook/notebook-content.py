# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_silver_taxi: taxi trips (Silver)
# Reads every monthly Parquet file from `lh_bronze/Files/taxi/<dataset>/year=YYYY/month=MM/` and
# writes `lh_silver.dbo.silver_taxi_trips`, partitioned by year and month:
# 1. **Standardise**: snake_case names, one fixed schema (files differ slightly between months).
# 2. **Deduplicate**: identical rows are removed.
# 3. **Enrich**: pickup date and hour, trip duration, airport flag, pickup / drop-off zone names.
# 4. **Flag, don't drop**: suspicious rows get reasons in `dq_flags` and `is_valid = false`, so they
#    can still be analysed; Gold uses only valid rows.
# Row counts and the number of rows per check go to `silver_dq_log`.

# PARAMETERS CELL ********************

DATASET = "yellow"
RUN_ID = ""

# CELL ********************

import json
import uuid
from functools import reduce

from pyspark.sql import functions as F

WORKSPACE_ID = "{{workspace}}"          # ids are filled in by tools/fabric_sync.py on deploy
BRONZE_ID = "{{item:lh_bronze.Lakehouse}}"
SILVER_ID = "{{item:lh_silver.Lakehouse}}"
ONELAKE = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com"
BRONZE, SILVER = f"{ONELAKE}/{BRONZE_ID}", f"{ONELAKE}/{SILVER_ID}"
RUN_ID = RUN_ID or str(uuid.uuid4())


def log_dq(step: str, table: str, metrics: dict) -> None:
    """Append row counts / check results to silver_dq_log and keep the latest run as JSON."""
    rows = [(RUN_ID, step, table, name, float(value)) for name, value in metrics.items()]
    (spark.createDataFrame(rows, "run_id string, step string, table_name string, metric string, value double")
        .withColumn("logged_at", F.current_timestamp())
        .write.format("delta").mode("append").save(f"{SILVER}/Tables/dbo/silver_dq_log"))
    notebookutils.fs.put(f"{SILVER}/Files/dq/{step}_{table}.json", json.dumps({"run_id": RUN_ID, **metrics}, indent=2), True)

# MARKDOWN ********************

# ## 1. Standardise: every month is cast to the same Silver schema

# CELL ********************

# Bronze column (lower-cased) -> Silver column; the other columns only change to lower case
RENAME = {
    "vendorid": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "ratecodeid": "rate_code_id",
    "pulocationid": "pickup_location_id",
    "dolocationid": "dropoff_location_id",
}
SCHEMA = {
    "vendor_id": "int", "pickup_datetime": "timestamp_ntz", "dropoff_datetime": "timestamp_ntz",
    "passenger_count": "int", "trip_distance": "double", "rate_code_id": "int",
    "store_and_fwd_flag": "string", "pickup_location_id": "int", "dropoff_location_id": "int",
    "payment_type": "int", "fare_amount": "double", "extra": "double", "mta_tax": "double",
    "tip_amount": "double", "tolls_amount": "double", "improvement_surcharge": "double",
    "total_amount": "double", "congestion_surcharge": "double", "airport_fee": "double",
    "cbd_congestion_fee": "double",
}


def read_month(path: str, year: int, month: int):
    raw = spark.read.parquet(path)
    raw = raw.toDF(*[RENAME.get(c.lower(), c.lower()) for c in raw.columns])
    columns = [(F.col(c) if c in raw.columns else F.lit(None)).cast(t).alias(c) for c, t in SCHEMA.items()]
    return raw.select(*columns, F.lit(year).alias("year"), F.lit(month).alias("month"))


def partition_value(name: str) -> int:
    return int(name.strip("/").split("=")[1])


months = []
for year_dir in notebookutils.fs.ls(f"{BRONZE}/Files/taxi/{DATASET}"):
    if year_dir.isDir and year_dir.name.startswith("year="):
        for month_dir in notebookutils.fs.ls(year_dir.path):
            if month_dir.isDir and month_dir.name.startswith("month="):
                months.append((partition_value(year_dir.name), partition_value(month_dir.name), month_dir.path))

print(f"{len(months)} monthly files:", [f"{y}-{m:02d}" for y, m, _ in sorted(months)])
raw_trips = reduce(lambda a, b: a.unionByName(b), [read_month(p, y, m) for y, m, p in sorted(months)])
rows_in = raw_trips.count()

# MARKDOWN ********************

# ## 2–3. Deduplicate and enrich

# CELL ********************

zones = (spark.read.option("header", True).csv(f"{BRONZE}/Files/taxi/reference/taxi_zone_lookup.csv")
         .select(F.col("LocationID").cast("int").alias("location_id"),
                 F.col("Borough").alias("borough"),
                 F.col("Zone").alias("zone"),
                 F.col("service_zone")))
(zones.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .save(f"{SILVER}/Tables/dbo/silver_taxi_zones"))


def zone_names(prefix: str):
    return zones.select(F.col("location_id").alias(f"{prefix}_location_id"),
                        F.col("borough").alias(f"{prefix}_borough"),
                        F.col("zone").alias(f"{prefix}_zone"))


AIRPORT_ZONES = [1, 132, 138]           # Newark, JFK, LaGuardia

trips = (raw_trips.dropDuplicates()
         .join(F.broadcast(zone_names("pickup")), "pickup_location_id", "left")
         .join(F.broadcast(zone_names("dropoff")), "dropoff_location_id", "left")
         .withColumn("pickup_date", F.to_date("pickup_datetime"))
         .withColumn("pickup_hour", F.hour("pickup_datetime"))
         .withColumn("trip_duration_min",
                     F.round(F.expr("timestampdiff(SECOND, pickup_datetime, dropoff_datetime)") / 60.0, 2))
         .withColumn("is_airport_trip",
                     F.col("pickup_location_id").isin(AIRPORT_ZONES) | F.col("dropoff_location_id").isin(AIRPORT_ZONES)))

# MARKDOWN ********************

# ## 4. Data quality: flag, don't drop
# * **Errors** make a trip invalid (`is_valid = false`): Gold leaves it out.
# * **Warnings** only mark the trip: it still counts in trips and revenue. About a quarter of the 2025–2026 trips have no passenger count, so treating that as an error would hide real trips.

# CELL ********************

UNKNOWN_BOROUGHS = ["Unknown", "N/A"]   # zones 264 / 265 in the lookup
ERRORS = {
    "zero_distance": F.col("trip_distance") <= 0,
    "distance_over_200mi": F.col("trip_distance") > 200,
    "negative_amount": (F.col("fare_amount") < 0) | (F.col("total_amount") < 0),
    "non_positive_duration": F.col("trip_duration_min") <= 0,
    "duration_over_24h": F.col("trip_duration_min") > 24 * 60,
    "outside_file_month": (F.year("pickup_datetime") != F.col("year")) | (F.month("pickup_datetime") != F.col("month")),
}
WARNINGS = {
    "missing_passenger_count": F.col("passenger_count").isNull() | (F.col("passenger_count") == 0),
    "unknown_zone": (F.col("pickup_zone").isNull() | F.col("dropoff_zone").isNull()
                     | F.col("pickup_borough").isin(UNKNOWN_BOROUGHS)
                     | F.col("dropoff_borough").isin(UNKNOWN_BOROUGHS)),
}


def failed(checks: dict):
    """Array with the names of the checks a row fails (empty when it passes all of them)."""
    return F.array_compact(F.array(*[F.when(rule, F.lit(name)) for name, rule in checks.items()]))


trips = (trips
         .withColumn("dq_errors", failed(ERRORS))
         .withColumn("dq_flags", F.concat("dq_errors", failed(WARNINGS)))
         .withColumn("is_valid", F.size("dq_errors") == 0)
         .drop("dq_errors"))

COLUMNS = (["pickup_datetime", "dropoff_datetime", "pickup_date", "pickup_hour", "trip_duration_min",
            "vendor_id", "passenger_count", "trip_distance", "rate_code_id", "store_and_fwd_flag",
            "pickup_location_id", "pickup_borough", "pickup_zone",
            "dropoff_location_id", "dropoff_borough", "dropoff_zone", "is_airport_trip", "payment_type"]
           + [c for c in SCHEMA if c.endswith(("_amount", "_fee", "_surcharge", "_tax")) or c == "extra"]
           + ["dq_flags", "is_valid", "year", "month"])

(trips.select(*COLUMNS)
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .partitionBy("year", "month")
    .save(f"{SILVER}/Tables/dbo/silver_taxi_trips"))

# CELL ********************

silver = spark.read.format("delta").load(f"{SILVER}/Tables/dbo/silver_taxi_trips")
totals = silver.agg(F.count("*").alias("rows_out"),
                    F.sum(F.when(F.size("dq_flags") > 0, 1).otherwise(0)).alias("rows_flagged"),
                    F.sum(F.when(~F.col("is_valid"), 1).otherwise(0)).alias("rows_invalid")).first()
per_check = silver.select(F.explode("dq_flags").alias("check")).groupBy("check").count().collect()

metrics = {
    "rows_in": rows_in,
    "duplicates_removed": rows_in - totals["rows_out"],
    "rows_out": totals["rows_out"],
    "rows_flagged": totals["rows_flagged"],
    "rows_invalid": totals["rows_invalid"],
    **{f"check:{row['check']}": row["count"] for row in per_check},
}
log_dq("nb_silver_taxi", "silver_taxi_trips", metrics)
print(json.dumps(metrics, indent=2))
