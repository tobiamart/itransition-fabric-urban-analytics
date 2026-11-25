# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_silver_air: air quality (Silver)
# * `silver_air_locations`: one row per OpenAQ location, mapped to the NYC taxi zone that contains it (point-in-polygon with the TLC zone shapefile), so air quality can be compared with taxi traffic per zone and borough. Locations outside NYC (New Jersey, Long Island) have no zone.
# * `silver_air_quality_daily`: one row per sensor and day. Units are normalised: PM2.5 in µg/m³, NO2 and O3 in ppb. Days with incomplete measurements are flagged, not dropped.

# PARAMETERS CELL ********************

MIN_COVERAGE_PCT = 75.0                 # a day needs at least 75 % of its hourly measurements
RUN_ID = ""

# CELL ********************

# The zone shapefile needs geometry libraries that the Fabric runtime does not include. They are
# only used on the driver (76 points), so a driver-side install is enough.
import subprocess
import sys

subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", "shapely", "pyproj", "pyshp"], check=True)

# CELL ********************

import io
import json
import uuid
import zipfile

import shapefile
from pyproj import Transformer
from pyspark.sql import functions as F
from shapely.geometry import Point, shape

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

# ## Sensor locations → taxi zones
# The TLC shapefile uses the New York State Plane projection (EPSG:2263, feet), so sensor coordinates (WGS84 lat/lon) are projected first.

# CELL ********************

zip_bytes = spark.read.format("binaryFile").load(f"{BRONZE}/Files/taxi/reference/taxi_zones.zip").first()["content"]
with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
    parts = {name.rsplit(".", 1)[-1].lower(): name for name in archive.namelist() if "__MACOSX" not in name}
    reader = shapefile.Reader(**{ext: io.BytesIO(archive.read(parts[ext])) for ext in ("shp", "shx", "dbf")})
    zone_polygons = [(int(item.record.as_dict()["LocationID"]), shape(item.shape.__geo_interface__))
                     for item in reader.shapeRecords()]

to_state_plane = Transformer.from_crs("EPSG:4326", "EPSG:2263", always_xy=True)


def taxi_zone_of(longitude: float, latitude: float):
    point = Point(*to_state_plane.transform(longitude, latitude))
    return next((zone_id for zone_id, polygon in zone_polygons if polygon.contains(point)), None)


locations = (spark.read.format("delta").load(f"{BRONZE}/Tables/dbo/bronze_openaq_locations")
             .select("location_id", "location_name", "locality", "provider", "is_monitor",
                     "latitude", "longitude", "datetime_first_utc", "datetime_last_utc")
             .dropDuplicates(["location_id"])
             .toPandas())
locations["taxi_zone_id"] = [taxi_zone_of(lon, lat) for lon, lat in zip(locations["longitude"], locations["latitude"])]
locations["taxi_zone_id"] = locations["taxi_zone_id"].astype("Int64")

zone_lookup = (spark.read.option("header", True).csv(f"{BRONZE}/Files/taxi/reference/taxi_zone_lookup.csv")
               .select(F.col("LocationID").cast("int").alias("taxi_zone_id"),
                       F.col("Borough").alias("borough"), F.col("Zone").alias("zone")))

air_locations = (spark.createDataFrame(locations)
                 .withColumn("taxi_zone_id", F.col("taxi_zone_id").cast("int"))
                 .join(F.broadcast(zone_lookup), "taxi_zone_id", "left")
                 .withColumn("in_nyc", F.col("taxi_zone_id").isNotNull()))

(air_locations.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .save(f"{SILVER}/Tables/dbo/silver_air_locations"))

location_metrics = {"rows_out": air_locations.count(), "locations_in_nyc": int(locations["taxi_zone_id"].notna().sum())}
log_dq("nb_silver_air", "silver_air_locations", location_metrics)
print(location_metrics)

# MARKDOWN ********************

# ## Daily values: normalised units and quality flags
# Conversion to ppb at 25 °C: ppb = µg/m³ × 24.45 / molar mass (g/mol); 1 ppm = 1000 ppb.

# CELL ********************

MOLAR_MASS = {"no2": 46.0055, "o3": 47.9982}
STANDARD_UNIT = {"pm25": "ug/m3", "no2": "ppb", "o3": "ppb"}

unit = F.lower(F.translate(F.col("units"), "µμ³", "uu3"))      # "µg/m³" -> "ug/m3"
pollutant = F.col("pollutant")
factor = (F.when((pollutant == "pm25") & (unit == "ug/m3"), 1.0)
          .when(pollutant.isin("no2", "o3") & (unit == "ppb"), 1.0)
          .when(pollutant.isin("no2", "o3") & (unit == "ppm"), 1000.0)
          .when((pollutant == "no2") & (unit == "ug/m3"), 24.45 / MOLAR_MASS["no2"])
          .when((pollutant == "o3") & (unit == "ug/m3"), 24.45 / MOLAR_MASS["o3"]))

daily_raw = spark.read.format("delta").load(f"{BRONZE}/Tables/dbo/bronze_openaq_daily")
rows_in = daily_raw.count()

daily = (daily_raw
         .select(F.expr("try_cast(substring(period_from_local, 1, 10) AS date)").alias("date"),
                 "location_id", "sensor_id", F.col("parameter").alias("pollutant"), "units",
                 "value_avg", "value_min", "value_max", "observed_count", "percent_complete")
         .dropDuplicates(["sensor_id", "date"])
         .withColumn("factor", factor)
         .withColumn("unit", F.create_map(*[F.lit(x) for kv in STANDARD_UNIT.items() for x in kv])[pollutant])
         .withColumn("value_avg", F.round(F.col("value_avg") * F.col("factor"), 4))
         .withColumn("value_min", F.round(F.col("value_min") * F.col("factor"), 4))
         .withColumn("value_max", F.round(F.col("value_max") * F.col("factor"), 4)))

CHECKS = {
    "unknown_unit": F.col("factor").isNull(),
    "missing_value": F.col("value_avg").isNull(),
    "negative_value": F.col("value_avg") < 0,
    "low_coverage": F.col("percent_complete") < MIN_COVERAGE_PCT,
    "missing_date": F.col("date").isNull(),
}

air_daily = (daily
             .withColumn("dq_flags", F.array_compact(F.array(*[F.when(rule, F.lit(name)) for name, rule in CHECKS.items()])))
             .withColumn("is_valid", F.size("dq_flags") == 0)
             .join(air_locations.select("location_id", "taxi_zone_id", "borough", "zone", "in_nyc", "is_monitor", "provider"),
                   "location_id", "left")
             .select("date", "location_id", "sensor_id", "pollutant", "unit", "value_avg", "value_min", "value_max",
                     "observed_count", "percent_complete", "taxi_zone_id", "borough", "zone", "in_nyc",
                     "is_monitor", "provider", F.col("units").alias("source_unit"), "dq_flags", "is_valid"))

(air_daily.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .save(f"{SILVER}/Tables/dbo/silver_air_quality_daily"))

saved = spark.read.format("delta").load(f"{SILVER}/Tables/dbo/silver_air_quality_daily")
rows_out = saved.count()
per_check = saved.select(F.explode("dq_flags").alias("check")).groupBy("check").count().collect()
metrics = {
    "rows_in": rows_in,
    "duplicates_removed": rows_in - rows_out,
    "rows_out": rows_out,
    "rows_flagged": saved.where(~F.col("is_valid")).count(),
    **{f"check:{row['check']}": row["count"] for row in per_check},
    **{f"rows:{row['pollutant']}": row["count"] for row in saved.groupBy("pollutant").count().collect()},
}
log_dq("nb_silver_air", "silver_air_quality_daily", metrics)
print(json.dumps(metrics, indent=2))
