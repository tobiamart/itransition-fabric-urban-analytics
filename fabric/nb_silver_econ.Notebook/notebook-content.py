# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_silver_econ: exchange rates and GDP (Silver)
# * `silver_fx_daily`: one row per calendar day with the USD/EUR rate. The ECB publishes rates on
#   working days only, so weekends and holidays get the last known rate (`is_filled = true`).
# * `silver_gdp_yearly`: GDP in USD per country and year, with year-over-year growth.

# PARAMETERS CELL ********************

RUN_ID = ""

# CELL ********************

import json
import uuid

from pyspark.sql import Window
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

# ## Exchange rate: one row per day, gaps filled with the last published rate

# CELL ********************

fx_raw = spark.read.format("delta").load(f"{BRONZE}/Tables/dbo/bronze_ecb_fx")
fx = (fx_raw
      .select(F.expr("try_cast(TIME_PERIOD AS date)").alias("date"),
              F.expr("try_cast(nullif(OBS_VALUE, '') AS double)").alias("usd_per_eur"))
      .where(F.col("date").isNotNull())
      .dropDuplicates(["date"]))

first_day, last_day = fx.agg(F.min("date"), F.max("date")).first()
calendar = spark.sql(f"SELECT explode(sequence(DATE'{first_day}', DATE'{last_day}', INTERVAL 1 DAY)) AS date")

up_to_today = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
fx_daily = (calendar.join(fx, "date", "left")
            .withColumn("rate_date", F.when(F.col("usd_per_eur").isNotNull(), F.col("date")))
            .withColumn("usd_per_eur", F.last("usd_per_eur", ignorenulls=True).over(up_to_today))
            .withColumn("rate_date", F.last("rate_date", ignorenulls=True).over(up_to_today))
            .select("date",
                    F.round("usd_per_eur", 6).alias("usd_per_eur"),
                    F.round(1 / F.col("usd_per_eur"), 6).alias("eur_per_usd"),
                    (F.col("date") != F.col("rate_date")).alias("is_filled"),
                    "rate_date"))

(fx_daily.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .save(f"{SILVER}/Tables/dbo/silver_fx_daily"))

fx_metrics = {
    "rows_in": fx_raw.count(),
    "published_rates": fx.where(F.col("usd_per_eur").isNotNull()).count(),
    "rows_out": fx_daily.count(),
    "rows_filled": fx_daily.where("is_filled").count(),
}
log_dq("nb_silver_econ", "silver_fx_daily", fx_metrics)
print(fx_metrics)

# MARKDOWN ********************

# ## GDP: typed, one row per country and year, with growth

# CELL ********************

gdp_raw = spark.read.format("delta").load(f"{BRONZE}/Tables/dbo/bronze_worldbank_gdp")
by_year = Window.partitionBy("country_code").orderBy("year")
gdp = (gdp_raw
       .select("country_code", "country_name",
               F.expr("try_cast(year AS int)").alias("year"),
               F.col("value").alias("gdp_usd"))
       .where(F.col("year").isNotNull())
       .dropDuplicates(["country_code", "year"])
       .withColumn("gdp_growth_pct",
                   F.round((F.col("gdp_usd") / F.lag("gdp_usd").over(by_year) - 1) * 100, 3))
       .withColumn("is_valid", F.col("gdp_usd").isNotNull()))

(gdp.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .save(f"{SILVER}/Tables/dbo/silver_gdp_yearly"))

gdp_metrics = {"rows_in": gdp_raw.count(), "rows_out": gdp.count(), "rows_flagged": gdp.where(~F.col("is_valid")).count()}
log_dq("nb_silver_econ", "silver_gdp_yearly", gdp_metrics)
print(gdp_metrics)
