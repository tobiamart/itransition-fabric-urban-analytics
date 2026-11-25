# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_dim_date: calendar table (Silver)
# One row per day with the attributes used for slicing in the reports: weekday, month, quarter,
# season, weekend and US / New York holidays. Written to `lh_silver.dbo.silver_dim_date` and
# copied to `wh_gold.dbo.DimDate` in the Gold layer.

# PARAMETERS CELL ********************

START_DATE = "2015-01-01"
END_DATE = "2027-12-31"
RUN_ID = ""

# CELL ********************

import json
import uuid

import holidays
import pandas as pd
from pyspark.sql import functions as F

WORKSPACE_ID = "{{workspace}}"          # ids are filled in by tools/fabric_sync.py on deploy
SILVER_ID = "{{item:lh_silver.Lakehouse}}"
SILVER = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{SILVER_ID}"
RUN_ID = RUN_ID or str(uuid.uuid4())


def log_dq(step: str, table: str, metrics: dict) -> None:
    """Append row counts / check results to silver_dq_log and keep the latest run as JSON."""
    rows = [(RUN_ID, step, table, name, float(value)) for name, value in metrics.items()]
    (spark.createDataFrame(rows, "run_id string, step string, table_name string, metric string, value double")
        .withColumn("logged_at", F.current_timestamp())
        .write.format("delta").mode("append").save(f"{SILVER}/Tables/dbo/silver_dq_log"))
    notebookutils.fs.put(f"{SILVER}/Files/dq/{step}_{table}.json", json.dumps({"run_id": RUN_ID, **metrics}, indent=2), True)

# CELL ********************

SEASONS = {12: "Winter", 1: "Winter", 2: "Winter", 3: "Spring", 4: "Spring", 5: "Spring",
           6: "Summer", 7: "Summer", 8: "Summer", 9: "Autumn", 10: "Autumn", 11: "Autumn"}

days = pd.date_range(START_DATE, END_DATE, freq="D")
us_holidays = holidays.country_holidays("US", subdiv="NY", years=range(days.min().year, days.max().year + 1))

calendar = pd.DataFrame({
    "date_key": days.strftime("%Y%m%d").astype(int),
    "date": days.date,
    "year": days.year,
    "quarter": days.quarter,
    "month": days.month,
    "month_name": days.strftime("%B"),
    "day": days.day,
    "day_of_week": days.dayofweek + 1,           # 1 = Monday ... 7 = Sunday
    "day_name": days.strftime("%A"),
    "iso_week": days.isocalendar().week.astype(int).to_numpy(),
})
calendar["is_weekend"] = calendar["day_of_week"] >= 6
calendar["season"] = calendar["month"].map(SEASONS)
calendar["holiday_name"] = [us_holidays.get(day) for day in calendar["date"]]
calendar["is_holiday"] = calendar["holiday_name"].notna()

dim_date = spark.createDataFrame(calendar, schema=(
    "date_key int, date date, year int, quarter int, month int, month_name string, day int, "
    "day_of_week int, day_name string, iso_week int, is_weekend boolean, season string, "
    "holiday_name string, is_holiday boolean"))

(dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .save(f"{SILVER}/Tables/dbo/silver_dim_date"))

metrics = {"rows_out": dim_date.count(), "holidays": int(calendar["is_holiday"].sum())}
log_dq("nb_dim_date", "silver_dim_date", metrics)
print(metrics)
