# Databricks notebook source
from pyspark.sql.functions import lit, current_timestamp

dbutils.widgets.text(
    "input_file",
    "valorant_incremental_day5_250.csv"
)

input_file = dbutils.widgets.get("input_file")

silver_path = "abfss://datalake@valorantdatalakeadlsgen2.dfs.core.windows.net/silver/valorant_matches"

fact_path = "abfss://datalake@valorantdatalakeadlsgen2.dfs.core.windows.net/gold/fact_match_performance"

audit_path = "abfss://datalake@valorantdatalakeadlsgen2.dfs.core.windows.net/gold/pipeline_audit"

# COMMAND ----------

try:
    silver_rows = (
        spark.read
        .format("delta")
        .load(silver_path)
        .count()
    )
except:
    silver_rows = -1

try:
    gold_rows = (
        spark.read
        .format("delta")
        .load(fact_path)
        .count()
    )
except:
    gold_rows = -1

print("Silver Rows:", silver_rows)
print("Gold Rows:", gold_rows)

# COMMAND ----------

failure_audit_df = (
    spark.range(1)
    .select(
        lit("Valorant_Data_Engineering_Pipeline").alias("pipeline_name"),
        lit(input_file).alias("input_file"),
        lit(silver_rows).cast("long").alias("silver_rows"),
        lit(gold_rows).cast("long").alias("gold_rows"),
        lit("FAILED").alias("status"),
        current_timestamp().alias("run_timestamp")
    )
)

display(failure_audit_df)

# COMMAND ----------

(
    failure_audit_df
    .write
    .format("delta")
    .mode("append")
    .save(audit_path)
)

print("❌ FAILED pipeline run logged in audit table")