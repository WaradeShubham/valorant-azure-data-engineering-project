# Databricks notebook source
dbutils.widgets.text(
    "input_file",
    "valorant_incremental_day5_250.csv"
)

input_file = dbutils.widgets.get("input_file")

print("Input File:", input_file)

# COMMAND ----------

silver_path = "abfss://datalake@valorantdatalakeadlsgen2.dfs.core.windows.net/silver/valorant_matches"

fact_path = "abfss://datalake@valorantdatalakeadlsgen2.dfs.core.windows.net/gold/fact_match_performance"

audit_path = "abfss://datalake@valorantdatalakeadlsgen2.dfs.core.windows.net/gold/pipeline_audit"

# COMMAND ----------

silver_df = (
    spark.read
    .format("delta")
    .load(silver_path)
)

fact_df = (
    spark.read
    .format("delta")
    .load(fact_path)
)

silver_rows = silver_df.count()
gold_rows = fact_df.count()

print("Silver Rows:", silver_rows)
print("Gold Rows:", gold_rows)

# COMMAND ----------

from pyspark.sql.functions import (
    lit,
    current_timestamp
)

audit_record_df = (
    spark.range(1)
    .select(
        lit("Valorant_Data_Engineering_Pipeline").alias("pipeline_name"),
        lit(input_file).alias("input_file"),
        lit(silver_rows).cast("long").alias("silver_rows"),
        lit(gold_rows).cast("long").alias("gold_rows"),
        lit("SUCCESS").alias("status"),
        current_timestamp().alias("run_timestamp")
    )
)

display(audit_record_df)

# COMMAND ----------

(
    audit_record_df
    .write
    .format("delta")
    .mode("append")
    .save(audit_path)
)

print("✅ Pipeline audit record saved")

# COMMAND ----------

audit_history_df = (
    spark.read
    .format("delta")
    .load(audit_path)
)

display(
    audit_history_df
    .orderBy("run_timestamp", ascending=False)
)

# COMMAND ----------

audit_path = "abfss://datalake@valorantdatalakeadlsgen2.dfs.core.windows.net/gold/pipeline_audit"

audit_df = (
    spark.read
    .format("delta")
    .load(audit_path)
)

display(
    audit_df
    .orderBy("run_timestamp", ascending=False)
    .limit(10)
)