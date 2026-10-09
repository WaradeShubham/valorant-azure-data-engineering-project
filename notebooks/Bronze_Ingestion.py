# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./Common_Config

# COMMAND ----------

# ADF / Databricks Job passes this parameter dynamically.
# The default is only for manual testing.
dbutils.widgets.text("input_file", "valorant_incremental_day5_250.csv")
input_file = dbutils.widgets.get("input_file").strip()

if not input_file:
    raise ValueError("input_file parameter is required")

raw_incremental_path = f"{raw_incremental_base_path}/{input_file}"

print("Input File:", input_file)
print("Reading From:", raw_incremental_path)
print("Writing To:", bronze_incremental_path)

# COMMAND ----------

incremental_raw_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(raw_incremental_path)
)

raw_count = incremental_raw_df.count()
print("Raw Incremental Rows:", raw_count)

if raw_count == 0:
    raise Exception("FAILED: Incoming file contains 0 rows")

display(incremental_raw_df.limit(10))

# COMMAND ----------

(
    incremental_raw_df
    .write
    .format("delta")
    .mode("overwrite")
    .save(bronze_incremental_path)
)

print("Bronze incremental load completed")

# COMMAND ----------

bronze_check_df = load_delta(bronze_incremental_path)
bronze_count = bronze_check_df.count()

print("Bronze Incremental Rows:", bronze_count)

if bronze_count != raw_count:
    raise Exception(
        f"FAILED: Raw/Bronze count mismatch. Raw={raw_count}, Bronze={bronze_count}"
    )