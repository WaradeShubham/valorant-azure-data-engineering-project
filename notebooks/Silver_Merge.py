# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./Common_Config

# COMMAND ----------

bronze_df = load_delta(bronze_incremental_path)
silver_df = load_delta(silver_path)

print("Bronze Rows:", bronze_df.count())
print("Current Silver Rows:", silver_df.count())

# COMMAND ----------

# Clean current incoming batch. No Day-2 hard-coded ID ranges.
silver_source_df = (
    bronze_df
    .dropDuplicates(["match_id"])
    .withColumn("match_date", to_date("match_date"))
    .withColumn("kills", col("kills").cast("int"))
    .withColumn("deaths", col("deaths").cast("int"))
    .withColumn("assists", col("assists").cast("int"))
    .withColumn("headshots", col("headshots").cast("int"))
    .withColumn("damage", col("damage").cast("int"))
    .withColumn("rounds_played", col("rounds_played").cast("int"))
)

source_count = silver_source_df.count()
source_unique = silver_source_df.select("match_id").distinct().count()

print("Silver Source Rows:", source_count)
print("Silver Source Unique Match IDs:", source_unique)

if source_count != source_unique:
    raise Exception("FAILED: Duplicate match_id remains in Silver source")

# COMMAND ----------

# Optional observability: how many incoming rows are new vs already existing?
target_keys = silver_df.select("match_id")
new_count = silver_source_df.join(target_keys, "match_id", "left_anti").count()
existing_count = silver_source_df.join(target_keys, "match_id", "inner").count()

print("Incoming New Records:", new_count)
print("Incoming Existing Records:", existing_count)

# COMMAND ----------

# Persist the exact clean batch for the downstream Gold task.
(
    silver_source_df
    .write
    .format("delta")
    .mode("overwrite")
    .save(staging_path)
)

staging_check_df = load_delta(staging_path)
staging_count = staging_check_df.count()
print("Staging Rows:", staging_count)

if staging_count != source_count:
    raise Exception(
        f"FAILED: Silver source/Staging count mismatch. Source={source_count}, Staging={staging_count}"
    )

# COMMAND ----------

silver_delta = DeltaTable.forPath(spark, silver_path)

(
    silver_delta.alias("target")
    .merge(
        silver_source_df.alias("source"),
        "target.match_id = source.match_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("Silver MERGE completed")

# COMMAND ----------

updated_silver_df = load_delta(silver_path)
print("Silver Rows After MERGE:", updated_silver_df.count())

updated_silver_df.select(
    count("*").alias("total_rows"),
    countDistinct("match_id").alias("unique_match_ids")
).show()

# COMMAND ----------

display(
    silver_delta.history(1).select(
        "version",
        "timestamp",
        "operation",
        "operationMetrics"
    )
)

# COMMAND ----------

from pyspark.sql.functions import col, countDistinct

print("Silver Total Rows:", silver_df.count())

silver_df.select(
    countDistinct("match_id").alias("Unique Match IDs")
).show()

print(
    "IDs 1-100:",
    silver_df.filter(col("match_id").between(1, 100)).count()
)

print(
    "Day2 IDs 10001-10400:",
    silver_df.filter(col("match_id").between(10001, 10400)).count()
)

print(
    "Day5 IDs 10401-10600:",
    silver_df.filter(col("match_id").between(10401, 10600)).count()
)