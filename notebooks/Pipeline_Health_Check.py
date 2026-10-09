# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import DeltaTable

silver_path = "abfss://datalake@valorantdatalakeadlsgen2.dfs.core.windows.net/silver/valorant_matches"

fact_path = "abfss://datalake@valorantdatalakeadlsgen2.dfs.core.windows.net/gold/fact_match_performance"

audit_path = "abfss://datalake@valorantdatalakeadlsgen2.dfs.core.windows.net/gold/pipeline_audit"

silver_df = spark.read.format("delta").load(silver_path)
fact_df = spark.read.format("delta").load(fact_path)
audit_df = spark.read.format("delta").load(audit_path)

print("Tables loaded successfully ✅")

# COMMAND ----------

silver_total = silver_df.count()
silver_unique = silver_df.select("match_id").distinct().count()

gold_total = fact_df.count()
gold_unique = fact_df.select("match_id").distinct().count()

print("Silver Rows:", silver_total)
print("Silver Unique IDs:", silver_unique)

print("Gold Rows:", gold_total)
print("Gold Unique IDs:", gold_unique)

# COMMAND ----------

fact_df.select(
    count("*").alias("total_rows"),

    sum(when(col("date_id").isNull(), 1).otherwise(0))
        .alias("missing_date_id"),

    sum(when(col("player_id").isNull(), 1).otherwise(0))
        .alias("missing_player_id"),

    sum(when(col("agent_id").isNull(), 1).otherwise(0))
        .alias("missing_agent_id"),

    sum(when(col("map_id").isNull(), 1).otherwise(0))
        .alias("missing_map_id"),

    sum(when(col("rank_id").isNull(), 1).otherwise(0))
        .alias("missing_rank_id")
).show()

# COMMAND ----------

display(
    audit_df
    .orderBy(col("run_timestamp").desc())
    .limit(10)
)

# COMMAND ----------

silver_delta = DeltaTable.forPath(
    spark,
    silver_path
)

silver_merge_history = (
    silver_delta
    .history()
    .filter(col("operation") == "MERGE")
    .orderBy(col("version").desc())
    .limit(1)
)

display(
    silver_merge_history.select(
        "version",
        "timestamp",
        "operation",
        "operationMetrics"
    )
)

# COMMAND ----------

fact_delta = DeltaTable.forPath(
    spark,
    fact_path
)

gold_merge_history = (
    fact_delta
    .history()
    .filter(col("operation") == "MERGE")
    .orderBy(col("version").desc())
    .limit(1)
)

display(
    gold_merge_history.select(
        "version",
        "timestamp",
        "operation",
        "operationMetrics"
    )
)

# COMMAND ----------

missing_fk_count = (
    fact_df
    .filter(
        col("date_id").isNull() |
        col("player_id").isNull() |
        col("agent_id").isNull() |
        col("map_id").isNull() |
        col("rank_id").isNull()
    )
    .count()
)

if silver_total != silver_unique:
    raise Exception("❌ HEALTH CHECK FAILED: Duplicate match_id in Silver")

if gold_total != gold_unique:
    raise Exception("❌ HEALTH CHECK FAILED: Duplicate match_id in Gold")

if silver_total != gold_total:
    raise Exception("❌ HEALTH CHECK FAILED: Silver and Gold counts do not match")

if missing_fk_count > 0:
    raise Exception(
        f"❌ HEALTH CHECK FAILED: {missing_fk_count} Gold rows have missing dimension keys"
    )

print("✅ FINAL PIPELINE HEALTH CHECK PASSED")
print("Silver Rows:", silver_total)
print("Gold Rows:", gold_total)
print("Unique Match IDs:", gold_unique)
print("Missing Foreign Keys:", missing_fk_count)