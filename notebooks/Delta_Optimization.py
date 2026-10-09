# Databricks notebook source
silver_path = "abfss://datalake@valorantdatalakeadlsgen2.dfs.core.windows.net/silver/valorant_matches"

fact_path = "abfss://datalake@valorantdatalakeadlsgen2.dfs.core.windows.net/gold/fact_match_performance"

print("Silver Path:", silver_path)
print("Gold Fact Path:", fact_path)

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

print("Silver Rows:", silver_df.count())
print("Gold Rows:", fact_df.count())

# COMMAND ----------

spark.sql(f"""
OPTIMIZE delta.`{silver_path}`
ZORDER BY (match_id)
""")

print("✅ Silver optimization completed")

# COMMAND ----------

spark.sql(f"""
OPTIMIZE delta.`{fact_path}`
ZORDER BY (match_id)
""")

print("✅ Gold Fact optimization completed")

# COMMAND ----------

from delta.tables import DeltaTable

silver_delta = DeltaTable.forPath(spark, silver_path)

display(
    silver_delta.history(5).select(
        "version",
        "timestamp",
        "operation",
        "operationMetrics"
    )
)

# COMMAND ----------

fact_delta = DeltaTable.forPath(spark, fact_path)

display(
    fact_delta.history(5).select(
        "version",
        "timestamp",
        "operation",
        "operationMetrics"
    )
)