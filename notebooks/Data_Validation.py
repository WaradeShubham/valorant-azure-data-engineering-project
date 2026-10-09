# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./Common_Config

# COMMAND ----------

# Fresh reads: validation must inspect the latest committed state.
silver_df = load_delta(silver_path)
fact_df = load_delta(fact_path)

silver_total = silver_df.count()
silver_unique = silver_df.select("match_id").distinct().count()
fact_total = fact_df.count()
fact_unique = fact_df.select("match_id").distinct().count()

print("Silver Rows:", silver_total)
print("Silver Unique Match IDs:", silver_unique)
print("Gold Rows:", fact_total)
print("Gold Unique Match IDs:", fact_unique)

# COMMAND ----------

fk_row = fact_df.select(
    count("*").alias("total_rows"),
    count("date_id").alias("date_id_count"),
    count("player_id").alias("player_id_count"),
    count("agent_id").alias("agent_id_count"),
    count("map_id").alias("map_id_count"),
    count("rank_id").alias("rank_id_count")
).first()

print("Foreign-key validation:", dict(fk_row.asDict()))

# COMMAND ----------

negative_value_count = silver_df.filter(
    (col("kills") < 0) |
    (col("deaths") < 0) |
    (col("assists") < 0) |
    (col("headshots") < 0) |
    (col("damage") < 0) |
    (col("rounds_played") < 0)
).count()

invalid_result_count = silver_df.filter(
    col("result").isNotNull() & (~col("result").isin("Win", "Loss"))
).count()

print("Negative numeric rows:", negative_value_count)
print("Invalid result rows:", invalid_result_count)

# COMMAND ----------

if silver_total != silver_unique:
    raise Exception("FAILED: Duplicate match_id found in Silver")

if fact_total != fact_unique:
    raise Exception("FAILED: Duplicate match_id found in Gold Fact")

if silver_total != fact_total:
    raise Exception(
        f"FAILED: Silver and Gold row counts do not match. Silver={silver_total}, Gold={fact_total}"
    )

for key in ["date_id_count", "player_id_count", "agent_id_count", "map_id_count", "rank_id_count"]:
    if fk_row[key] != fk_row["total_rows"]:
        raise Exception(f"FAILED: Gold foreign-key validation failed for {key}")

if negative_value_count > 0:
    raise Exception("FAILED: Negative numeric values found in Silver")

if invalid_result_count > 0:
    raise Exception("FAILED: Invalid result values found in Silver")

print("✅ DATA VALIDATION PASSED")
print("Silver Rows:", silver_total)
print("Gold Rows:", fact_total)
print("Unique Match IDs:", fact_unique)