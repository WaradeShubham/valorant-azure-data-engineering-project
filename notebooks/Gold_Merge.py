# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./Common_Config

# COMMAND ----------

# Read the exact clean batch produced by Silver_Merge.
gold_source_df = load_delta(staging_path)
print("Gold Source Rows:", gold_source_df.count())

# COMMAND ----------

# Load current dimensions.
dim_player = load_delta(dim_player_path)
dim_agent = load_delta(dim_agent_path)
dim_map = load_delta(dim_map_path)
dim_rank = load_delta(dim_rank_path)
dim_date = load_delta(dim_date_path)

print("dim_player:", dim_player.count())
print("dim_agent:", dim_agent.count())
print("dim_map:", dim_map.count())
print("dim_rank:", dim_rank.count())
print("dim_date:", dim_date.count())

# COMMAND ----------

# 1) PLAYER DIMENSION: update name when matched, insert when new.
player_source = (
    gold_source_df
    .select("player_id", "player_name")
    .filter(col("player_id").isNotNull())
    .dropDuplicates(["player_id"])
)

player_delta = DeltaTable.forPath(spark, dim_player_path)
(
    player_delta.alias("target")
    .merge(player_source.alias("source"), "target.player_id = source.player_id")
    .whenMatchedUpdate(set={"player_name": "source.player_name"})
    .whenNotMatchedInsert(values={
        "player_id": "source.player_id",
        "player_name": "source.player_name"
    })
    .execute()
)

# COMMAND ----------

# Helper for small categorical dimensions with surrogate integer IDs.
def append_new_dimension_members(source_df, dim_path, value_col, id_col):
    current_dim = load_delta(dim_path)

    new_values = (
        source_df
        .select(value_col)
        .dropDuplicates()
        .join(current_dim.select(value_col), value_col, "left_anti")
    )

    new_count = new_values.count()
    print(f"New {value_col} values:", new_count)

    if new_count > 0:
        current_max = current_dim.agg(max(id_col).alias("max_id")).first()["max_id"] or 0

        new_rows = (
            new_values
            .withColumn(
                id_col,
                row_number().over(Window.orderBy(value_col)) + lit(current_max)
            )
            .select(id_col, value_col)
        )

        (
            new_rows
            .write
            .format("delta")
            .mode("append")
            .save(dim_path)
        )

# COMMAND ----------

agent_source = gold_source_df.select(
    coalesce(col("agent"), lit("Unknown")).alias("agent")
)
map_source = gold_source_df.select(
    coalesce(col("map"), lit("Unknown")).alias("map")
)
rank_source = gold_source_df.select(
    coalesce(col("rank"), lit("Unknown")).alias("rank")
)

append_new_dimension_members(agent_source, dim_agent_path, "agent", "agent_id")
append_new_dimension_members(map_source, dim_map_path, "map", "map_id")
append_new_dimension_members(rank_source, dim_rank_path, "rank", "rank_id")

# COMMAND ----------

# 5) DATE DIMENSION: deterministic date_id, append only unseen dates.
dim_date = load_delta(dim_date_path)

new_dates = (
    gold_source_df
    .select("match_date")
    .filter(col("match_date").isNotNull())
    .dropDuplicates()
    .join(dim_date.select("match_date"), "match_date", "left_anti")
)

new_date_count = new_dates.count()
print("New Dates:", new_date_count)

if new_date_count > 0:
    new_dates_formatted = (
        new_dates
        .withColumn("date_id", date_format("match_date", "yyyyMMdd").cast("int"))
        .withColumn("year", year("match_date"))
        .withColumn("quarter", quarter("match_date"))
        .withColumn("month", month("match_date"))
        .withColumn("month_name", date_format("match_date", "MMMM"))
        .withColumn("day", dayofmonth("match_date"))
        .withColumn("day_name", date_format("match_date", "EEEE"))
        .select(
            "date_id", "match_date", "year", "quarter",
            "month", "month_name", "day", "day_name"
        )
    )

    (
        new_dates_formatted
        .write
        .format("delta")
        .mode("append")
        .save(dim_date_path)
    )

# COMMAND ----------

# Reload dimensions after any inserts.
dim_player = load_delta(dim_player_path)
dim_agent = load_delta(dim_agent_path)
dim_map = load_delta(dim_map_path)
dim_rank = load_delta(dim_rank_path)
dim_date = load_delta(dim_date_path)

# COMMAND ----------

# Build fact-ready incremental batch with dimension keys.
incremental_fact_df = (
    gold_source_df.alias("s")
    .join(
        dim_player.alias("p"),
        col("s.player_id") == col("p.player_id"),
        "left"
    )
    .join(
        dim_agent.alias("a"),
        coalesce(col("s.agent"), lit("Unknown")) == col("a.agent"),
        "left"
    )
    .join(
        dim_map.alias("m"),
        coalesce(col("s.map"), lit("Unknown")) == col("m.map"),
        "left"
    )
    .join(
        dim_rank.alias("r"),
        coalesce(col("s.rank"), lit("Unknown")) == col("r.rank"),
        "left"
    )
    .join(
        dim_date.alias("d"),
        col("s.match_date") == col("d.match_date"),
        "left"
    )
    .select(
        col("s.match_id"),
        col("d.date_id"),
        col("p.player_id"),
        col("a.agent_id"),
        col("m.map_id"),
        col("r.rank_id"),
        col("s.kills"),
        col("s.deaths"),
        col("s.assists"),
        col("s.headshots"),
        col("s.damage"),
        col("s.rounds_played"),
        col("s.result")
    )
)

fact_source_count = incremental_fact_df.count()
print("Incremental Fact Rows:", fact_source_count)

# COMMAND ----------

# Fail before MERGE if any dimension lookup failed.
fk_row = incremental_fact_df.select(
    count("*").alias("total_rows"),
    count("date_id").alias("date_id_count"),
    count("player_id").alias("player_id_count"),
    count("agent_id").alias("agent_id_count"),
    count("map_id").alias("map_id_count"),
    count("rank_id").alias("rank_id_count")
).first()

print(dict(fk_row.asDict()))

for key in ["date_id_count", "player_id_count", "agent_id_count", "map_id_count", "rank_id_count"]:
    if fk_row[key] != fk_row["total_rows"]:
        raise Exception(f"FAILED: Foreign-key validation failed for {key}")

# COMMAND ----------

fact_delta = DeltaTable.forPath(spark, fact_path)

(
    fact_delta.alias("target")
    .merge(
        incremental_fact_df.alias("source"),
        "target.match_id = source.match_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("Gold Fact MERGE completed")

# COMMAND ----------

updated_fact_df = load_delta(fact_path)
print("Gold Fact Rows:", updated_fact_df.count())

updated_fact_df.select(
    count("*").alias("total_rows"),
    countDistinct("match_id").alias("unique_matches")
).show()

# COMMAND ----------

display(
    fact_delta.history(1).select(
        "version",
        "timestamp",
        "operation",
        "operationMetrics"
    )
)