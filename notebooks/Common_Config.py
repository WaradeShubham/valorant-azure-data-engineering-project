# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

storage_account = "valorantdatalakeadlsgen2"
container = "datalake"
base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

# One-time initial load paths
raw_initial_path = f"{base_path}/raw/matches/valorant_matches_10000.csv"
bronze_full_path = f"{base_path}/bronze/valorant_matches"

# Recurring incremental pipeline paths
raw_incremental_base_path = f"{base_path}/raw/incremental"
bronze_incremental_path = f"{base_path}/bronze/incremental/current_batch"
staging_path = f"{base_path}/bronze/staging/current_batch"

# Silver / Gold paths
silver_path = f"{base_path}/silver/valorant_matches"
gold_base_path = f"{base_path}/gold"
dim_player_path = f"{gold_base_path}/dim_player"
dim_agent_path = f"{gold_base_path}/dim_agent"
dim_map_path = f"{gold_base_path}/dim_map"
dim_rank_path = f"{gold_base_path}/dim_rank"
dim_date_path = f"{gold_base_path}/dim_date"
fact_path = f"{gold_base_path}/fact_match_performance"

def load_delta(path):
    return spark.read.format("delta").load(path)
