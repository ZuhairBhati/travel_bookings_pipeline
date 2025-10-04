# Databricks notebook source
# =============================================================================
# TRAVEL BOOKING SCD2 MERGE PROJECT - BRONZE LAYER: CUSTOMER DATA INGESTION
# =============================================================================
# This notebook ingests customer master data into the bronze layer with SCD2 preparation
# Purpose: Raw customer data ingestion with SCD2 temporal columns for dimension processing
# Data Quality: Adds SCD2 metadata columns for historical tracking
# Output: Creates/updates bronze.customer_inc table with SCD2-ready customer data

import datetime as _dt
try:
    arrival_date = dbutils.widgets.get("arrival_date")
except Exception:
    arrival_date = _dt.date.today().strftime("%Y-%m-%d")
try:
    catalog = dbutils.widgets.get("catalog")
except Exception:
    catalog = "travel_bookings"
try:
    schema = dbutils.widgets.get("schema")
except Exception:
    schema = "default"
try:
    base_volume = dbutils.widgets.get("base_volume")
except Exception:
    base_volume = f"/Volumes/{catalog}/{schema}/data"

from pyspark.sql import functions as F

# =============================================================================
# DATA INGESTION CONFIGURATION
# =============================================================================
# Construct file path and configure CSV reader for customer data
# Handles quoted fields, multi-line records, and schema inference

customer_path = f"{base_volume}/customer_data/customers_{arrival_date}.csv"

df = (spark.read.format("csv")
      .option("header","true").option("inferSchema","true")
      .option("quote","\"").option("multiLine","true")
      .load(customer_path))

# =============================================================================
# SCD2 TEMPORAL COLUMNS PREPARATION
# =============================================================================
# Add SCD2 temporal columns for dimension table processing
# valid_from: Start date for this version (business date)
# valid_to: End date for this version (9999-12-31 for current records)
# is_current: Boolean flag indicating if this is the current version
# business_date: Business date for partitioning and filtering

out = (df
  .withColumn("valid_from", F.to_date(F.lit(arrival_date)))
  .withColumn("valid_to", F.to_date(F.lit("9999-12-31")))
  .withColumn("is_current", F.lit(True))
  .withColumn("business_date", F.to_date(F.lit(arrival_date))))

# =============================================================================
# BRONZE LAYER STORAGE
# =============================================================================
# Create bronze schema and save data to Delta table
# Uses append mode for incremental loading
# Delta format provides ACID properties and schema evolution

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")

out.write.format("delta").mode("append").saveAsTable(f"{catalog}.bronze.customer_inc")

print(f"Ingested rows: {out.count()}")
