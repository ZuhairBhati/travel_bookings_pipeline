# Databricks notebook source
# =============================================================================
# TRAVEL BOOKING SCD2 MERGE PROJECT - BRONZE LAYER: BOOKING DATA INGESTION
# =============================================================================
# This notebook ingests booking transaction data into the bronze layer
# Purpose: Raw data ingestion with metadata enrichment for downstream processing
# Data Quality: Preserves original data structure with audit columns
# Output: Creates/updates bronze.booking_inc table with daily booking transactions

from pyspark.sql.functions import current_timestamp
from pyspark.sql import functions as F

# =============================================================================
# PARAMETER EXTRACTION WITH DEFAULTS
# =============================================================================
# Extract widget parameters with fallback defaults for flexibility
# arrival_date: Business date for processing (defaults to today)
# catalog: Unity Catalog name (defaults to travel_bookings)
# schema: Target schema (defaults to default)
# base_volume: Base path for data files (defaults to /Volumes/{catalog}/{schema}/data)

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

# =============================================================================
# DATA INGESTION CONFIGURATION
# =============================================================================
# Construct file path and configure CSV reader for booking data
# Handles quoted fields, multi-line records, and schema inference

booking_path = f"{base_volume}/booking_data/bookings_{arrival_date}.csv"

df = (spark.read.format("csv")
      .option("header","true").option("inferSchema","true")
      .option("quote","\"").option("multiLine","true")
      .load(booking_path))

# =============================================================================
# METADATA ENRICHMENT
# =============================================================================
# Add audit columns for data lineage and business context
# ingestion_time: Timestamp when data was processed
# business_date: Business date for partitioning and filtering

df = df.withColumn("ingestion_time", current_timestamp()) \
       .withColumn("business_date", F.to_date(F.lit(arrival_date)))

# =============================================================================
# BRONZE LAYER STORAGE
# =============================================================================
# Create bronze schema and save data to Delta table
# Uses append mode for incremental loading
# Delta format provides ACID properties and schema evolution

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")

df.write.format("delta").mode("append").saveAsTable(f"{catalog}.bronze.booking_inc")

print(f"Ingested rows: {df.count()}")
