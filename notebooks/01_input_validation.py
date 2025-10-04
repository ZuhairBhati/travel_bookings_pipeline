# Databricks notebook source
# =============================================================================
# TRAVEL BOOKING SCD2 MERGE PROJECT - INPUT VALIDATION
# =============================================================================
# This notebook validates input parameters and file existence before processing
# Purpose: Ensures all required inputs are available and creates audit logging
# Dependencies: Requires booking and customer CSV files for the specified date
# Output: Creates run_log table for pipeline tracking and validation

import datetime as _dt

# =============================================================================
# PARAMETER EXTRACTION WITH DEFAULTS
# =============================================================================
# Extract widget parameters with fallback defaults for flexibility
# arrival_date: Business date for processing (defaults to today)
# catalog: Unity Catalog name (defaults to travel_bookings)
# schema: Target schema (defaults to default)
# base_volume: Base path for data files (defaults to /Volumes/{catalog}/{schema}/data)

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

from pyspark.sql.functions import current_timestamp, lit, to_date
from pyspark.sql import functions as F
import time

# =============================================================================
# FILE PATH CONSTRUCTION
# =============================================================================
# Construct expected file paths based on business date
# Format: /Volumes/{catalog}/{schema}/data/{data_type}_data/{file_type}_{date}.csv

booking_path = f"{base_volume}/booking_data/bookings_{arrival_date}.csv"
customer_path = f"{base_volume}/customer_data/customers_{arrival_date}.csv"

# =============================================================================
# FILE EXISTENCE VALIDATION
# =============================================================================
# Check if required input files exist before proceeding
# Collects missing files and raises exception if any are not found

missing = []
try:
    dbutils.fs.ls(booking_path)
except Exception:
    missing.append(booking_path)

try:
    dbutils.fs.ls(customer_path)
except Exception:
    missing.append(customer_path)

if missing:
    raise FileNotFoundError(f"Missing input files: {missing}")

# =============================================================================
# AUDIT LOGGING SETUP
# =============================================================================
# Create operations schema and run_log table for pipeline tracking
# Tracks run_id, arrival_date, stage, status, message, and timestamp

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.ops")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.ops.run_log (
  run_id STRING,
  arrival_date DATE,
  stage STRING,
  status STRING,
  message STRING,
  recorded_at TIMESTAMP
) USING DELTA
""")

# =============================================================================
# LOG VALIDATION COMPLETION
# =============================================================================
# Record successful validation in run_log for audit trail
# Uses timestamp-based run_id for uniqueness

run_id = f"nb-validate-{arrival_date}-{int(time.time())}"
log_df = spark.createDataFrame([
    (run_id, arrival_date, "validate_inputs", "STARTED", "Inputs validated")
], ["run_id","arrival_date","stage","status","message"])
log_df = log_df.withColumn("arrival_date", F.to_date("arrival_date")).withColumn("recorded_at", current_timestamp())
log_df.write.mode("append").saveAsTable(f"{catalog}.ops.run_log")

print("Validation successful:", booking_path, customer_path)
