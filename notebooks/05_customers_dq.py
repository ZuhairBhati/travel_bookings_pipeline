# Databricks notebook source
# =============================================================================
# TRAVEL BOOKING SCD2 MERGE PROJECT - DATA QUALITY: CUSTOMER DATA VALIDATION
# =============================================================================
# This notebook performs comprehensive data quality checks on customer data
# Purpose: Validates customer data integrity using PyDeequ framework
# Data Quality: Checks completeness and business rules for customer attributes
# Output: Logs DQ results and raises exceptions for failed validations

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
from pyspark.sql import functions as F

# =============================================================================
# PARAMETER EXTRACTION WITH DEFAULTS
# =============================================================================
# Extract widget parameters with fallback defaults for flexibility
# arrival_date: Business date for processing (defaults to today)
# catalog: Unity Catalog name (defaults to travel_bookings)
# schema: Target schema (defaults to default)

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

# =============================================================================
# SOURCE DATA PREPARATION
# =============================================================================
# Load customer data from bronze layer for the specified business date
# Filters to current day's data for incremental DQ processing

src = spark.table(f"{catalog}.bronze.customer_inc").where(F.col("business_date") == F.to_date(F.lit(arrival_date)))

# =============================================================================
# DATA QUALITY CHECKS DEFINITION
# =============================================================================
# Define comprehensive DQ checks using PyDeequ framework
# hasSize: Ensures data exists (row count > 0)
# isComplete: Validates required customer fields are not null
# Focus on customer-specific attributes: name, address, email

check = (Check(spark, CheckLevel.Error, "Customer Data Check")
         .hasSize(lambda x: x > 0)
         .isComplete("customer_name")
         .isComplete("customer_address")
         .isComplete("email"))

# =============================================================================
# DQ EXECUTION AND RESULTS
# =============================================================================
# Execute DQ checks and capture results for audit logging
# Displays results for immediate review and stores for historical tracking

result = (VerificationSuite(spark).onData(src).addCheck(check).run())
df = VerificationResult.checkResultsAsDataFrame(spark, result)

# =============================================================================
# DQ RESULTS STORAGE SETUP
# =============================================================================
# Create operations schema and DQ results table for audit tracking
# Stores DQ check results with metadata for monitoring and reporting

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.ops")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.ops.dq_results (
  business_date DATE,
  dataset STRING,
  check_name STRING,
  status STRING,
  constraint STRING,
  message STRING,
  recorded_at TIMESTAMP
) USING DELTA
""")

# =============================================================================
# DQ RESULTS LOGGING
# =============================================================================
# Transform and store DQ results with metadata for audit trail
# Includes business_date, dataset name, and timestamp for tracking

out = (df
  .withColumn("business_date", F.to_date(F.lit(arrival_date)))
  .withColumn("dataset", F.lit("customer_inc"))
  .withColumn("recorded_at", F.current_timestamp()))

display(df)

out.select("business_date","dataset","check","check_status","constraint","constraint_status","constraint_message","recorded_at") \
   .write.mode("append").option("mergeSchema", "true").saveAsTable(f"{catalog}.ops.dq_results")

# =============================================================================
# DQ VALIDATION AND ERROR HANDLING
# =============================================================================
# Validate DQ results and raise exception if any checks failed
# Ensures data quality before proceeding to downstream processing

if result.status != "Success":
  raise ValueError("DQ failed for customers")

print("Customer DQ passed")
