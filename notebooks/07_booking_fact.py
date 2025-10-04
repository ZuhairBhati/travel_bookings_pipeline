# Databricks notebook source
# =============================================================================
# TRAVEL BOOKING SCD2 MERGE PROJECT - SILVER LAYER: BOOKING FACT TABLE
# =============================================================================
# This notebook builds the booking fact table with dimension integration
# Purpose: Creates aggregated fact table with surrogate keys from customer dimension
# Data Model: Daily grain fact table with customer surrogate key integration
# Output: Creates/updates booking_fact table with daily booking aggregations

from pyspark.sql import functions as F
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
# Load booking data from bronze layer for the specified business date
# Filters to current day's data for incremental processing

book = spark.table(f"{catalog}.bronze.booking_inc").where(F.col("business_date") == F.to_date(F.lit(arrival_date)))

# =============================================================================
# DIMENSION INTEGRATION
# =============================================================================
# Load current customer dimension records with surrogate keys
# Joins booking data with customer dimension to get surrogate keys
# Uses LEFT JOIN to preserve all bookings even if customer not in dimension

current_dim = spark.sql(f"SELECT customer_sk, customer_id FROM {catalog}.{schema}.customer_dim WHERE is_current = true")

book_enriched = (book.alias("b")
  .join(current_dim.alias("d"), F.col("b.customer_id") == F.col("d.customer_id"), "left")
  .withColumn("customer_sk", F.col("d.customer_sk")))

# =============================================================================
# FACT TABLE COLUMN SELECTION
# =============================================================================
# Select relevant columns for fact table aggregation
# Includes booking_type, customer keys, business_date, and financial metrics

book_enriched_sel = book_enriched.select(
    F.col("booking_type"),
    F.col("b.customer_id").alias("customer_id"),
    F.col("customer_sk"),
    F.col("business_date"),
    F.col("amount"),
    F.col("discount"),
    F.col("quantity")
)

# =============================================================================
# FACT TABLE AGGREGATION
# =============================================================================
# Aggregate booking data to daily grain by booking_type, customer, and date
# Calculates total amount (after discount) and total quantity
# Daily grain provides idempotent processing and business-friendly aggregation

agg = (book_enriched_sel.groupBy("booking_type","customer_sk","customer_id","business_date")
        .agg(F.sum(F.col("amount") - F.col("discount")).alias("total_amount_sum"),
             F.sum("quantity").alias("total_quantity_sum")))

fact_full_name = f"{catalog}.{schema}.booking_fact"

# =============================================================================
# FACT TABLE SCHEMA CREATION
# =============================================================================
# Create fact table schema if it doesn't exist
# Uses Delta format for ACID properties and schema evolution

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

if not spark.catalog.tableExists(fact_full_name):
  a = agg.limit(0)
  a.write.format("delta").mode("overwrite").option("mergeSchema","true").saveAsTable(fact_full_name)

# =============================================================================
# FACT TABLE MERGE OPERATION
# =============================================================================
# Merge aggregated data into fact table for idempotent processing
# Updates existing records or inserts new ones based on business key
# Business key: booking_type + customer_sk + business_date

agg.createOrReplaceTempView("src")
spark.sql(f"""
  MERGE INTO {fact_full_name} t
  USING src s
  ON  t.booking_type = s.booking_type
  AND t.customer_sk <=> s.customer_sk
  AND t.business_date = s.business_date
  WHEN MATCHED THEN UPDATE SET
    t.total_amount_sum = s.total_amount_sum,
    t.total_quantity_sum = s.total_quantity_sum,
    t.customer_id = s.customer_id
  WHEN NOT MATCHED THEN INSERT *
""")

print("Fact build complete (daily grain, surrogate key)")
