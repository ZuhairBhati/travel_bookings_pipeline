# Databricks notebook source
# =============================================================================
# TRAVEL BOOKING SCD2 MERGE PROJECT - STATISTICS ANALYSIS: TABLE STATISTICS
# =============================================================================
# This notebook updates table statistics for query optimization
# Purpose: Computes and updates column statistics for optimal query planning
# Performance: Enables cost-based optimizer to make better execution plans
# Output: Updated statistics for all columns in fact and dimension tables

# =============================================================================
# PARAMETER EXTRACTION WITH DEFAULTS
# =============================================================================
# Extract widget parameters with fallback defaults for flexibility
# catalog: Unity Catalog name (defaults to travel_bookings)
# schema: Target schema (defaults to default)

try:
    catalog = dbutils.widgets.get("catalog")
except Exception:
    catalog = "travel_bookings"
try:
    schema = dbutils.widgets.get("schema")
except Exception:
    schema = "default"

# =============================================================================
# TABLE STATISTICS COMPUTATION
# =============================================================================
# Compute statistics for all columns in fact and dimension tables
# booking_fact: Statistics for customer_id, booking_type, business_date, amounts
# customer_dim: Statistics for customer_id, name, address, email, temporal columns
# FOR ALL COLUMNS: Ensures comprehensive statistics for all query patterns

spark.sql(f"ANALYZE TABLE {catalog}.{schema}.booking_fact COMPUTE STATISTICS FOR ALL COLUMNS")
spark.sql(f"ANALYZE TABLE {catalog}.{schema}.customer_dim COMPUTE STATISTICS FOR ALL COLUMNS")

print("Statistics analysis complete")
