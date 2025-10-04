-- =============================================================================
-- TRAVEL BOOKING SCD2 MERGE PROJECT - SQL WORKFLOW INITIALIZATION
-- =============================================================================
-- This script initializes schemas and tables for the SQL-driven workflow
-- Purpose: Sets up analytics schema and workflow tracking tables
-- Dependencies: Run after notebook workflow completion
-- Output: Creates analytics schema and workflow2_run_log table

-- =============================================================================
-- SCHEMA CREATION
-- =============================================================================
-- Create analytics schema for reporting and analytics tables
-- Create ops schema for operational tracking (if not already exists)

CREATE SCHEMA IF NOT EXISTS travel_bookings.analytics;
CREATE SCHEMA IF NOT EXISTS travel_bookings.ops;

-- =============================================================================
-- WORKFLOW TRACKING TABLE
-- =============================================================================
-- Create table to track SQL workflow execution status
-- run_id: Unique identifier for each workflow run
-- arrival_date: Business date being processed
-- status: Execution status (SUCCESS, FAILED, etc.)
-- message: Descriptive message about the run
-- recorded_at: Timestamp when the record was created

CREATE TABLE IF NOT EXISTS travel_bookings.ops.workflow2_run_log (
   run_id STRING,
   arrival_date DATE,
   status STRING,
   message STRING,
   recorded_at TIMESTAMP
) USING DELTA;