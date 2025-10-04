-- =============================================================================
-- TRAVEL BOOKING SCD2 MERGE PROJECT - WORKFLOW COMPLETION LOGGING
-- =============================================================================
-- This script logs the successful completion of the SQL workflow
-- Purpose: Records workflow completion status for audit and monitoring
-- Business Value: Pipeline tracking, audit trails, operational monitoring
-- Dependencies: Requires workflow2_run_log table to be created by init script

-- =============================================================================
-- WORKFLOW COMPLETION LOGGING
-- =============================================================================
-- Log successful completion of SQL workflow execution
-- run_id: Unique identifier with timestamp for uniqueness
-- arrival_date: Business date being processed (from parameter or current date)
-- status: SUCCESS indicates successful completion
-- message: Descriptive message about the workflow completion
-- recorded_at: Timestamp when the completion was logged

INSERT INTO travel_bookings.ops.workflow2_run_log
SELECT
  CONCAT('wf2-', STRING(current_timestamp())) AS run_id,
  COALESCE(TRY_CAST(:arrival_date AS DATE), current_date()) AS arrival_date,
  'SUCCESS' AS status,
  'Completed SQL workflow' AS message,
  current_timestamp() AS recorded_at;