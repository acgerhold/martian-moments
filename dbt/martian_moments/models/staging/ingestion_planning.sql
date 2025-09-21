{{ config(
    materialized='view',
    tags='schedule'
) }}

WITH rover_gaps AS (
    SELECT 
        rover_name,
        actual_max_sol,
        manifest_max_sol,
        sol_gap,
        needs_ingestion
    FROM {{ ref('validation_rover_gaps') }}
    WHERE needs_ingestion = TRUE
),

sol_ranges AS (
    SELECT 
        rover_name,
        actual_max_sol + 1 as start_sol,
        manifest_max_sol as max_sol,
        sol_gap as total_sols_needed,
        CEIL(sol_gap / 50.0) as estimated_batches
    FROM rover_gaps
),

ingestion_tasks AS (
    SELECT 
        rover_name,
        start_sol,
        1250 as end_sol,
        max_sol,        
        total_sols_needed,
        estimated_batches,
        CASE 
            WHEN total_sols_needed <= 50 THEN 'SINGLE_BATCH'
            WHEN total_sols_needed <= 200 THEN 'SMALL_BACKFILL'
            WHEN total_sols_needed <= 500 THEN 'MEDIUM_BACKFILL'
            ELSE 'LARGE_BACKFILL'
        END as ingestion_priority,
        CURRENT_TIMESTAMP() as task_created_at
    FROM sol_ranges
)

SELECT * FROM ingestion_tasks