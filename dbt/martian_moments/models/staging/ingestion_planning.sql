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
        CEIL(sol_gap / 100.0) as estimated_batches
    FROM rover_gaps
),

ingestion_tasks AS (
    SELECT 
        rover_name,
        CEIL(start_sol / 100.0) * 100 as start_sol,
        LEAST(CEIL((start_sol + 100) / 100.0) * 100, max_sol) as end_sol,
        start_sol as photos_max_sol,
        max_sol as manifest_max_sol,
        start_sol >= max_sol as up_to_date,        
        total_sols_needed,
        estimated_batches,
        CURRENT_TIMESTAMP() as task_created_at
    FROM sol_ranges
)

SELECT * FROM ingestion_tasks