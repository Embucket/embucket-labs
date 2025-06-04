WITH source AS (

    SELECT *
    FROM {{ ref('driveload_financial_metrics_program_phase_1_source') }}

)

SELECT *
FROM source
