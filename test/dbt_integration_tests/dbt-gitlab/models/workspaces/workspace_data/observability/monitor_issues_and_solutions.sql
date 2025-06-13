WITH source AS (

    SELECT *
    FROM {{ ref('monitor_issues_and_solutions_source') }}

)

SELECT *
FROM source
