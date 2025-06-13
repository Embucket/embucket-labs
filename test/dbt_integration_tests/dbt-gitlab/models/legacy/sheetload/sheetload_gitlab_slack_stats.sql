WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_gitlab_slack_stats_source') }}

)

SELECT *
FROM source