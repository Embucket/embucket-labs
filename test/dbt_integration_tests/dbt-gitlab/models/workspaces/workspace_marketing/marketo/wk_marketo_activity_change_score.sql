WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_change_score_source') }}

)

SELECT *
FROM source