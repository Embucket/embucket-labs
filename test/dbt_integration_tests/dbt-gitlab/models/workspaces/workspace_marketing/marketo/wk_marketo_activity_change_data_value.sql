WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_change_data_value_source') }}

)

SELECT *
FROM source