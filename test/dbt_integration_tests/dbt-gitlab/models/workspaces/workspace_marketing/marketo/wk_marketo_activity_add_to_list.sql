WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_add_to_list_source') }}

)

SELECT *
FROM source