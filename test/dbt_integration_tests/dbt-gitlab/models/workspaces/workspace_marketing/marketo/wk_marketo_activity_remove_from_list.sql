WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_remove_from_list_source') }}

)

SELECT *
FROM source