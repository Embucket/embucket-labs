WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_event_dictionary_simplified_source') }}

)

SELECT *
FROM source
