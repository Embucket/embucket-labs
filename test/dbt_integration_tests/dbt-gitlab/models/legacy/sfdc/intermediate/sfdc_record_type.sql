WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_record_type_source') }}

)

SELECT *
FROM source
