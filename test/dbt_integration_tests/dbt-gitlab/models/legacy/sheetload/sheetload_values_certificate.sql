WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_values_certificate_source') }}
), 
{{ cleanup_certificates("'values_certificate'") }}
