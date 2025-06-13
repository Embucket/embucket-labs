WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_compensation_certificate_source') }}

),
{{cleanup_certificates("'compensation_certificate'")}}
