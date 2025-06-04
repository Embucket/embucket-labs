WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_communication_certificate_source') }}

),
{{cleanup_certificates("'communication_certificate'",)}}
