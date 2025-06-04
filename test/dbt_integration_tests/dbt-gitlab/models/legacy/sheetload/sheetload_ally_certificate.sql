WITH source AS (

    SELECT * 
    FROM {{ ref('sheetload_ally_certificate_source') }}
),
{{cleanup_certificates("'ally_certificate'")}}
