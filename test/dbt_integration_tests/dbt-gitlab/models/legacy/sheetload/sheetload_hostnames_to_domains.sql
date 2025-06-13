WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_hostnames_to_domains_source') }}

)

SELECT *
FROM source
