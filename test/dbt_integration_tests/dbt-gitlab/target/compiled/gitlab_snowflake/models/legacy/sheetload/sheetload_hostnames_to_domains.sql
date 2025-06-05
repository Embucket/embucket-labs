WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.sheetload_hostnames_to_domains_source

)

SELECT *
FROM source