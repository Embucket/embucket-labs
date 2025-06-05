WITH source AS (

    SELECT * 
    FROM EMBUCKET.sheetload.hostnames_to_domains

)

SELECT * 
FROM source