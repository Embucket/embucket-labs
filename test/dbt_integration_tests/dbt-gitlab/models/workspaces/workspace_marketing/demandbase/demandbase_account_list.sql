WITH source AS (

    SELECT *
    FROM {{ ref('demandbase_account_list_source') }}

)

SELECT *
FROM source