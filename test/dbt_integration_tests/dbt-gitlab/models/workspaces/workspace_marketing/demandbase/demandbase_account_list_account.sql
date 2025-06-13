WITH source AS (

    SELECT *
    FROM {{ ref('demandbase_account_list_account_source') }}

)

SELECT *
FROM source