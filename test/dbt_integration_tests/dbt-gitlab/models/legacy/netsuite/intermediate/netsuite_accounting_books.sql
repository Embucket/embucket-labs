WITH source AS (

    SELECT *
    FROM {{ ref('netsuite_accounting_books_source') }}

)

SELECT *
FROM source
