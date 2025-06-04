WITH source AS (

    SELECT *
    FROM {{ ref('demandbase_account_site_page_metrics_source') }}

)

SELECT *
FROM source