WITH source AS (

    SELECT *
    FROM {{ ref('google_search_console_keyword_page_report_source') }}

)

SELECT *
FROM source