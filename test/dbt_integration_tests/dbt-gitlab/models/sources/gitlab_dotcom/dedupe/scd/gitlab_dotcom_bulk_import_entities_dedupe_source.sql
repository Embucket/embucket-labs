WITH base AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'bulk_import_entities') }}

)

{{ scd_latest_state() }}