WITH base AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'group_import_states') }}

)

{{ scd_latest_state(primary_key='group_id') }}