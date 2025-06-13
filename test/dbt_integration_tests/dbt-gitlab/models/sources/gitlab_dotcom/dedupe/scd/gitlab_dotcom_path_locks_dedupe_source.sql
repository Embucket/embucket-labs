WITH base AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'path_locks') }}

)

{{ scd_latest_state() }}