WITH base AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'lfs_file_locks') }}

)

{{ scd_latest_state() }}