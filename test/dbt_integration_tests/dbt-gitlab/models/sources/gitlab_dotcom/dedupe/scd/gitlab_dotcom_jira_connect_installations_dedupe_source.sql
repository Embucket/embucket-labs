WITH base AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'jira_connect_installations') }}

)

{{ scd_latest_state() }}