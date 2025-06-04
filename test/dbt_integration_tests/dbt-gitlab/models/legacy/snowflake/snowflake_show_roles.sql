WITH source AS (

    SELECT *
    FROM {{ ref('snowflake_show_roles_source') }}

), max_select AS (

    SELECT *
    FROM source
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM source)

)

SELECT *
FROM max_select
