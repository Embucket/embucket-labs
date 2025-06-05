

WITH base AS (

    SELECT *
    FROM EMBUCKET.tap_postgres.gitlab_db_milestone_releases
    
    QUALIFY ROW_NUMBER() OVER (PARTITION BY milestone_id, release_id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

    SELECT md5(cast(coalesce(cast(milestone_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(release_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS primary_key,
           milestone_id::INT                                             AS milestone_id,
           release_id::INT                                               AS release_id,
           _uploaded_at                                                  AS _uploaded_at
    FROM base

)

SELECT *
FROM renamed