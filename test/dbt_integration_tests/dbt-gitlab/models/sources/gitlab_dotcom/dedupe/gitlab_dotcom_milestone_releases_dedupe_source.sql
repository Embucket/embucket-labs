{{ config({
    "materialized": "incremental",
    "unique_key": "primary_key"
    })
}}

WITH base AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'milestone_releases') }}
    {% if is_incremental() %}

    WHERE _uploaded_at >= (SELECT MAX(_uploaded_at) FROM {{this}})

    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY milestone_id, release_id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

    SELECT {{ dbt_utils.generate_surrogate_key( ['milestone_id', 'release_id'])}} AS primary_key,
           milestone_id::INT                                             AS milestone_id,
           release_id::INT                                               AS release_id,
           _uploaded_at                                                  AS _uploaded_at
    FROM base

)

SELECT *
FROM renamed