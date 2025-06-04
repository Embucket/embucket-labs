WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_cluster_agents_dedupe_source') }}

), renamed AS (

    SELECT

      id::NUMBER                                               AS cluster_agent_id,
      project_id::NUMBER                                       AS project_id,
      created_by_user_id::NUMBER                               AS created_by_user_id,
      created_at::TIMESTAMP                                    AS created_at,
      updated_at::TIMESTAMP                                    AS updated_at

    FROM source

)

SELECT *
FROM renamed
