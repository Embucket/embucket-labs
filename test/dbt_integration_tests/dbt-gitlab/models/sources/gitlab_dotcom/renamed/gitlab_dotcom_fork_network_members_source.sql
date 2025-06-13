WITH source AS (

   SELECT *
   FROM {{ ref('gitlab_dotcom_fork_network_members_dedupe_source') }}

), renamed AS (

   SELECT
     id::NUMBER                                AS id,
     fork_network_id::NUMBER                   AS fork_network_id,
     project_id::NUMBER                        AS project_id,
     forked_from_project_id::NUMBER            AS forked_from_project_id
   FROM source
  
)

SELECT *
FROM renamed