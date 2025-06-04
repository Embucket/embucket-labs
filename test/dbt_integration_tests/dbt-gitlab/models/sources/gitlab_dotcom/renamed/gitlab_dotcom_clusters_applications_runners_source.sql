    
WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_clusters_applications_runners_dedupe_source') }}
    
), 

renamed AS (
    
    SELECT
      id::NUMBER              AS clusters_applications_runners_id,
      cluster_id::NUMBER      AS cluster_id,
      created_at::TIMESTAMP    AS created_at,
      updated_at::TIMESTAMP    AS updated_at,
      status::NUMBER          AS status,
      version::VARCHAR         AS version,
      status_reason::VARCHAR   AS status_reason
    FROM source

)


SELECT *
FROM renamed
