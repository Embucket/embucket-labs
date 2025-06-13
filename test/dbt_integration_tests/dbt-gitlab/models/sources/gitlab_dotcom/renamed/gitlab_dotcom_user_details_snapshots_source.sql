WITH source AS (

    SELECT *
    FROM {{ source('snapshots','gitlab_dotcom_user_details_snapshots') }}

), renamed AS (

    SELECT
      user_id::NUMBER AS user_id,
      job_title::VARCHAR AS job_title,
      registration_objective::NUMBER AS registration_objective,
      dbt_valid_from::TIMESTAMP AS dbt_valid_from,
      dbt_valid_to::TIMESTAMP AS dbt_valid_to
    FROM source
    
)

SELECT  *
FROM renamed