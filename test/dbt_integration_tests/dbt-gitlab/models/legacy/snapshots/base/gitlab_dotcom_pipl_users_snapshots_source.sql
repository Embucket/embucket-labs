WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_pipl_users_snapshots') }}

    QUALIFY ROW_NUMBER() OVER (
    PARTITION BY 
        dbt_valid_from::DATE, 
        user_id 
    ORDER BY dbt_valid_from DESC
    ) = 1

), renamed AS(

    SELECT

      -- ids
      dbt_scd_id                                                        AS pipl_user_snapshot_id,
      user_id                                                           AS user_id,
     
      -- attributes
      initial_email_sent_at                                             AS initial_email_sent_at,
      last_access_from_pipl_country_at                                  AS last_access_from_pipl_country_at,

      -- metadata
      created_at                                                        AS created_at,
      updated_at                                                        AS updated_at,

      --dbt last run
      CURRENT_TIMESTAMP()                                               AS _last_dbt_run,

      -- snapshot metadata
      dbt_updated_at,
      dbt_valid_from                                                   AS valid_from,
      dbt_valid_to                                                     AS valid_to

    FROM source

)

SELECT *
FROM renamed