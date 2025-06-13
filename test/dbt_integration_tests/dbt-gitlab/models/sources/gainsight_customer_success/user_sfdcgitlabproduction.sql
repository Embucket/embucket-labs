{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','user_sfdcgitlabproduction') }}
),

renamed AS (

  SELECT
    gsid::VARCHAR                                                                                                             AS gsid,
    _fivetran_deleted::BOOLEAN                                                                                                AS _fivetran_deleted,
    gs_user_gsid::VARCHAR                                                                                                     AS gs_user_gsid,
    email::VARCHAR                                                                                                            AS email,
    created_date::TIMESTAMP                                                                                                   AS created_date,
    sfdcuser_name::VARCHAR                                                                                                    AS sfdcuser_name,
    sfdc_user_id::VARCHAR                                                                                                     AS sfdc_user_id,
    modified_date::TIMESTAMP                                                                                                  AS modified_date,
    name::VARCHAR                                                                                                             AS name,
    _fivetran_synced::TIMESTAMP                                                                                               AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed
