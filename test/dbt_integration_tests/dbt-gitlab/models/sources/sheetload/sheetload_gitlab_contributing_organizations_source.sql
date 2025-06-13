WITH source AS (
    
    SELECT * 
    FROM {{ source('sheetload','gitlab_contributing_organizations') }}

), renamed AS (

    SELECT
      contributor_organization::VARCHAR      AS contributor_organization,
      contributor_usernames::VARCHAR         AS contributor_usernames,
      sfdc_account_id::VARCHAR               AS sfdc_account_id
    FROM source

)

SELECT *
FROM renamed
