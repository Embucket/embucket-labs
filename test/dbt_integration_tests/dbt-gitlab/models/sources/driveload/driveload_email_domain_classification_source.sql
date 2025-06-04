WITH source AS (

    SELECT *
    FROM {{ source('driveload', 'email_domain_classification') }}

)

SELECT
  domain::VARCHAR               AS domain,
  classification::VARCHAR       AS classification
FROM source
