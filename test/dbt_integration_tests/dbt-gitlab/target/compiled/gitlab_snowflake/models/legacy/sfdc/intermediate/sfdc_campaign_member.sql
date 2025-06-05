WITH source AS (

    SELECT *
    FROM EMBUCKET.sfdc.sfdc_campaign_member_source
    WHERE is_deleted = FALSE

)

SELECT *
FROM source