WITH source AS (

    SELECT *
    FROM {{ source('zendesk', 'organizations') }}

),

renamed AS (

    SELECT

        --ids
        id                                                  AS organization_id,
        organization_fields['salesforce_id']::VARCHAR       AS sfdc_account_id,

        --fields
        name                                                AS organization_name,
        tags                                                AS organization_tags,
        organization_fields['aar']::NUMBER                  AS arr,
        organization_fields['market_segment']::VARCHAR      AS organization_market_segment,
        organization_fields['org_region']::VARCHAR          AS organization_region,
        --dates
        created_at,
        updated_at

    FROM source

)

SELECT *
FROM renamed
