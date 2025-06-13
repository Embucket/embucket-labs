WITH source AS (

    SELECT *
    FROM {{ ref('google_ads_ad_group_hourly_stats_source') }}

)

SELECT *
FROM source

