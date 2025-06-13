{{ config({
    "materialized": "view",
    })
}}

SELECT *
FROM {{ ref('google_analytics_4_pseudonymous_users_source') }}
