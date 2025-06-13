WITH tiers AS (

    SELECT *
    FROM {{ ref('prep_product_tier') }}
    WHERE product_deployment_type IN ('Self-Managed', 'Dedicated')

), license AS (

    SELECT *
    FROM {{ ref('prep_license') }}

), environment AS (

    SELECT *
    FROM {{ ref('prep_environment') }}

), dedicated_md5 AS (

    SELECT DISTINCT
      license_md5
    FROM {{ ref('prep_ping_instance') }}
    WHERE is_saas_dedicated = TRUE

), dedicated_sha256 AS (

    SELECT DISTINCT
      license_sha256
    FROM {{ ref('prep_ping_instance') }}
    WHERE is_saas_dedicated = TRUE

), final AS (

    SELECT
      -- Primary key
      license.dim_license_id,

     -- Foreign keys
      license.dim_subscription_id,
      license.dim_subscription_id_original,
      license.dim_subscription_id_previous,
      environment.dim_environment_id,
      tiers.dim_product_tier_id,

      -- Descriptive information
      license.license_md5,
      license.license_sha256,
      license.subscription_name,
      license.environment,
      license.license_user_count,
      license.license_plan,
      license.is_trial,
      license.is_internal,
      license.company,
      license.license_start_date,
      license.license_expire_date,
      license.created_at,
      license.updated_at
    FROM license
    LEFT JOIN dedicated_md5
      ON license.license_md5 = dedicated_md5.license_md5
    LEFT JOIN dedicated_sha256
      ON license.license_sha256 = dedicated_sha256.license_sha256
    LEFT JOIN tiers  -- The product information in the license does not distinguish between Self-managed and Dedicated
                     -- Because of this, we join to the ping data to detect which licenses are from Self-managed and which from Dedicated
      ON LOWER(tiers.product_tier_historical_short) = license.license_plan
      AND tiers.product_deployment_type =  CASE
                                              WHEN COALESCE(dedicated_sha256.license_sha256, dedicated_md5.license_md5) IS NULL
                                                THEN 'Self-Managed'
                                              ELSE 'Dedicated'
                                            END 
    LEFT JOIN environment
      ON environment.environment = license.environment
)


{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@jpeguero",
    created_date="2021-01-08",
    updated_date="2023-07-04"
) }}
