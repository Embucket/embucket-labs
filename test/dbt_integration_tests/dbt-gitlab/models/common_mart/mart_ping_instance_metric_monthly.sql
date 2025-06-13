{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ simple_cte([
    ('dim_ping_instance', 'dim_ping_instance'),    
    ('dim_date', 'dim_date'), 
    ('dim_location', 'dim_location_country'),
    ('dim_ping_metric', 'dim_ping_metric'),    
    ('dim_app_release_major_minor', 'dim_app_release_major_minor')
    ])

}}

,  fct_ping_instance_metric AS  (

  SELECT *
  FROM {{ ref('fct_ping_instance_metric_monthly') }}
  WHERE IS_REAL(TO_VARIANT(metric_value))

), 

license_subscriptions_w_latest_subscription AS (
  SELECT *
  FROM {{ ref('prep_license_subscription') }}
),
    
bdg_license_instance AS (
  SELECT DISTINCT
    dim_ping_instance_id,
    license_md5,
    license_sha256
  FROM {{ ref('dim_ping_instance') }}
  WHERE COALESCE(license_md5, license_sha256) IS NOT NULL
),

license_subscriptions_instance AS (
  SELECT
    license_subscriptions.*,
    bdg_license_instance.dim_ping_instance_id
  FROM license_subscriptions_w_latest_subscription AS license_subscriptions
  LEFT JOIN bdg_license_instance
    ON license_subscriptions.license_md5 = bdg_license_instance.license_md5

  UNION

  SELECT
    license_subscriptions.*,
    bdg_license_instance.dim_ping_instance_id
  FROM license_subscriptions_w_latest_subscription AS license_subscriptions
  LEFT JOIN bdg_license_instance
    ON license_subscriptions.license_sha256 = bdg_license_instance.license_sha256
),

  joined AS (

      SELECT
        fct_ping_instance_metric.ping_instance_metric_monthly_pk                                                                        AS ping_instance_metric_monthly_pk,
        fct_ping_instance_metric.dim_ping_date_id                                                                                       AS dim_ping_date_id,
        fct_ping_instance_metric.dim_license_id                                                                                         AS dim_license_id,
        fct_ping_instance_metric.dim_installation_id                                                                                    AS dim_installation_id,
        fct_ping_instance_metric.dim_ping_instance_id                                                                                   AS dim_ping_instance_id,
        fct_ping_instance_metric.metrics_path                                                                                           AS metrics_path,
        fct_ping_instance_metric.metric_value                                                                                           AS metric_value,
        fct_ping_instance_metric.monthly_metric_value                                                                                   AS monthly_metric_value,
        fct_ping_instance_metric.has_timed_out                                                                                          AS has_timed_out,
        dim_ping_metric.time_frame                                                                                                      AS time_frame,
        dim_ping_metric.group_name                                                                                                      AS group_name,
        dim_ping_metric.stage_name                                                                                                      AS stage_name,
        dim_ping_metric.section_name                                                                                                    AS section_name,
        dim_ping_metric.is_smau                                                                                                         AS is_smau,
        dim_ping_metric.is_gmau                                                                                                         AS is_gmau,
        dim_ping_metric.is_paid_gmau                                                                                                    AS is_paid_gmau,
        dim_ping_metric.is_umau                                                                                                         AS is_umau,
        dim_ping_instance.license_sha256                                                                                                AS license_sha256,
        dim_ping_instance.license_md5                                                                                                   AS license_md5,
        dim_ping_instance.is_trial                                                                                                      AS is_trial,
        fct_ping_instance_metric.umau_value                                                                                             AS umau_value,
        license_subscriptions.license_id                                                                                                AS license_id,
        license_subscriptions.license_company_name                                                                                      AS license_company_name,
        license_subscriptions.latest_subscription_id                                                                                    AS latest_subscription_id,
        license_subscriptions.original_subscription_name_slugify                                                                        AS original_subscription_name_slugify,
        license_subscriptions.product_category_array                                                                                    AS product_category_array,
        license_subscriptions.product_rate_plan_name_array                                                                              AS product_rate_plan_name_array,
        license_subscriptions.subscription_name                                                                                         AS subscription_name,
        license_subscriptions.subscription_start_month                                                                                  AS subscription_start_month,
        license_subscriptions.subscription_end_month                                                                                    AS subscription_end_month,
        license_subscriptions.dim_billing_account_id                                                                                    AS dim_billing_account_id,
        license_subscriptions.dim_crm_account_id                                                                                        AS dim_crm_account_id,
        license_subscriptions.crm_account_name                                                                                          AS crm_account_name,
        license_subscriptions.dim_parent_crm_account_id                                                                                 AS dim_parent_crm_account_id,
        license_subscriptions.parent_crm_account_name                                                                                   AS parent_crm_account_name,
        license_subscriptions.parent_crm_account_upa_country                                                                            AS parent_crm_account_upa_country,
        license_subscriptions.parent_crm_account_sales_segment                                                                          AS parent_crm_account_sales_segment,
        license_subscriptions.parent_crm_account_industry                                                                               AS parent_crm_account_industry,
        license_subscriptions.parent_crm_account_territory                                                                              AS parent_crm_account_territory,
        license_subscriptions.technical_account_manager                                                                                 AS technical_account_manager,
        CASE
          WHEN license_subscriptions.license_expire_date < dim_ping_instance.ping_created_at THEN FALSE
          WHEN license_subscriptions.is_paid_subscription = TRUE THEN TRUE
          ELSE FALSE
        END                                                                                                                             AS is_paid_subscription,
        COALESCE(license_subscriptions.is_program_subscription, FALSE)                                                                  AS is_program_subscription,
        dim_ping_instance.ping_delivery_type                                                                                            AS ping_delivery_type,
        dim_ping_instance.ping_deployment_type                                                                                          AS ping_deployment_type,
        dim_ping_instance.ping_edition                                                                                                  AS ping_edition,
        dim_ping_instance.product_tier                                                                                                  AS ping_product_tier,
        dim_ping_instance.ping_edition || ' - ' || dim_ping_instance.product_tier                                                       AS ping_edition_product_tier,
        dim_ping_instance.major_version                                                                                                 AS major_version,
        dim_ping_instance.minor_version                                                                                                 AS minor_version,
        dim_ping_instance.major_minor_version                                                                                           AS major_minor_version,
        dim_app_release_major_minor.major_minor_version_num                                                                             AS major_minor_version_num,
        dim_ping_instance.major_minor_version_id                                                                                        AS major_minor_version_id,
        dim_ping_instance.version_is_prerelease                                                                                         AS version_is_prerelease,
        IFF(DATEDIFF('days', dim_app_release_major_minor.release_date, fct_ping_instance_metric.ping_created_at) < 0 AND version_is_prerelease = FALSE,
          0, DATEDIFF('days', dim_app_release_major_minor.release_date, fct_ping_instance_metric.ping_created_at))
                                                                                                                                        AS days_after_version_release_date,
        latest_version.major_minor_version                                                                                              AS latest_version_available_at_ping_creation,
        IFF(latest_version.version_number - dim_app_release_major_minor.version_number < 0 AND version_is_prerelease = FALSE,
          0, latest_version.version_number - dim_app_release_major_minor.version_number)                                                AS versions_behind_latest_at_ping_creation,
        dim_ping_instance.is_internal                                                                                                   AS is_internal,
        dim_ping_instance.is_staging                                                                                                    AS is_staging,
        dim_ping_instance.instance_user_count                                                                                           AS instance_user_count,
        dim_ping_instance.ping_created_at                                                                                               AS ping_created_at,
        dim_date.first_day_of_month                                                                                                     AS ping_created_date_month,
        fct_ping_instance_metric.dim_host_id                                                                                            AS dim_host_id,
        fct_ping_instance_metric.dim_instance_id                                                                                        AS dim_instance_id,
        dim_ping_instance.host_name                                                                                                     AS host_name,
        dim_ping_instance.is_last_ping_of_month                                                                                         AS is_last_ping_of_month,
        fct_ping_instance_metric.dim_location_country_id                                                                                AS dim_location_country_id,
        dim_location.country_name                                                                                                       AS country_name,
        dim_location.iso_2_country_code                                                                                                 AS iso_2_country_code
      FROM fct_ping_instance_metric
      LEFT JOIN dim_ping_metric
        ON fct_ping_instance_metric.metrics_path = dim_ping_metric.metrics_path
      INNER JOIN dim_date
        ON fct_ping_instance_metric.dim_ping_date_id = dim_date.date_id
      LEFT JOIN dim_ping_instance
        ON fct_ping_instance_metric.dim_ping_instance_id = dim_ping_instance.dim_ping_instance_id
      LEFT JOIN license_subscriptions_instance AS license_subscriptions
        ON dim_date.first_day_of_month = license_subscriptions.reporting_month
        AND fct_ping_instance_metric.dim_ping_instance_id = license_subscriptions.dim_ping_instance_id
      LEFT JOIN dim_location
        ON fct_ping_instance_metric.dim_location_country_id = dim_location.dim_location_country_id
      LEFT JOIN dim_app_release_major_minor
        ON fct_ping_instance_metric.dim_app_release_major_minor_sk = dim_app_release_major_minor.dim_app_release_major_minor_sk
      LEFT JOIN dim_app_release_major_minor AS latest_version
        ON fct_ping_instance_metric.dim_latest_available_app_release_major_minor_sk = latest_version.dim_app_release_major_minor_sk
     WHERE dim_ping_instance.ping_deployment_type IN ('Self-Managed', 'Dedicated')
        OR (dim_ping_instance.ping_delivery_type = 'SaaS' AND fct_ping_instance_metric.dim_installation_id = '8b52effca410f0a380b0fcffaa1260e7')

), sorted AS (

    SELECT

      -- Primary Key
      ping_instance_metric_monthly_pk,

      -- Outdated, Misstated Primary Key
      {{ dbt_utils.generate_surrogate_key(['dim_ping_instance_id', 'metrics_path']) }} AS ping_instance_metric_id,

      dim_ping_date_id,
      metrics_path,
      metric_value,
      monthly_metric_value,
      has_timed_out,
      dim_ping_instance_id,

      --Foreign Key
      dim_instance_id,
      dim_license_id,
      dim_installation_id,
      latest_subscription_id,
      dim_billing_account_id,
      dim_crm_account_id,
      dim_parent_crm_account_id,
      major_minor_version_id,
      dim_host_id,
      host_name,
      -- metadata usage ping
      ping_delivery_type,
      ping_deployment_type,
      ping_edition,
      ping_product_tier,
      ping_edition_product_tier,
      major_version,
      minor_version,
      major_minor_version,
      major_minor_version_num,
      version_is_prerelease,
      days_after_version_release_date,
      latest_version_available_at_ping_creation,
      versions_behind_latest_at_ping_creation,
      is_internal,
      is_staging,
      is_trial,
      umau_value,

      -- metadata metrics

      group_name,
      stage_name,
      section_name,
      is_smau,
      is_gmau,
      is_paid_gmau,
      is_umau,
      time_frame,

      --metadata instance
      instance_user_count,

      --metadata subscription
      original_subscription_name_slugify,
      subscription_name,
      subscription_start_month,
      subscription_end_month,
      product_category_array,
      product_rate_plan_name_array,
      is_paid_subscription,
      is_program_subscription,

      -- account metadata
      crm_account_name,
      parent_crm_account_name,
      parent_crm_account_upa_country,
      parent_crm_account_sales_segment,
      parent_crm_account_industry,
      parent_crm_account_territory,
      technical_account_manager,

      ping_created_at,
      ping_created_date_month,
      is_last_ping_of_month


    FROM joined
    WHERE time_frame != 'none'
      AND TRY_TO_DECIMAL(monthly_metric_value::TEXT) >= 0

)

{{ dbt_audit(
    cte_ref="sorted",
    created_by="@icooper-acp",
    updated_by="@michellecooper",
    created_date="2022-03-11",
    updated_date="2024-07-17"
) }}
