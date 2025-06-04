{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "incremental",
    on_schema_change='sync_all_columns',
    unique_key = "ping_instance_metric_id"
) }}


{{ simple_cte([
    ('prep_subscription', 'prep_subscription'),
    ('dim_date', 'dim_date'),
    ('map_ip_to_country', 'map_ip_to_country'),
    ('locations', 'prep_location_country'),
    ('prep_ping_instance_flattened', 'prep_ping_instance_flattened'),
    ('dim_product_tier', 'dim_product_tier'),
    ('prep_app_release_major_minor', 'prep_app_release_major_minor')
    ])

}}
, prep_subscription_md5 AS (

    SELECT dim_subscription_id
    FROM {{ ref('prep_subscription') }}

), prep_subscription_sha256 AS (

    SELECT dim_subscription_id
    FROM {{ ref('prep_subscription') }}

), prep_license AS (

    SELECT
      license_md5,
      license_sha256,
      dim_license_id,
      dim_subscription_id
    FROM {{ ref('prep_license') }}

), prep_license_md5 AS (

    SELECT
      license_md5,
      dim_license_id,
      dim_subscription_id
    FROM prep_license
    WHERE license_md5 IS NOT NULL

), prep_license_sha256 AS (

    SELECT
      license_sha256,
      dim_license_id,
      dim_subscription_id
    FROM prep_license
    WHERE license_sha256 IS NOT NULL

), map_ip_location AS (

    SELECT
      map_ip_to_country.ip_address_hash                 AS ip_address_hash,
      map_ip_to_country.dim_location_country_id         AS dim_location_country_id
    FROM map_ip_to_country
    INNER JOIN locations
      WHERE map_ip_to_country.dim_location_country_id = locations.dim_location_country_id

), source AS (

    SELECT
      prep_ping_instance_flattened.*,
      REGEXP_REPLACE(NULLIF(prep_ping_instance_flattened.version, ''), '[^0-9.]+')    AS cleaned_version,
      SPLIT_PART(cleaned_version, '.', 1)::NUMBER                                     AS major_version,
      SPLIT_PART(cleaned_version, '.', 2)::NUMBER                                     AS minor_version,
      major_version || '.' || minor_version                                           AS major_minor_version
    FROM prep_ping_instance_flattened
      {% if is_incremental() %}
                  WHERE uploaded_at >= (SELECT MAX(uploaded_at) FROM {{this}})
      {% endif %}

), add_country_info_to_usage_ping AS (

    SELECT
      source.*,
      map_ip_location.dim_location_country_id     AS dim_location_country_id
    FROM source
    LEFT JOIN map_ip_location
      ON source.ip_address_hash = map_ip_location.ip_address_hash

), prep_usage_ping_cte AS (

    SELECT
      dim_ping_instance_id                                          AS dim_ping_instance_id,
      dim_host_id                                                   AS dim_host_id,
      dim_instance_id                                               AS dim_instance_id,
      dim_installation_id                                           AS dim_installation_id,
      dim_product_tier.dim_product_tier_id                          AS dim_product_tier_id,
      prep_app_release_major_minor.dim_app_release_major_minor_sk   AS dim_app_release_major_minor_sk,
      latest_version.dim_app_release_major_minor_sk                 AS dim_latest_available_app_release_major_minor_sk,
      ping_created_at                                               AS ping_created_at,
      uploaded_at                                                   AS uploaded_at,
      license_md5                                                   AS license_md5,
      license_sha256                                                AS license_sha256,
      dim_location_country_id                                       AS dim_location_country_id,
      license_trial_ends_on                                         AS license_trial_ends_on,
      license_subscription_id                                       AS license_subscription_id,
      umau_value                                                    AS umau_value,
      duo_pro_purchased_seats                                       AS duo_pro_purchased_seats,
      duo_pro_assigned_seats                                        AS duo_pro_assigned_seats,
      duo_enterprise_purchased_seats                                AS duo_enterprise_purchased_seats,
      duo_enterprise_assigned_seats                                 AS duo_enterprise_assigned_seats,
      product_tier                                                  AS product_tier,
      main_edition                                                  AS main_edition,
      metrics_path                                                  AS metrics_path,
      metric_value                                                  AS metric_value,
      has_timed_out                                                 AS has_timed_out,
    FROM add_country_info_to_usage_ping
    LEFT JOIN dim_product_tier
    ON TRIM(LOWER(add_country_info_to_usage_ping.product_tier)) = TRIM(LOWER(dim_product_tier.product_tier_historical_short))
    AND add_country_info_to_usage_ping.ping_deployment_type = dim_product_tier.product_deployment_type
    LEFT JOIN prep_app_release_major_minor
      ON prep_app_release_major_minor.major_minor_version = add_country_info_to_usage_ping.major_minor_version
      AND prep_app_release_major_minor.application = 'GitLab'
    LEFT JOIN prep_app_release_major_minor AS latest_version -- Join the latest version released at the time of the ping.
      ON add_country_info_to_usage_ping.ping_created_at BETWEEN latest_version.release_date AND {{ coalesce_to_infinity('latest_version.next_version_release_date') }}
      AND latest_version.application = 'GitLab'
    QUALIFY RANK() OVER(PARTITION BY add_country_info_to_usage_ping.dim_ping_instance_id ORDER BY latest_version.release_date DESC) = 1
    -- Adding the QUALIFY statement because of the latest_version CTE. There is rare case when the ping_created_at is right between the last day of a release and when the new one comes out.
    -- This causes two records to be matched and then we have two records per one ping.
    -- The rank statements gets rid of this. Using rank instead row_number since rank will preserve other might be duplicates in the data, while rank only addresses
    -- the duplicates that are entered in the data consequence of the latest_version CTE join condition.

), joined_payload AS (

    SELECT
      prep_usage_ping_cte.*,
      COALESCE(prep_license_md5.dim_license_id, prep_license_sha256.dim_license_id)                                                AS dim_license_id,
      dim_date.date_id                                                                                                             AS dim_ping_date_id,
      COALESCE(COALESCE(prep_subscription_md5.dim_subscription_id,prep_subscription_sha256.dim_subscription_id), license_subscription_id) AS dim_subscription_id,
      IFF(prep_usage_ping_cte.ping_created_at < license_trial_ends_on, TRUE, FALSE)                                                AS is_trial,
      IFF(COALESCE(prep_license_md5.dim_subscription_id,prep_license_sha256.dim_subscription_id) IS NOT NULL, TRUE, FALSE)         AS is_license_mapped_to_subscription, -- does the license table have a value in both license_id and subscription_id
      IFF(COALESCE(prep_subscription_md5.dim_subscription_id,prep_subscription_sha256.dim_subscription_id) IS NULL, FALSE, TRUE)          AS is_license_subscription_id_valid   -- is the subscription_id in the license table valid (does it exist in the subscription table?)
    FROM prep_usage_ping_cte
    LEFT JOIN prep_license_md5
      ON prep_usage_ping_cte.license_md5    = prep_license_md5.license_md5
    LEFT JOIN prep_license_sha256
      ON prep_usage_ping_cte.license_sha256 = prep_license_sha256.license_sha256
    LEFT JOIN prep_subscription_md5
      ON prep_license_md5.dim_subscription_id = prep_subscription_md5.dim_subscription_id
    LEFT JOIN prep_subscription_sha256
      ON prep_license_sha256.dim_subscription_id = prep_subscription_sha256.dim_subscription_id
    LEFT JOIN dim_date
      ON TO_DATE(prep_usage_ping_cte.ping_created_at) = dim_date.date_day

), flattened_high_level as (
    SELECT
      {{ dbt_utils.generate_surrogate_key(['dim_ping_instance_id', 'joined_payload.metrics_path']) }}                      AS ping_instance_metric_id,
      dim_ping_instance_id                                                                                        AS dim_ping_instance_id,
      dim_app_release_major_minor_sk                                                                              AS dim_app_release_major_minor_sk,
      dim_latest_available_app_release_major_minor_sk                                                             AS dim_latest_available_app_release_major_minor_sk,
      joined_payload.metrics_path                                                                                 AS metrics_path,
      metric_value                                                                                                AS metric_value,
      has_timed_out                                                                                               AS has_timed_out,
      dim_product_tier_id                                                                                         AS dim_product_tier_id,
      dim_subscription_id                                                                                         AS dim_subscription_id,
      dim_location_country_id                                                                                     AS dim_location_country_id,
      dim_ping_date_id                                                                                            AS dim_ping_date_id,
      dim_instance_id                                                                                             AS dim_instance_id,
      dim_host_id                                                                                                 AS dim_host_id,
      dim_installation_id                                                                                         AS dim_installation_id,
      dim_license_id                                                                                              AS dim_license_id,
      license_md5                                                                                                 AS license_md5,
      license_sha256                                                                                              AS license_sha256,
      ping_created_at                                                                                             AS ping_created_at,
      uploaded_at                                                                                                 AS uploaded_at,
      ping_created_at::DATE                                                                                       AS ping_created_date,
      umau_value                                                                                                  AS umau_value,
      duo_pro_purchased_seats                                                                                     AS duo_pro_purchased_seats,
      duo_pro_assigned_seats                                                                                      AS duo_pro_assigned_seats,
      duo_enterprise_purchased_seats                                                                              AS duo_enterprise_purchased_seats,
      duo_enterprise_assigned_seats                                                                               AS duo_enterprise_assigned_seats,
      license_subscription_id                                                                                     AS dim_subscription_license_id,
      is_license_mapped_to_subscription                                                                           AS is_license_mapped_to_subscription,
      is_license_subscription_id_valid                                                                            AS is_license_subscription_id_valid,
      IFF(dim_license_id IS NULL, FALSE, TRUE)                                                                    AS is_service_ping_license_in_customerDot,
      'VERSION_DB'                                                                                                AS data_source,
  FROM joined_payload

)

{{ dbt_audit(
    cte_ref="flattened_high_level",
    created_by="@icooper-acp",
    updated_by="@utkarsh060",
    created_date="2022-03-08",
    updated_date="2024-10-29"
) }}
