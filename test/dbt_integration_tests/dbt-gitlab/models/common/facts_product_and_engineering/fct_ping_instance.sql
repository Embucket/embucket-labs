{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "incremental",
    on_schema_change='sync_all_columns',
    unique_key = "ping_instance_id"
) }}

{{ simple_cte([
    ('prep_license', 'prep_license'),
    ('prep_subscription', 'prep_subscription'),
    ('dim_date', 'dim_date'),
    ('map_ip_to_country', 'map_ip_to_country'),
    ('locations', 'prep_location_country'),
    ('dim_product_tier', 'dim_product_tier'),
    ('prep_ping_instance', 'prep_ping_instance'),
    ('dim_crm_account','dim_crm_account'),
    ('prep_app_release_major_minor', 'prep_app_release_major_minor'),
    ('dim_installation', 'dim_installation')
    ])

}}

, map_ip_location AS (

    SELECT
      map_ip_to_country.ip_address_hash                 AS ip_address_hash,
      map_ip_to_country.dim_location_country_id         AS dim_location_country_id
    FROM map_ip_to_country
    INNER JOIN locations
      WHERE map_ip_to_country.dim_location_country_id = locations.dim_location_country_id

), source AS (

    SELECT
      prep_ping_instance.*
    FROM prep_ping_instance
      {% if is_incremental() %}
                  WHERE uploaded_at >= (SELECT MAX(uploaded_at) FROM {{this}})
      {% endif %}

), add_country_info_to_usage_ping AS (

    SELECT
      source.*,
      REGEXP_REPLACE(NULLIF(source.version, ''), '[^0-9.]+')                                              AS cleaned_version,
      SPLIT_PART(cleaned_version, '.', 1)::NUMBER                                                         AS major_version,
      SPLIT_PART(cleaned_version, '.', 2)::NUMBER                                                         AS minor_version,
      major_version || '.' || minor_version                                                               AS major_minor_version,
      map_ip_location.dim_location_country_id                                                             AS dim_location_country_id

    FROM source
    LEFT JOIN map_ip_location
      ON source.ip_address_hash = map_ip_location.ip_address_hash

), prep_usage_ping_cte AS (

    SELECT
      add_country_info_to_usage_ping.dim_ping_instance_id                                AS dim_ping_instance_id,
      add_country_info_to_usage_ping.dim_host_id                                         AS dim_host_id,
      add_country_info_to_usage_ping.dim_instance_id                                     AS dim_instance_id,
      add_country_info_to_usage_ping.dim_installation_id                                 AS dim_installation_id,
      dim_product_tier.dim_product_tier_id                                               AS dim_product_tier_id,
      prep_app_release_major_minor.dim_app_release_major_minor_sk                        AS dim_app_release_major_minor_sk,
      latest_version.dim_app_release_major_minor_sk                                      AS dim_latest_available_app_release_major_minor_sk,
      add_country_info_to_usage_ping.ping_created_at                                     AS ping_created_at,
      add_country_info_to_usage_ping.uploaded_at                                         AS uploaded_at,
      add_country_info_to_usage_ping.hostname                                            AS hostname,
      add_country_info_to_usage_ping.license_sha256                                      AS license_sha256,
      add_country_info_to_usage_ping.license_md5                                         AS license_md5,
      add_country_info_to_usage_ping.dim_location_country_id                             AS dim_location_country_id,
      add_country_info_to_usage_ping.license_trial_ends_on                               AS license_trial_ends_on,
      add_country_info_to_usage_ping.license_subscription_id                             AS license_subscription_id,
      add_country_info_to_usage_ping.license_billable_users                              AS license_billable_users,
      add_country_info_to_usage_ping.Instance_user_count                                 AS instance_user_count,
      add_country_info_to_usage_ping.historical_max_users                                AS historical_max_users,
      add_country_info_to_usage_ping.license_user_count                                  AS license_user_count,
      add_country_info_to_usage_ping.umau_value                                          AS umau_value,
      add_country_info_to_usage_ping.duo_pro_purchased_seats                             AS duo_pro_purchased_seats,
      add_country_info_to_usage_ping.duo_pro_assigned_seats                              AS duo_pro_assigned_seats,
      add_country_info_to_usage_ping.duo_enterprise_purchased_seats                      AS duo_enterprise_purchased_seats,
      add_country_info_to_usage_ping.duo_enterprise_assigned_seats                       AS duo_enterprise_assigned_seats,
      add_country_info_to_usage_ping.product_tier                                        AS product_tier,
      add_country_info_to_usage_ping.main_edition                                        AS main_edition,
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

), prep_usage_ping_and_license AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['prep_usage_ping_cte.dim_ping_instance_id']) }}                AS ping_instance_id,
      prep_usage_ping_cte.dim_ping_instance_id                                                            AS dim_ping_instance_id,
      prep_usage_ping_cte.ping_created_at                                                                 AS ping_created_at,
      prep_usage_ping_cte.uploaded_at                                                                     AS uploaded_at,
      prep_usage_ping_cte.dim_product_tier_id                                                             AS dim_product_tier_id,
      COALESCE(sha256.dim_subscription_id, md5.dim_subscription_id)                                       AS dim_subscription_id,
      prep_usage_ping_cte.license_subscription_id                                                         AS license_subscription_id,
      prep_usage_ping_cte.dim_location_country_id                                                         AS dim_location_country_id,
      dim_date.date_id                                                                                    AS dim_ping_date_id,
      prep_usage_ping_cte.dim_instance_id                                                                 AS dim_instance_id,
      prep_usage_ping_cte.dim_host_id                                                                     AS dim_host_id,
      prep_usage_ping_cte.dim_installation_id                                                             AS dim_installation_id,
      COALESCE(sha256.dim_license_id, md5.dim_license_id)                                                 AS dim_license_id,
      prep_usage_ping_cte.dim_app_release_major_minor_sk                                                  AS dim_app_release_major_minor_sk,
      prep_usage_ping_cte.dim_latest_available_app_release_major_minor_sk                                 AS dim_latest_available_app_release_major_minor_sk,
      prep_usage_ping_cte.license_sha256                                                                  AS license_sha256,
      prep_usage_ping_cte.license_md5                                                                     AS license_md5,
      prep_usage_ping_cte.license_billable_users                                                          AS license_billable_users,
      prep_usage_ping_cte.Instance_user_count                                                             AS instance_user_count,
      prep_usage_ping_cte.historical_max_users                                                            AS historical_max_user_count,
      prep_usage_ping_cte.license_user_count                                                              AS license_user_count,
      prep_usage_ping_cte.hostname                                                                        AS hostname,
      prep_usage_ping_cte.umau_value                                                                      AS umau_value,
      prep_usage_ping_cte.duo_pro_purchased_seats                                                         AS duo_pro_purchased_seats,
      prep_usage_ping_cte.duo_pro_assigned_seats                                                          AS duo_pro_assigned_seats,
      prep_usage_ping_cte.duo_enterprise_purchased_seats                                                  AS duo_enterprise_purchased_seats,
      prep_usage_ping_cte.duo_enterprise_assigned_seats                                                   AS duo_enterprise_assigned_seats,
      prep_usage_ping_cte.license_subscription_id                                                         AS dim_subscription_license_id,
      IFF(prep_usage_ping_cte.ping_created_at < prep_usage_ping_cte.license_trial_ends_on, TRUE, FALSE)   AS is_trial,
      prep_usage_ping_cte.product_tier                                                                    AS product_tier,
      prep_usage_ping_cte.main_edition                                                                    AS main_edition_product_tier,
      'VERSION_DB'                                                                                        AS data_source,
    FROM prep_usage_ping_cte
    LEFT JOIN prep_license AS md5
      ON prep_usage_ping_cte.license_md5 = md5.license_md5
    LEFT JOIN prep_license AS sha256
      ON prep_usage_ping_cte.license_sha256 = sha256.license_sha256
    LEFT JOIN dim_date
      ON TO_DATE(prep_usage_ping_cte.ping_created_at) = dim_date.date_day

), joined_payload AS (

    SELECT
      prep_usage_ping_and_license.ping_instance_id                                                           AS ping_instance_id,
      prep_usage_ping_and_license.dim_ping_instance_id                                                       AS dim_ping_instance_id,
      prep_usage_ping_and_license.ping_created_at                                                            AS ping_created_at,
      prep_usage_ping_and_license.uploaded_at                                                                AS uploaded_at,
      prep_usage_ping_and_license.dim_product_tier_id                                                        AS dim_product_tier_id,
      COALESCE(prep_usage_ping_and_license.license_subscription_id, prep_subscription.dim_subscription_id)   AS dim_subscription_id,
      prep_subscription.dim_crm_account_id                                                                   AS dim_crm_account_id,
      dim_crm_account.dim_parent_crm_account_id                                                              AS dim_parent_crm_account_id,
      prep_usage_ping_and_license.dim_location_country_id                                                    AS dim_location_country_id,
      prep_usage_ping_and_license.dim_ping_date_id                                                           AS dim_ping_date_id,
      prep_usage_ping_and_license.dim_instance_id                                                            AS dim_instance_id,
      prep_usage_ping_and_license.dim_host_id                                                                AS dim_host_id,
      prep_usage_ping_and_license.dim_installation_id                                                        AS dim_installation_id,
      prep_usage_ping_and_license.dim_license_id                                                             AS dim_license_id,
      prep_usage_ping_and_license.dim_app_release_major_minor_sk                                             AS dim_app_release_major_minor_sk,
      prep_usage_ping_and_license.dim_latest_available_app_release_major_minor_sk                            AS dim_latest_available_app_release_major_minor_sk,
      prep_usage_ping_and_license.license_sha256                                                             AS license_sha256,
      prep_usage_ping_and_license.license_md5                                                                AS license_md5,
      prep_usage_ping_and_license.license_billable_users                                                     AS license_billable_users,
      prep_usage_ping_and_license.instance_user_count                                                        AS instance_user_count,
      dim_installation.installation_creation_date                                                            AS installation_creation_date,
      prep_usage_ping_and_license.historical_max_user_count                                                  AS historical_max_user_count,
      prep_usage_ping_and_license.license_user_count                                                         AS license_user_count,
      prep_usage_ping_and_license.hostname                                                                   AS hostname,
      prep_usage_ping_and_license.umau_value                                                                 AS umau_value,
      prep_usage_ping_and_license.duo_pro_purchased_seats                                                    AS duo_pro_purchased_seats,
      prep_usage_ping_and_license.duo_pro_assigned_seats                                                     AS duo_pro_assigned_seats,
      prep_usage_ping_and_license.duo_enterprise_purchased_seats                                             AS duo_enterprise_purchased_seats,
      prep_usage_ping_and_license.duo_enterprise_assigned_seats                                              AS duo_enterprise_assigned_seats,
      prep_usage_ping_and_license.dim_subscription_license_id                                                AS dim_subscription_license_id,
      prep_usage_ping_and_license.is_trial                                                                   AS is_trial,
      prep_usage_ping_and_license.product_tier                                                               AS product_tier,
      prep_usage_ping_and_license.main_edition_product_tier                                                  AS main_edition_product_tier,
      'VERSION_DB'                                                                                           AS data_source
    FROM prep_usage_ping_and_license
    LEFT JOIN prep_subscription
      ON prep_usage_ping_and_license.dim_subscription_id = prep_subscription.dim_subscription_id
    LEFT JOIN dim_crm_account
      ON prep_subscription.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    LEFT JOIN dim_installation
      ON dim_installation.dim_installation_id = prep_usage_ping_and_license.dim_installation_id

)

{{ dbt_audit(
    cte_ref="joined_payload",
    created_by="@icooper-acp",
    updated_by="@utkarsh060",
    created_date="2022-03-08",
    updated_date="2024-10-29"
) }}
