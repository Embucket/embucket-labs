{{ config(

    materialized = "incremental",
    unique_key = "ping_instance_flattened_id",
    full_refresh = only_force_full_refresh(),
    on_schema_change = "sync_all_columns",
    tags=["product", "mnpi_exception"],
    tmp_relation_type = "table"
,
snowflake_warehouse=generate_warehouse_name('XL')
) }}


WITH source AS (

    SELECT
        *
    FROM {{ ref('prep_ping_instance')}} as usage
    {% if is_incremental() %}
          WHERE uploaded_at >= (SELECT MAX(uploaded_at) FROM {{this}})
    {% endif %}

) , flattened_high_level as (
      SELECT
        {{ dbt_utils.generate_surrogate_key(['dim_ping_instance_id', 'path']) }}                         AS ping_instance_flattened_id,
        dim_ping_instance_id                                                                    AS dim_ping_instance_id,
        dim_host_id                                                                             AS dim_host_id,
        dim_instance_id                                                                         AS dim_instance_id,
        dim_installation_id                                                                     AS dim_installation_id,
        ping_created_at                                                                         AS ping_created_at,
        uploaded_at                                                                             AS uploaded_at,
        ip_address_hash                                                                         AS ip_address_hash,
        license_md5                                                                             AS license_md5,
        license_sha256                                                                          AS license_sha256,
        original_edition                                                                        AS original_edition,
        main_edition                                                                            AS main_edition,
        product_tier                                                                            AS product_tier,
        is_dedicated_metric                                                                     AS is_dedicated_metric,
        is_dedicated_hostname                                                                   AS is_dedicated_hostname,
        is_saas_dedicated                                                                       AS is_saas_dedicated,
        ping_delivery_type                                                                      AS ping_delivery_type,
        ping_deployment_type                                                                    AS ping_deployment_type,
        license_trial_ends_on                                                                   AS license_trial_ends_on,
        license_subscription_id                                                                 AS license_subscription_id,
        umau_value                                                                              AS umau_value,
        duo_pro_purchased_seats                                                                 AS duo_pro_purchased_seats,
        duo_pro_assigned_seats                                                                  AS duo_pro_assigned_seats,
        duo_enterprise_purchased_seats                                                          AS duo_enterprise_purchased_seats,
        duo_enterprise_assigned_seats                                                           AS duo_enterprise_assigned_seats,
        path                                                                                    AS metrics_path,
        IFF(value = -1, 0, value)                                                               AS metric_value,
        IFF(value = -1, TRUE, FALSE)                                                            AS has_timed_out,
        version
      FROM source,
        LATERAL FLATTEN(input => raw_usage_data_payload,
        RECURSIVE => true)

  )

  {{ dbt_audit(
      cte_ref="flattened_high_level",
      created_by="@icooper-acp",
      updated_by="@utkarsh060",
      created_date="2022-03-17",
      updated_date="2024-10-29"
  ) }}
