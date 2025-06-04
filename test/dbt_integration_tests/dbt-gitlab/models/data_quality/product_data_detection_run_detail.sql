{{ config(
    tags=["mnpi_exception"]
) }}

{{
  config({
      "materialized": "incremental",
      "unique_key": "primary_key",
      "full_refresh": false

  })
}}

{{ simple_cte([
  ('product_data_detection_rule_3', 'product_data_detection_rule_3'),
  ('dim_host_instance_type', 'dim_host_instance_type'),
  ('dim_license', 'dim_license'),
  ('dim_subscription', 'dim_subscription'),
  ('map_license_subscription_account', 'map_license_subscription_account'),
  ('fct_mrr', 'fct_mrr'),
  ('dim_product_detail', 'dim_product_detail'),
  ('dim_subscription', 'dim_subscription')
])}}

, rule_run_date AS (

   SELECT DISTINCT
     date_day AS rule_run_date,
     'Product' AS type_of_data
    FROM {{ ref('dim_date') }}
    WHERE rule_run_date BETWEEN '2021-06-23' AND TO_DATE(dbt_updated_at) --date when the code would be pushed to Production,we would be joining this with the dbt updated data for the models.

), bdg_namespace_order_subscription AS (

    SELECT *
    FROM {{ ref('bdg_namespace_order_subscription') }}
    WHERE is_subscription_active = 'Y'

), self_managed_subs_with_licenses AS (

    SELECT DISTINCT
      fct_mrr.dim_subscription_id,
      dim_subscription.subscription_name,
      IFF(dim_license.license_start_date > CURRENT_DATE, TRUE, FALSE)                    AS is_license_start_date_future,
      IFF(dim_license.license_start_date > dim_license.license_expire_date, TRUE, FALSE) AS is_license_start_date_greater_expire_date,
      fct_mrr.dbt_updated_at
    FROM fct_mrr
    LEFT JOIN dim_subscription
      ON dim_subscription.dim_subscription_id = fct_mrr.dim_subscription_id
    LEFT JOIN dim_product_detail
      ON fct_mrr.dim_product_detail_id = dim_product_detail.dim_product_detail_id
    LEFT JOIN dim_license
      ON dim_subscription.dim_subscription_id = dim_license.dim_subscription_id
    WHERE dim_product_detail.product_deployment_type IN ('Self-Managed', 'Dedicated')
      AND dim_subscription.subscription_start_date <= CURRENT_DATE

), expired_licenses_with_subs AS (

    SELECT DISTINCT
      dim_subscription.dim_subscription_id,
      dim_license.dim_license_id,
      dim_license.license_start_date,
      dim_license.License_expire_date,
      dim_subscription.subscription_start_date,
      dim_subscription.subscription_end_date,
      IFF(dim_license.license_expire_date <= CURRENT_DATE AND dim_subscription.subscription_end_date <= CURRENT_DATE,
        TRUE,
        FALSE)  AS is_license_expired_with_sub_end_date_past,
      dim_license.dbt_updated_at
    FROM dim_license
    LEFT JOIN dim_subscription
      ON dim_license.dim_subscription_id  = dim_subscription.dim_subscription_id
    WHERE license_expire_date <= CURRENT_DATE

), processed_passed_failed_record_count AS (

--Missing instance types for UUID or Namespaces
    SELECT
      1                                                                    AS rule_id,
      (COUNT(DISTINCT(instance_uuid)) + COUNT(DISTINCT(namespace_id)))     AS processed_record_count,
      (SELECT COUNT(DISTINCT(IFNULL(instance_uuid, namespace_id )))
        FROM dim_host_instance_type
        WHERE instance_type NOT IN ('Unknown'))                            AS passed_record_count,
      (processed_record_count - passed_record_count)                       AS failed_record_count,
      dbt_updated_at                                                       AS run_date
    FROM dim_host_instance_type
    GROUP BY run_date

  UNION

--Licenses with missing Subscriptions
    SELECT
      2                                                                                                AS rule_id,
      COUNT(DISTINCT(dim_license_id))                                                                  AS processed_record_count,
      (SELECT COUNT(DISTINCT(dim_license_id)) FROM dim_license WHERE dim_subscription_id IS NOT NULL)  AS passed_record_count,
      (SELECT COUNT(DISTINCT(dim_license_id)) FROM dim_license WHERE dim_subscription_id IS NULL)      AS failed_record_count,
      dbt_updated_at                                                                                   AS run_date
    FROM dim_license
    GROUP BY run_date

  UNION

--Subscriptions with missing Licenses
    SELECT
      3                                                                                                                AS rule_id,
      COUNT(DISTINCT(subscription_name))                                                                               AS processed_record_count,
      (SELECT COUNT(DISTINCT(subscription_name)) FROM product_data_detection_rule_3 WHERE dim_license_id IS NOT NULL)  AS passed_record_count,
      (SELECT COUNT(DISTINCT(subscription_name)) FROM product_data_detection_rule_3 WHERE dim_license_id IS NULL)      AS failed_record_count,
      dbt_updated_at                                                                                                   AS run_date
    FROM product_data_detection_rule_3
    GROUP BY run_date

  UNION

--Subscriptions with Self-Managed Plans having License Start dates in the future
    SELECT
      4                                                                                                            AS rule_id,
      COUNT(DISTINCT(dim_subscription_id))                                                                         AS processed_record_count,
      COUNT(DISTINCT(dim_subscription_id)) - COUNT(DISTINCT IFF(is_license_start_date_future, dim_subscription_id, NULL))
                                                                                                                   AS passed_record_count,
      COUNT(DISTINCT IFF(is_license_start_date_future, dim_subscription_id, NULL))                                 AS failed_record_count,
      dbt_updated_at                                                                                               AS run_date
    FROM self_managed_subs_with_licenses
    GROUP BY run_date

  UNION

--Subscriptions with Self-Managed Plans having License Start Date greater than License Expire date
    SELECT
      5                                                                                                                   AS rule_id,
      COUNT(DISTINCT(dim_subscription_id))                                                                                AS processed_record_count,
      COUNT(DISTINCT(dim_subscription_id)) - COUNT(DISTINCT IFF(is_license_start_date_greater_expire_date, dim_subscription_id, NULL))
                                                                                                                          AS passed_record_count,
      COUNT(DISTINCT IFF(is_license_start_date_greater_expire_date, dim_subscription_id, NULL))                           AS failed_record_count,
      dbt_updated_at                                                                                                      AS run_date
    FROM self_managed_subs_with_licenses
    GROUP BY run_date

  UNION

--Expired License IDs with Subscription End Dates in the Past
    SELECT
        6                                                                                                                    AS rule_id,
        COUNT(DISTINCT(dim_license_id))                                                                                      AS processed_record_count,
        SUM(IFF(is_license_expired_with_sub_end_date_past, 0, 1))                                                            AS passed_record_count,
        SUM(IFF(is_license_expired_with_sub_end_date_past, 1, 0))                                                            AS failed_record_count,
        dbt_updated_at                                                                                                       AS run_date
    FROM expired_licenses_with_subs
    GROUP BY run_date

  UNION

--SaaS Subscriptions Not Mapped to Namespaces
    SELECT
       7                                                                                                    AS rule_id,
       COUNT(DISTINCT(dim_subscription_id))                                                                 AS processed_record_count,
       COUNT(DISTINCT IFF(dim_subscription_id IS NOT NULL AND dim_namespace_id IS NOT NULL,
          dim_subscription_id,
          NULL))                                                                                            AS passed_record_count,
       (processed_record_count - passed_record_count)                                                       AS failed_record_count,
       dbt_updated_at                                                                                       AS run_date
    FROM bdg_namespace_order_subscription
    GROUP BY run_date

), final AS (

    SELECT
      --primary_key
      {{ dbt_utils.generate_surrogate_key(['rule_run_date.rule_run_date', 'processed_passed_failed_record_count.rule_id']) }} AS primary_key,

      --Detection Rule record counts
      rule_id,
      processed_record_count,
      passed_record_count,
      failed_record_count,
      rule_run_date.rule_run_date,
      type_of_data
    FROM processed_passed_failed_record_count
    RIGHT OUTER JOIN rule_run_date
      ON TO_DATE(processed_passed_failed_record_count.run_date) = rule_run_date.rule_run_date

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@jpguero",
    created_date="2021-06-16",
    updated_date="2023-03-13"
) }}
