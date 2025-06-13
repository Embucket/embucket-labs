{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('prep_subscription', 'prep_subscription'),
    ('prep_charge_mrr_daily', 'prep_charge_mrr_daily'),
    ('prep_date', 'prep_date'),
    ('prep_product_detail', 'prep_product_detail'),
    ('prep_ping_instance', 'prep_ping_instance'),
    ('prep_license', 'prep_license'),
    ('prep_usage_self_managed_seat_link', 'prep_usage_self_managed_seat_link')
]) }}

, prep_charge_mrr_daily_latest AS (

/*
To map the products (dim_product_detail_id) associated with the subscription, we need to look at the charges for the latest subscription version of the associated dim_subscription_id.
We have created a mapping table in prep_charge_mrr_daily at the daily grain which expands all of the charges for a subscription_name across the effective dates of the charges.

We want to limit this to the Active/Cancelled version of the subscription since this represents the latest valid version.

*/

  SELECT
    prep_charge_mrr_daily.* EXCLUDE (subscription_status),
    prep_subscription.subscription_status
  FROM prep_charge_mrr_daily
  LEFT JOIN prep_subscription
    ON prep_charge_mrr_daily.dim_subscription_id = prep_subscription.dim_subscription_id
  WHERE prep_subscription.subscription_status IN ('Active', 'Cancelled')

-- map_installation_subscription_product logic:

) , ping_subscription_mapping AS (

/*
Do the following to begin mapping an installation to a subscription:

installation_subscription_mapping
ping_subscription_data
installation_license_mapping

1. Map the ping to dim_subscription_id based on the license information sent with the record.
2. Find the next ping date for each Service Ping
*/

  SELECT
    prep_ping_instance.ping_created_at,
    prep_ping_instance.dim_installation_id,
    COALESCE(
      COALESCE(sha256.dim_subscription_id, md5.dim_subscription_id), 
      prep_ping_instance.raw_usage_data_payload:license_subscription_id::TEXT
      )                                                                                   AS dim_subscription_id,
    LEAD(ping_created_at) OVER (
      PARTITION BY dim_installation_id ORDER BY ping_created_at ASC
      )                                                                                   AS next_ping_date
  FROM prep_ping_instance
  LEFT JOIN prep_license AS md5
    ON prep_ping_instance.license_md5 = md5.license_md5
  LEFT JOIN prep_license AS sha256
    ON prep_ping_instance.license_sha256 = sha256.license_sha256
  WHERE COALESCE(
      COALESCE(sha256.dim_subscription_id, md5.dim_subscription_id), 
      prep_ping_instance.raw_usage_data_payload:license_subscription_id::TEXT
      ) IS NOT NULL

), ping_subscription_daily_mapping AS (

/*
Expand the Sevice Ping records to the day grain.

Assumptions:
1. The dim_installation_id <> dim_subscription_id mapping is valid between the ping_created_at date and the next received ping date for that installation
2. If no other pings have been received for this installation, this mapping is valid until one week the last report created date since this data is received weekly.
*/

  SELECT 
    prep_date.date_actual,
    ping_subscription_mapping.dim_installation_id,
    ping_subscription_mapping.dim_subscription_id
  FROM ping_subscription_mapping
  INNER JOIN prep_date
    ON ping_subscription_mapping.ping_created_at <= prep_date.date_actual
      AND COALESCE(ping_subscription_mapping.next_ping_date, DATEADD('day', 7, ping_subscription_mapping.ping_created_at)) > prep_date.date_actual

), seat_link_records AS (

/*
Not all installations will send a Service Ping, so we will incorporate the Seat Link records
to have a more complete view of all potential dim_installation_id <> dim_subscription_id mappings.

This CTE finds the next_report_date for each dim_installation_id so we can expand the Seat Link data in a subsequent CTE.
*/

  SELECT 
    prep_usage_self_managed_seat_link.*,
    LEAD(report_date) OVER (PARTITION BY dim_installation_id ORDER BY report_date ASC)  AS next_report_date
  FROM prep_usage_self_managed_seat_link

), seat_link_subscription_daily_mapping AS (

/*
Expand the Seat Link data to the daily grain for each installation - subscription.

Assumptions:
1. The dim_installation_id <> dim_subscription_id mapping is valid between the report_date and the next received report_date for that installation
2. If no other Seat Link records have been received for this installation, this mapping is valid until one day after the report date since this data is received daily.
*/

  SELECT
    prep_date.date_actual,
    seat_link_records.dim_installation_id,
    seat_link_records.dim_subscription_id
  FROM seat_link_records
  LEFT JOIN prep_subscription
    ON seat_link_records.dim_subscription_id = prep_subscription.dim_subscription_id
  INNER JOIN prep_date
    ON seat_link_records.report_date <= prep_date.date_actual
    AND COALESCE(seat_link_records.next_report_date, DATEADD('day', 1, seat_link_records.report_date)) > prep_date.date_actual
  -- take the last subscription version per day for each installation to reflect the lastest subscription in Zuora
  QUALIFY ROW_NUMBER() OVER (PARTITION BY prep_date.date_actual, seat_link_records.dim_installation_id ORDER BY prep_subscription.subscription_version DESC) = 1

), installation_subscription_product_joined AS (

/*
Combine the Seat Link and Service Ping mappings for dim_installation_id <> dim_subscription_id. This will create a source of truth
for all possible mappings across both sources.

We perform a LEFT OUTER JOIN on the two datasets because the set of installations that send Service Ping records and the set of 
installations that send Seat Link data overlaps, but both may contain additional mappings.
*/

  SELECT 
    COALESCE(ping_subscription_daily_mapping.date_actual, seat_link_subscription_daily_mapping.date_actual)                   AS date_actual,
    COALESCE(ping_subscription_daily_mapping.dim_installation_id, seat_link_subscription_daily_mapping.dim_installation_id)   AS dim_installation_id,
    COALESCE(seat_link_subscription_daily_mapping.dim_subscription_id, ping_subscription_daily_mapping.dim_subscription_id)   AS dim_subscription_id,
    prep_subscription.dim_subscription_id_original,
    prep_subscription.subscription_name
  FROM ping_subscription_daily_mapping
  FULL OUTER JOIN seat_link_subscription_daily_mapping
    ON ping_subscription_daily_mapping.dim_installation_id = seat_link_subscription_daily_mapping.dim_installation_id
      AND ping_subscription_daily_mapping.date_actual = seat_link_subscription_daily_mapping.date_actual
  LEFT JOIN prep_subscription
    ON COALESCE(seat_link_subscription_daily_mapping.dim_subscription_id, ping_subscription_daily_mapping.dim_subscription_id) = prep_subscription.dim_subscription_id

), final AS (

/*

These charges contains a full history of the products associated with a subscription (dim_subscription_id_original/subscription_name) as well as the effective dates of the 
products as they were used by the customer. They are all associated with the most current dim_subscription_id in the subscription_name lineage.

We need to map these charges to the dim_subscription_id at the time the charges were effective otherwise the most recent version will be associated with dates before it was created, based on
how the charges track the history of the subscription. To map between the current dim_subscription_id and the one active at the time of the charges, we join to the subscription object 
between the subscription_created_datetime (adjusted for the first version of a subscription due to backdated effective dates in subscriptions) and the created date of the next subscription version.
*/

  SELECT DISTINCT
    prep_charge_mrr_daily_latest.* EXCLUDE (
        dim_subscription_id,
        dim_subscription_id_original,
        subscription_version,
        created_by, 
        updated_by, 
        model_created_date, 
        model_updated_date, 
        dbt_updated_at, 
        dbt_created_at),
    CASE
      WHEN prep_product_detail.product_deployment_type = 'GitLab.com' THEN prep_subscription.namespace_id
      ELSE NULL
    END AS dim_namespace_id,
    CASE
      WHEN prep_product_detail.product_deployment_type IN ('Self-Managed', 'Dedicated') THEN installation_subscription_product_joined.dim_installation_id
      ELSE NULL
    END AS dim_installation_id,
    COALESCE(
      CASE
        WHEN prep_product_detail.product_deployment_type = 'GitLab.com' THEN prep_subscription.dim_subscription_id
        ELSE subscriptions_ping.dim_subscription_id
      END,
      subscriptions_charge.dim_subscription_id
    ) AS dim_subscription_id,
    COALESCE(
      CASE
        WHEN prep_product_detail.product_deployment_type = 'GitLab.com' THEN prep_subscription.dim_subscription_id_original
        ELSE subscriptions_ping.dim_subscription_id_original
      END,
      subscriptions_charge.dim_subscription_id_original
    ) AS dim_subscription_id_original,
    COALESCE(
      CASE
        WHEN prep_product_detail.product_deployment_type = 'GitLab.com' THEN prep_subscription.subscription_version
        ELSE subscriptions_ping.subscription_version
      END,
      subscriptions_charge.subscription_version
    ) AS subscription_version
  FROM prep_charge_mrr_daily_latest
  LEFT JOIN prep_product_detail
    ON prep_charge_mrr_daily_latest.dim_product_detail_id = prep_product_detail.dim_product_detail_id
  -- For GitLab.com
  LEFT JOIN prep_subscription
    ON prep_charge_mrr_daily_latest.subscription_name = prep_subscription.subscription_name
      AND prep_charge_mrr_daily_latest.date_actual >= prep_subscription.subscription_created_datetime_adjusted::DATE
      AND prep_charge_mrr_daily_latest.date_actual < prep_subscription.next_subscription_created_datetime::DATE
  -- For Self-Managed and Dedicated
  LEFT JOIN installation_subscription_product_joined
    ON prep_charge_mrr_daily_latest.dim_subscription_id_original = installation_subscription_product_joined.dim_subscription_id_original
      AND prep_charge_mrr_daily_latest.date_actual = installation_subscription_product_joined.date_actual
  LEFT JOIN prep_subscription AS subscriptions_ping
    ON installation_subscription_product_joined.dim_subscription_id = subscriptions_ping.dim_subscription_id
  LEFT JOIN prep_subscription AS subscriptions_charge
    ON prep_charge_mrr_daily_latest.dim_subscription_id_original = subscriptions_charge.dim_subscription_id_original
      AND prep_charge_mrr_daily_latest.date_actual BETWEEN subscriptions_charge.subscription_created_datetime_adjusted AND subscriptions_charge.next_subscription_created_datetime
)

SELECT *
FROM final
