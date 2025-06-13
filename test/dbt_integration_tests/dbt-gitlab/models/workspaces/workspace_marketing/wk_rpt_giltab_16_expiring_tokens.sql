{{ config(
    tags=["mnpi_exception"],
    materialized='table'
) }}

WITH mart_arr AS (

  SELECT *
  FROM {{ ref('mart_arr') }}

),

mart_crm_person AS (

  SELECT *
  FROM {{ ref('mart_crm_person') }}

),

mart_ping_instance AS (

  SELECT
    dim_installation_id,
    host_name,
    is_paid_subscription,
    major_minor_version_id,
    latest_subscription_id,
    crm_account_name,
    parent_crm_account_name,
    dim_billing_account_id,
    dim_parent_crm_account_id,
    ping_deployment_type,
    ping_product_tier,
    ping_edition,
    ping_created_at
  FROM {{ ref('mart_ping_instance') }}

),

latest_ping_sent AS (

  SELECT
    dim_installation_id,
    host_name,
    is_paid_subscription,
    major_minor_version_id AS latest_major_minor_version,
    latest_subscription_id,
    crm_account_name,
    parent_crm_account_name,
    dim_billing_account_id,
    dim_parent_crm_account_id,
    ping_deployment_type,
    ping_product_tier,
    ping_edition,
    ping_created_at::DATE  AS date_of_latest_ping_sent
  FROM mart_ping_instance
  WHERE major_minor_version_id != 10000
  QUALIFY RANK() OVER (PARTITION BY dim_installation_id ORDER BY ping_created_at DESC) = 1 --latest ping record

),

all_installations_on_16 AS (
-- all results from previous CTE, filtering to installations with latest ping sent on 16.0 and max(latest_major_minor_version) to ensure results are at installation grain
  SELECT
    dim_installation_id,
    host_name,
    is_paid_subscription,
    ping_product_tier,
    ping_edition,
    MAX(latest_major_minor_version) AS latest_major_minor_version, -- there is one installation ddea7372abd38377ba5bebe8f2e1a88e that sent two major minor version values in the same ping - both over 1600
    latest_subscription_id,
    crm_account_name,
    parent_crm_account_name,
    dim_billing_account_id,
    dim_parent_crm_account_id,
    ping_deployment_type,
    date_of_latest_ping_sent
  FROM latest_ping_sent
  WHERE latest_major_minor_version >= 1600
  GROUP BY ALL

),

days_since_first_ping_on_16 AS (
--Not all installations send a ping on every major minor version. It is possible to jump versions or to simply not send a ping while they are on a certain version.
--This methodology allows us to see the first ping sent on a major minor version at 16.0 or above even if we don't have a ping for all installations that are on 16.0 or higher
  SELECT
    dim_installation_id,
    major_minor_version_id,
    ping_created_at::DATE                                                    AS date_of_first_ping_sent_on_1600_or_higher,
    DATEDIFF(DAY, date_of_first_ping_sent_on_1600_or_higher, CURRENT_DATE)   AS days_since_first_ping_sent_on_1600_or_higher,
    DATEDIFF(WEEK, date_of_first_ping_sent_on_1600_or_higher, CURRENT_DATE)  AS weeks_since_first_ping_sent_on_1600_or_higher,
    DATEDIFF(MONTH, date_of_first_ping_sent_on_1600_or_higher, CURRENT_DATE) AS months_since_first_ping_sent_on_1600_or_higher
  FROM mart_ping_instance
  WHERE major_minor_version_id >= 1600
  QUALIFY RANK() OVER (PARTITION BY dim_installation_id ORDER BY ping_created_at ASC) = 1 --first ping record for installations on 1600 or higher

),

combine_ctes AS (

  SELECT
    all_installations_on_16.*,
    days_since_first_ping_on_16.major_minor_version_id AS first_major_minor_version_sent_on_1600_or_higher,
    days_since_first_ping_on_16.date_of_first_ping_sent_on_1600_or_higher,
    days_since_first_ping_on_16.days_since_first_ping_sent_on_1600_or_higher,
    days_since_first_ping_on_16.weeks_since_first_ping_sent_on_1600_or_higher,
    days_since_first_ping_on_16.months_since_first_ping_sent_on_1600_or_higher
  FROM all_installations_on_16
  INNER JOIN days_since_first_ping_on_16
    ON all_installations_on_16.dim_installation_id = days_since_first_ping_on_16.dim_installation_id
  WHERE all_installations_on_16.ping_deployment_type != 'GitLab.com' -- Our organization manages version upgrades for the Gitlab.com deployment
    AND days_since_first_ping_sent_on_1600_or_higher BETWEEN 335 AND 365

),

final AS (

  SELECT
    combine_ctes.*,
    mart_arr.dim_crm_account_id
  FROM combine_ctes
  LEFT JOIN mart_arr
    ON combine_ctes.latest_subscription_id = mart_arr.dim_subscription_id
      AND arr_month = DATE_TRUNC('MONTH', CURRENT_DATE)
  WHERE ping_product_tier != 'Free'
    AND is_paid_subscription
)

SELECT DISTINCT
  final.dim_crm_account_id,
  mart_crm_person.sfdc_record_id,
  mart_crm_person.inactive_contact,
  mart_crm_person.person_role
FROM final
LEFT JOIN mart_crm_person
  ON final.dim_crm_account_id = mart_crm_person.dim_crm_account_id
    AND CONTAINS(LOWER(person_role), '%gitlab admin%')
