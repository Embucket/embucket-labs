{{ config(
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('cloud_activation','customers_db_cloud_activations_source')
]) }}

, final AS (

     SELECT
      -- Surrogate Key
      {{ dbt_utils.generate_surrogate_key(['cloud_activation.cloud_activation_id']) }}  AS dim_cloud_activation_sk,

      -- Natural Key
      cloud_activation_id                                                      AS dim_cloud_activation_id,

      --Foreign Keys
      customer_id                                                              AS internal_customer_id, --Specific to Customer Dot Customers and can only be joined to CDot Customers,
      billing_account_id                                                       AS dim_billing_account_id,
      subscription_name,
      cloud_activation_created_at,
      cloud_activation_updated_at,

      --Other Attributes
      is_super_sonics_aware_subscription,
      seat_utilization_reminder_sent_at

    FROM cloud_activation

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2023-10-03",
    updated_date="2023-10-03"
) }}

