{{ config(
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('prep_trial', 'prep_trial')

]) }},


trials AS (

  SELECT DISTINCT
    --Primary Key--
    {{ dbt_utils.generate_surrogate_key(['prep_trial.internal_order_id', 'prep_trial.dim_namespace_id', 'prep_trial.subscription_name', 'prep_trial.order_updated_at']) }} AS trial_pk,

    --Natural Key--
    prep_trial.internal_order_id,

    --Foreign Keys--
    prep_trial.dim_namespace_id,
    prep_trial.product_rate_plan_id,
    prep_trial.internal_customer_id,
    prep_trial.user_id,

    --Other Attributes
    prep_trial.is_gitlab_user,
    prep_trial.user_created_at,

    prep_trial.namespace_created_at,
    prep_trial.namespace_type,

    prep_trial.is_trial_converted,
    prep_trial.subscription_name,
    prep_trial.subscription_name_slugify,
    prep_trial.subscription_start_date,
    prep_trial.country,
    prep_trial.company_size,

    prep_trial.order_created_at,
    prep_trial.order_updated_at,
    prep_trial.trial_type,
    prep_trial.trial_type_name,
    prep_trial.trial_start_date,
    prep_trial.trial_end_date

  FROM prep_trial

)

{{ dbt_audit(
    cte_ref="trials",
    created_by="@snalamaru",
    updated_by="@utkarsh060",
    created_date="2023-06-30",
    updated_date="2025-02-03"
) }}
