{{
    config(
        materialized='incremental',
        unique_key = "iterable_campaign_metric_sk"
    )
}}

{{ simple_cte([
    ('prep_iterable_campaign_metrics','prep_iterable_campaign_metrics'),
    ('iterable_campaign_history_source', 'iterable_campaign_history_source')
]) }}

, prep_iterable_campaign_metrics_incremental AS (

SELECT *
FROM prep_iterable_campaign_metrics

{% if is_incremental() %}

WHERE iterable_campaign_date >= (SELECT MAX(iterable_campaign_date) FROM {{ this }} )

{% endif %}

), final AS (

  SELECT
  -- Ids
    {{ dbt_utils.generate_surrogate_key(['prep_iterable_campaign_metrics_incremental.iterable_campaign_sk','iterable_campaign_history_source.iterable_campaign_id','iterable_campaign_history_source.iterable_campaign_template_id','iterable_campaign_history_source.iterable_campaign_updated_at']) }} AS iterable_campaign_metric_sk,
    iterable_campaign_history_source.iterable_campaign_id,
    {{ get_date_id('iterable_campaign_history_source.iterable_campaign_updated_date') }} AS iterable_campaign_updated_date_id,
    iterable_campaign_history_source.iterable_campaign_template_id,
    iterable_campaign_history_source.iterable_recurring_campaign_id,
    {{ get_date_id('iterable_campaign_history_source.iterable_campaign_created_date') }} AS iterable_campaign_created_date_id,
    iterable_campaign_history_source.iterable_campaign_created_by_user_id,
    {{ get_date_id('iterable_campaign_history_source.iterable_campaign_ended_date') }} AS iterable_campaign_ended_date_id,
    iterable_campaign_history_source.iterable_campaign_workflow_id,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_date_id,

    -- Date
    prep_iterable_campaign_metrics_incremental.iterable_campaign_date,
    iterable_campaign_history_source.iterable_campaign_updated_at,
    iterable_campaign_history_source.iterable_campaign_updated_date,
    iterable_campaign_history_source.iterable_campaign_created_at,
    iterable_campaign_history_source.iterable_campaign_created_date,
    iterable_campaign_history_source.iterable_campaign_ended_at,
    iterable_campaign_history_source.iterable_campaign_ended_date,

    -- Totals
    prep_iterable_campaign_metrics_incremental.iterable_campaign_app_uninstalls_total,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_complaints_total,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_email_bounced_total,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_email_clicked_total,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_email_opened_total,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_email_delivered_total,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_email_sends_total,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_hosted_unsubscribe_clicks_total,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_purchases_total,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_pushes_bounced_total,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_pushes_opened_total,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_pushes_sent_total,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_unsubscribes_total,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_email_opens_filtered_total,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_custom_conversions_total,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_email_holdouts_total,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_email_send_skips_total,

    -- Unique Counts
    prep_iterable_campaign_metrics_incremental.iterable_campaign_email_bounced_unique,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_email_clicks_unique,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_email_opens_unique,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_hosted_unsubscribe_clicks_unique,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_purchases_unique,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_pushes_bounced_unique,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_pushes_opened_unique,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_pushes_sent_unique,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_unsubscribes_unique,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_email_opens_filtered_unique,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_emails_delivered_unique,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_custom_conversions_unique,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_email_sends_unique,

    -- Other Values
    prep_iterable_campaign_metrics_incremental.iterable_campaign_custom_conversion_value_average,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_custom_conversions_sum,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_delivery_rate,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_average_order_value,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_purchases,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_revenue,

    -- Calculated Rates
    prep_iterable_campaign_metrics_incremental.iterable_campaign_click_rate,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_unique_click_rate,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_click_to_open_rate,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_unique_click_to_open_rate,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_unsubscribe_rate,
    prep_iterable_campaign_metrics_incremental.iterable_campaign_unique_unsubscribe_rate
  FROM prep_iterable_campaign_metrics_incremental
  LEFT JOIN iterable_campaign_history_source
    ON prep_iterable_campaign_metrics_incremental.iterable_campaign_id = iterable_campaign_history_source.iterable_campaign_id

)

SELECT *
FROM final