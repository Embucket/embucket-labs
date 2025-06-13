{{ simple_cte([
    ('iterable_campaign_metrics_source', 'iterable_campaign_metrics_source')
]) }},

calculations AS (

  SELECT
    iterable_campaign_sk,
    SUM(iterable_campaign_email_delivered_total / NULLIF(iterable_campaign_email_sends_total, 0)) AS iterable_campaign_delivery_rate,
    SUM(iterable_campaign_email_clicked_total / NULLIF(iterable_campaign_email_sends_total, 0))   AS iterable_campaign_click_rate,
    SUM(iterable_campaign_email_clicks_unique / NULLIF(iterable_campaign_email_sends_unique, 0))  AS iterable_campaign_unique_click_rate,
    SUM(iterable_campaign_email_clicked_total / NULLIF(iterable_campaign_email_opened_total, 0))  AS iterable_campaign_click_to_open_rate,
    SUM(iterable_campaign_email_clicks_unique / NULLIF(iterable_campaign_email_opens_unique, 0))  AS iterable_campaign_unique_click_to_open_rate,
    SUM(iterable_campaign_unsubscribes_total / NULLIF(iterable_campaign_email_sends_total, 0))    AS iterable_campaign_unsubscribe_rate,
    SUM(iterable_campaign_unsubscribes_unique / NULLIF(iterable_campaign_email_sends_unique, 0))  AS iterable_campaign_unique_unsubscribe_rate
  FROM iterable_campaign_metrics_source
  GROUP BY 1

),

final AS (

  SELECT
    iterable_campaign_metrics_source.iterable_campaign_sk,
    iterable_campaign_id,
    {{ get_date_id('iterable_campaign_date') }} AS iterable_campaign_date_id,
    iterable_campaign_date,
    is_iterable_campaign_deleted,
    iterable_campaign_average_order_value,
    iterable_campaign_purchases,
    iterable_campaign_revenue,

    iterable_campaign_app_uninstalls_total,
    iterable_campaign_complaints_total,
    iterable_campaign_email_bounced_total,
    iterable_campaign_email_clicked_total,
    iterable_campaign_email_opened_total,
    iterable_campaign_email_delivered_total,
    iterable_campaign_email_sends_total,
    iterable_campaign_hosted_unsubscribe_clicks_total,
    iterable_campaign_purchases_total,
    iterable_campaign_pushes_bounced_total,
    iterable_campaign_pushes_opened_total,
    iterable_campaign_pushes_sent_total,
    iterable_campaign_unsubscribes_total,
    iterable_campaign_email_bounced_unique,
    iterable_campaign_email_clicks_unique,
    iterable_campaign_email_opens_or_clicks_unique,
    iterable_campaign_hosted_unsubscribe_clicks_unique,
    iterable_campaign_purchases_unique,
    iterable_campaign_pushes_bounced_unique,
    iterable_campaign_pushes_opened_unique,
    iterable_campaign_pushes_sent_unique,
    iterable_campaign_unsubscribes_unique,
    iterable_campaign_email_opens_filtered_unique,
    iterable_campaign_custom_conversions_sum,
    iterable_campaign_emails_delivered_unique,
    iterable_campaign_custom_conversions_unique,
    iterable_campaign_email_opens_filtered_total,
    iterable_campaign_email_opens_unique,
    iterable_campaign_custom_conversions_total,
    iterable_campaign_email_holdouts_total,
    iterable_campaign_custom_conversion_value_average,
    iterable_campaign_email_send_skips_total,
    iterable_campaign_email_sends_unique,
    calculations.iterable_campaign_delivery_rate,
    calculations.iterable_campaign_click_rate,
    calculations.iterable_campaign_unique_click_rate,
    calculations.iterable_campaign_click_to_open_rate,
    calculations.iterable_campaign_unique_click_to_open_rate,
    calculations.iterable_campaign_unsubscribe_rate,
    calculations.iterable_campaign_unique_unsubscribe_rate
  FROM iterable_campaign_metrics_source
  LEFT JOIN calculations
    ON iterable_campaign_metrics_source.iterable_campaign_sk = calculations.iterable_campaign_sk

)

SELECT *
FROM final
