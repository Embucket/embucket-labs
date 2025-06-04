WITH source AS (
  
   SELECT *
   FROM {{ source('iterable','campaign_metrics') }}
 
), final AS (
 
    SELECT
        id::VARCHAR                                AS iterable_campaign_id,
        date::TIMESTAMP                            AS iterable_campaign_datetime,
        date::DATE                                 AS iterable_campaign_date,
        {{ dbt_utils.generate_surrogate_key(['iterable_campaign_id','iterable_campaign_date']) }} 
                                                   AS iterable_campaign_sk,
        _fivetran_deleted::BOOLEAN                 AS is_iterable_campaign_deleted,
        average_order_value::NUMBER                AS iterable_campaign_average_order_value,
        purchases::NUMBER                          AS iterable_campaign_purchases,
        revenue::NUMBER                            AS iterable_campaign_revenue,
        revenue_m::NUMBER                          AS iterable_campaign_revenue_m,
        total_app_uninstalls::NUMBER               AS iterable_campaign_app_uninstalls_total, 
        total_complaints::NUMBER                   AS iterable_campaign_complaints_total,
        total_email_bounced::NUMBER                AS iterable_campaign_email_bounced_total,
        total_email_clicked::NUMBER                AS iterable_campaign_email_clicked_total,
        total_email_opens::NUMBER                  AS iterable_campaign_email_opened_total,
        total_email_delivered::NUMBER              AS iterable_campaign_email_delivered_total,
        total_email_sends::NUMBER                  AS iterable_campaign_email_sends_total,
        total_hosted_unsubscribe_clicks::NUMBER    AS iterable_campaign_hosted_unsubscribe_clicks_total,
        total_purchases::NUMBER                    AS iterable_campaign_purchases_total,
        total_pushes_bounced::NUMBER               AS iterable_campaign_pushes_bounced_total,
        total_pushes_opened::NUMBER                AS iterable_campaign_pushes_opened_total,
        total_pushes_sent::NUMBER                  AS iterable_campaign_pushes_sent_total,
        total_unsubscribes::NUMBER                 AS iterable_campaign_unsubscribes_total,
        unique_email_bounced::NUMBER               AS iterable_campaign_email_bounced_unique,
        unique_email_clicks::NUMBER                AS iterable_campaign_email_clicks_unique,
        unique_email_opens_or_clicks::NUMBER       AS iterable_campaign_email_opens_or_clicks_unique,
        unique_hosted_unsubscribe_clicks::NUMBER   AS iterable_campaign_hosted_unsubscribe_clicks_unique,
        unique_purchases::NUMBER                   AS iterable_campaign_purchases_unique,
        unique_pushes_bounced::NUMBER              AS iterable_campaign_pushes_bounced_unique,
        unique_pushes_opened::NUMBER               AS iterable_campaign_pushes_opened_unique,
        unique_pushes_sent::NUMBER                 AS iterable_campaign_pushes_sent_unique,
        unique_unsubscribes::NUMBER                AS iterable_campaign_unsubscribes_unique,
        unique_email_opens_filtered_::NUMBER       AS iterable_campaign_email_opens_filtered_unique,
        purchases_m_email_::NUMBER                 AS iterable_campaign_purchases_email_m,
        sum_of_custom_conversions::NUMBER          AS iterable_campaign_custom_conversions_sum,
        unique_emails_delivered::NUMBER            AS iterable_campaign_emails_delivered_unique,
        unique_custom_conversions::NUMBER          AS iterable_campaign_custom_conversions_unique,
        total_email_opens_filtered_::NUMBER        AS iterable_campaign_email_opens_filtered_total,
        unique_email_opens::NUMBER                 AS iterable_campaign_email_opens_unique,
        total_custom_conversions::NUMBER           AS iterable_campaign_custom_conversions_total,
        total_email_holdout::NUMBER                AS iterable_campaign_email_holdouts_total,
        average_custom_conversion_value::NUMBER    AS iterable_campaign_custom_conversion_value_average,
        total_email_send_skips::NUMBER             AS iterable_campaign_email_send_skips_total,
        unique_email_sends::NUMBER                 AS iterable_campaign_email_sends_unique,
        revenue_m_email_::NUMBER                   AS iterable_campaign_revenue_m_email
    FROM source

)

SELECT *
FROM final
