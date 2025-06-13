WITH source AS (
  
   SELECT *
   FROM {{ source('iterable','email_template_history') }}
 
), final AS (
 
    SELECT
        id::VARCHAR                              AS iterable_email_template_id,
        updated_at::TIMESTAMP                    AS iterable_email_template_updated_at,
        updated_at::DATE                         AS iterable_email_template_updated_date,
        campaign_id::VARCHAR                     AS iterable_email_template_campaign_id,
        from_email::VARCHAR                      AS iterable_email_template_from_email,
        from_name::VARCHAR                       AS iterable_email_template_from_name,
        google_analytics_campaign_name::VARCHAR  AS iterable_email_template_google_analytics_campaign_name,
        html::VARCHAR                            AS iterable_email_template_html,
        plain_text::VARCHAR                      AS iterable_email_template_plain_text,
        preheader_text::VARCHAR                  AS iterable_email_template_preheader_text,
        reply_to_email::VARCHAR                  AS iterable_email_template_reply_to_email,
        subject::VARCHAR                         AS iterable_email_template_subject,
        locale::VARCHAR                          AS iterable_email_template_locale,
        cache_data_feed::VARCHAR                 AS iterable_email_template_cache_data_feed,
        data_feed_id::VARCHAR                    AS iterable_email_template_data_feed_id,
        data_feed_ids::VARCHAR                   AS iterable_email_template_data_feed_ids,
        is_default_locale::BOOLEAN               AS is_iterable_email_template_default_locale,
        merge_data_feed_context::VARCHAR         AS iterable_email_template_merge_data_feed_context,
    FROM source

)

SELECT *
FROM final
