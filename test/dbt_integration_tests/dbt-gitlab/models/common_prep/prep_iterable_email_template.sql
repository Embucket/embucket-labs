{{ simple_cte([
    ('iterable_email_template_history_source', 'iterable_email_template_history_source'),
    ('iterable_template_history_source', 'iterable_template_history_source'),
    ('iterable_email_link_param_history_source', 'iterable_email_link_param_history_source')
]) }},

final AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['iterable_email_template_history_source.iterable_email_template_id','iterable_email_template_history_source.iterable_email_template_updated_at','iterable_email_link_param_history_source.iterable_email_link_param_key','iterable_email_link_param_history_source.iterable_email_link_param_value','iterable_template_history_source.iterable_template_name','iterable_template_history_source.iterable_template_type']) }} AS dim_iterable_template_sk,
    iterable_email_template_history_source.iterable_email_template_id,
    iterable_email_template_history_source.iterable_email_template_updated_at,
    {{ get_date_id('iterable_email_template_history_source.iterable_email_template_updated_at') }} AS iterable_email_template_updated_at_id,
    iterable_email_template_history_source.iterable_email_template_updated_date,
    {{ get_date_id('iterable_email_template_history_source.iterable_email_template_updated_date') }} AS iterable_email_template_updated_date_id,
    iterable_email_template_history_source.iterable_email_template_campaign_id,
    iterable_email_template_history_source.iterable_email_template_from_email,
    iterable_email_template_history_source.iterable_email_template_from_name,
    iterable_email_template_history_source.iterable_email_template_google_analytics_campaign_name,
    iterable_email_template_history_source.iterable_email_template_html,
    iterable_email_template_history_source.iterable_email_template_plain_text,
    iterable_email_template_history_source.iterable_email_template_preheader_text,
    iterable_email_template_history_source.iterable_email_template_reply_to_email,
    iterable_email_template_history_source.iterable_email_template_subject,
    iterable_email_template_history_source.iterable_email_template_locale,
    iterable_email_template_history_source.iterable_email_template_cache_data_feed,
    iterable_email_template_history_source.iterable_email_template_data_feed_id,
    iterable_email_template_history_source.iterable_email_template_data_feed_ids                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 AS iterable_email_template_data_feed_ids_array,
    iterable_email_template_history_source.is_iterable_email_template_default_locale,
    iterable_email_template_history_source.iterable_email_template_merge_data_feed_context,
    iterable_template_history_source.iterable_template_message_type_id,
    iterable_template_history_source.iterable_template_client_id,
    iterable_template_history_source.iterable_template_created_at,
    {{ get_date_id('iterable_template_history_source.iterable_template_created_at') }} AS iterable_template_created_at_id,
    iterable_template_history_source.iterable_template_created_date,
    {{ get_date_id('iterable_template_history_source.iterable_template_created_date') }} AS iterable_template_created_date_id,
    iterable_template_history_source.iterable_template_creator_user_id,
    iterable_template_history_source.iterable_template_name,
    iterable_template_history_source.iterable_template_type,
    iterable_email_link_param_history_source.iterable_email_link_param_key,
    iterable_email_link_param_history_source.iterable_email_link_param_updated_at,
    {{ get_date_id('iterable_email_link_param_history_source.iterable_email_link_param_updated_at') }} AS iterable_email_link_param_updated_at_id,
    iterable_email_link_param_history_source.iterable_email_link_param_updated_date,
    {{ get_date_id('iterable_email_link_param_history_source.iterable_email_link_param_updated_date') }} AS iterable_email_link_param_updated_date_id,
    iterable_email_link_param_history_source.iterable_email_link_param_value,
    iterable_template_history_source.iterable_template_updated_at                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        AS iterable_template_updated_at_last
  FROM iterable_email_template_history_source
  LEFT JOIN iterable_template_history_source
    ON iterable_email_template_history_source.iterable_email_template_id = iterable_template_history_source.iterable_template_id
      AND iterable_email_template_history_source.iterable_email_template_updated_date = iterable_template_history_source.iterable_template_updated_date
  LEFT JOIN iterable_email_link_param_history_source
    ON iterable_email_template_history_source.iterable_email_template_id = iterable_email_link_param_history_source.iterable_email_template_id
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY iterable_email_template_history_source.iterable_email_template_id 
    ORDER BY iterable_template_history_source.iterable_template_updated_at DESC ) = 1

)

SELECT *
FROM final
