{{ simple_cte([
    ('iterable_user_history_source', 'iterable_user_history_source'),
    ('iterable_user_unsubscribed_channel_source', 'iterable_user_unsubscribed_channel_source'),
    ('iterable_channel_source', 'iterable_channel_source')
]) }},

iterable_user_history_source_hash AS (

  SELECT
    {{ hash_sensitive_columns('iterable_user_history_source') }}
  FROM iterable_user_history_source

),

final AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['iterable_user_history_source_hash.iterable_user_fivetran_id','iterable_user_history_source_hash.iterable_user_updated_at','iterable_user_unsubscribed_channel_source.iterable_user_unsubscribed_channel_id']) }} AS dim_iterable_user_history_sk,
    iterable_user_history_source_hash.iterable_user_fivetran_id,
    iterable_user_history_source_hash.iterable_user_email_hash,
    iterable_user_history_source_hash.iterable_user_first_name_hash,
    iterable_user_history_source_hash.iterable_user_last_name_hash,
    iterable_user_history_source_hash.iterable_user_phone_number_hash,
    COALESCE(iterable_user_history_source_hash.iterable_user_default_id, iterable_user_history_source_hash.iterable_user_id)                                                                                                                                                                                                                                                                                                    AS iterable_user_id,
    iterable_user_history_source_hash.iterable_user_updated_date,
    iterable_user_history_source_hash.iterable_user_updated_at,
    {{ get_date_id('iterable_user_updated_date') }} AS iterable_user_updated_date_id,
    iterable_user_history_source_hash.iterable_user_signup_date,
    {{ get_date_id('iterable_user_signup_date') }} AS iterable_user_signup_date_id,
    iterable_user_history_source_hash.iterable_user_signup_at,
    iterable_user_history_source_hash.iterable_user_signup_source,
    iterable_user_history_source_hash.iterable_user_email_list_ids                                                                                                                                                                                                                                                                                                                                                              AS iterable_user_email_list_id_array,
    iterable_user_history_source_hash.iterable_user_phone_number_carrier,
    iterable_user_history_source_hash.iterable_user_phone_number_country_code_iso,
    iterable_user_history_source_hash.iterable_user_phone_number_line_type,
    iterable_user_history_source_hash.iterable_user_phone_number_updated_at,
    iterable_user_history_source_hash.iterable_user_additional_properties,
    iterable_user_unsubscribed_channel_source.iterable_user_unsubscribed_channel_id,
    iterable_channel_source.iterable_channel_type                                                                                                                                                                                                                                                                                                                                                                               AS iterable_user_unsubscribed_channel_type,
    iterable_channel_source.iterable_channel_message_medium                                                                                                                                                                                                                                                                                                                                                                     AS iterable_user_unsubscribed_channel_message_medium,
    iterable_channel_source.iterable_channel_name                                                                                                                                                                                                                                                                                                                                                                               AS iterable_user_unsubscribed_channel_name
  FROM iterable_user_history_source_hash
  LEFT JOIN iterable_user_unsubscribed_channel_source
    ON iterable_user_history_source_hash.iterable_user_fivetran_id = iterable_user_unsubscribed_channel_source.iterable_user_unsubscribed_fivetran_id
  LEFT JOIN iterable_channel_source
    ON iterable_user_unsubscribed_channel_source.iterable_user_unsubscribed_channel_id = iterable_channel_source.iterable_channel_id

)

SELECT *
FROM final
