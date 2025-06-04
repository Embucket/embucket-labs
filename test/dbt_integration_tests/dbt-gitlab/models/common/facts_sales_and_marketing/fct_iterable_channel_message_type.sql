{{ simple_cte([
    ('iterable_channel_source', 'iterable_channel_source'),
    ('prep_iterable_message_type','prep_iterable_message_type')
]) }},

prep_iterable_message_channel AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['iterable_channel_id']) }} AS iterable_channel_sk,
    iterable_channel_id
  FROM iterable_channel_source

)

SELECT
-- Ids
  prep_iterable_message_channel.iterable_channel_sk,
  prep_iterable_message_channel.iterable_channel_id,
  prep_iterable_message_type.iterable_message_type_sk,
  prep_iterable_message_type.iterable_message_type_id,
  prep_iterable_message_type.iterable_user_fivetran_id,
  prep_iterable_message_type.iterable_message_channel_id,
  prep_iterable_message_type.iterable_message_created_date_id,
  {{ get_date_id('prep_iterable_message_type.iterable_message_updated_at_last') }} AS iterable_message_updated_at_id,


  -- Dates
  prep_iterable_message_type.iterable_message_created_at,
  prep_iterable_message_type.iterable_message_created_date,
  prep_iterable_message_type.iterable_message_updated_at_last

FROM prep_iterable_message_type
LEFT JOIN prep_iterable_message_channel
  ON prep_iterable_message_type.iterable_message_channel_id = prep_iterable_message_channel.iterable_channel_id
