{{ simple_cte([
    ('iterable_list_source', 'iterable_list_source'),
    ('iterable_list_user_history_source', 'iterable_list_user_history_source')
]) }},

final AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['iterable_list_source.iterable_list_id','iterable_list_user_history_source.iterable_list_user_fivetran_id','iterable_list_user_history_source.iterable_list_user_history_updated_at']) }} AS dim_iterable_list_sk,
    iterable_list_source.iterable_list_id,
    iterable_list_source.iterable_list_name,
    iterable_list_source.iterable_list_created_at,
    {{ get_date_id('iterable_list_source.iterable_list_created_at') }} AS iterable_list_created_at_id,
    iterable_list_source.iterable_list_created_date,
    {{ get_date_id('iterable_list_source.iterable_list_created_date') }} AS iterable_list_created_date_id,
    iterable_list_source.iterable_list_description,
    iterable_list_source.iterable_list_type,
    iterable_list_user_history_source.iterable_list_user_fivetran_id,
    iterable_list_user_history_source.iterable_list_user_history_updated_at,
    {{ get_date_id('iterable_list_user_history_source.iterable_list_user_history_updated_at') }} AS iterable_list_user_history_updated_at_id,
    iterable_list_user_history_source.iterable_list_user_history_updated_date,
    {{ get_date_id('iterable_list_user_history_source.iterable_list_user_history_updated_date') }} AS iterable_list_user_history_updated_date_id
  FROM iterable_list_source
  LEFT JOIN iterable_list_user_history_source
    ON iterable_list_source.iterable_list_id = iterable_list_user_history_source.iterable_list_id

)

SELECT *
FROM final
