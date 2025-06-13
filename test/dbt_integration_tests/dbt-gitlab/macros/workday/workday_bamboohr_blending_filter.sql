{%- macro workday_bamboohr_blending_filter(cte,unique_columns,sort_columns=1,filter_date=none) -%}

{%- set cut_over_date = '\'2022-06-16\'' -%} -- this will be changed to the actual cut over date once the system cut over happens

SELECT 
  *,
  {{ dbt_utils.generate_surrogate_key(unique_columns) }} AS unique_filter_key,
  IFF(IFF(uploaded_at >= {{ cut_over_date }} ,'workday','bamboohr' ) = source_system,1,0) AS sort_order
FROM {{ cte }} 
WHERE (source_system = 'workday' AND {{ filter_date or '\'9999-01-01\'' }} >= {{ cut_over_date }} ) 
OR (source_system = 'bamboohr' AND {{ filter_date or '\'0001-01-01\''}} < {{ cut_over_date }} )
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_filter_key ORDER BY sort_order DESC, {{ sort_columns }}) = 1

{%- endmacro -%}
