{%- macro create_sample_tables() -%}

  {% set samples_dict = samples() %}
  {% set sample_models = samples_dict.samples | map(attribute='name')| list %}

  {% if sample_models | length < 1 %}
    {% do exceptions.raise_compiler_error('There are no table samples configured in the samples macro')%}
  {% endif %}

  {% set sql %}

    {% for model in sample_models %}

      {{ generate_sample_table_sql(model) }}
      
    {% endfor %}

  {% endset %}

  {% do run_query(sql) %}
  {% do log("Tables Sampled: " ~ sample_models, info=True) %}

{%- endmacro -%}