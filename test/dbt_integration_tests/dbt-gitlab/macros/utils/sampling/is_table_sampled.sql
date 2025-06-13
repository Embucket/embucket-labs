{%- macro is_table_sampled(model_name) -%}

  {% set samples_dict = samples() %}
  {% set model_sample = samples_dict.samples | selectattr("name", "equalto",model_name) | list %} 

  {% set is_sampled = model_sample|length >0 %}
  {% set is_override = var('local_data') != 'sample' %}
  {% set is_development = target.name not in production_targets() %}
  
  {% do return(is_development and is_sampled and not is_override) %}    

{%- endmacro -%}
