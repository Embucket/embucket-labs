
{%- macro mask_model() -%}
{# called as a post-hook for an individual model #}

{%- if execute -%}
 {%- set mask_columns = get_columns_to_mask('model', this.identifier) %} {#  #}

 {# {% do log(mask_columns, info=true) %} #}

 {% if mask_columns %}

    {{ apply_masking_policy(this.database,this.schema,this.identifier,mask_columns) }}   

 {% endif %}

{% endif %}


{%- endmacro -%}