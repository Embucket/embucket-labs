{%- macro get_sample_relation(relation) -%}

  {% set sample_relation = relation.replace_path(identifier = relation.identifier ~ var('sample_suffix')) %}

  {{ return(sample_relation) }}

{%- endmacro -%}