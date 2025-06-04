{%- macro only_force_full_refresh() -%}
{% do return(true if flags.FULL_REFRESH and var('full_refresh_force', false) else false) %}
{%- endmacro -%}