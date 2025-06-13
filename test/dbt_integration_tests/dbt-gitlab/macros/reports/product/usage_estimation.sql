{% macro usage_estimation(recorded_usage, percent_reporting) -%}

{{ recorded_usage }} + DIV0(({{ recorded_usage }} * (1 - {{ percent_reporting }} )),{{ percent_reporting }} )

{%- endmacro %}
