{%- macro sfdc_deal_size(iacv_value, column_name) -%}

	CASE WHEN {{ iacv_value }} < 5000 THEN '1 - Small (<5k)'
		WHEN {{ iacv_value }} < 25000 THEN '2 - Medium (5k - 25k)'
		WHEN {{ iacv_value }} < 100000 THEN '3 - Big (25k - 100k)'
		WHEN {{ iacv_value }} >= 100000 THEN '4 - Jumbo (>100k)'
	ELSE '5 - Unknown' END AS {{ column_name }}

{%- endmacro -%}
