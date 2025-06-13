{%- macro churn_type(original_mrr, new_mrr) -%}

	CASE WHEN {{ new_mrr }} = 0 and {{ original_mrr }} > 0 THEN 'Cancelled'
		 WHEN {{ new_mrr }} < {{ original_mrr }} AND {{ new_mrr }} > 0 THEN 'Downgraded'
		 WHEN {{ new_mrr }} > {{ original_mrr }} THEN 'Upgraded'
		 WHEN {{ new_mrr }} = {{ original_mrr }} THEN 'Maintained'
		 ELSE 'Other'
		 END AS churn_type

{%- endmacro -%}
