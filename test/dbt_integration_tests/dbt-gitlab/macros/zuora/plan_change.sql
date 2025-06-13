{%- macro plan_change(original_product_ranking, original_mrr, new_product_ranking, new_mrr) -%}

    CASE
      WHEN {{ new_mrr }} = 0 AND {{ original_mrr }} > 0
        THEN 'Cancelled'
      WHEN {{ new_product_ranking }} = 0 OR {{ original_product_ranking }} = 0
        THEN 'Not Valid'
      WHEN {{ original_product_ranking }} = {{ new_product_ranking }}
        THEN 'Maintained'
      WHEN {{ original_product_ranking }} > {{ new_product_ranking }}
        THEN 'Downgraded'
      WHEN {{ original_product_ranking }} < {{ new_product_ranking }}
        THEN 'Upgraded'
      END                                       AS plan_change

{%- endmacro -%}
