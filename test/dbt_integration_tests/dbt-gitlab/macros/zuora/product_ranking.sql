{%- macro product_ranking(product_category) -%}

  MAX(DECODE({{ product_category }},   --Need to account for the 'other' categories
            'Bronze', 1,
            'Silver', 2,
            'Gold', 3,

            'Starter', 1,
            'Premium', 2,
            'Ultimate', 3,
            0
       ))

{%- endmacro -%}