{%- macro partner_category(sales_qualified_source, fulfillment_partner_name) -%}

    CASE
      WHEN (
            {{ sales_qualified_source }} = 'Channel Generated' 
              OR {{ sales_qualified_source }} = 'Partner Generated'
          )
        THEN 'Partner Sourced'
      WHEN (
            {{ sales_qualified_source }} != 'Channel Generated'
              AND {{ sales_qualified_source }} != 'Partner Generated'
            )
          AND NOT LOWER({{ fulfillment_partner_name }}) LIKE ANY ('%google%','%gcp%','%amazon%')
        THEN 'Channel Co-Sell'
      WHEN (
            {{ sales_qualified_source }} != 'Channel Generated' 
              AND {{ sales_qualified_source }} != 'Partner Generated')
                AND LOWER({{ fulfillment_partner_name }}) LIKE ANY ('%google%','%gcp%','%amazon%')
        THEN 'Alliance Co-Sell'
      ELSE 'Direct'
    END

{%- endmacro -%}