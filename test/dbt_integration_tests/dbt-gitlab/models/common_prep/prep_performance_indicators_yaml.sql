{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "table",
    "unique_key": "performance_indicator_pk"
    })
}}

WITH unioned AS (

  {{ dbt_utils.union_relations(
      relations=[
          ref('performance_indicators_cost_source'),
          ref('performance_indicators_corporate_finance_source'),
          ref('performance_indicators_customer_support_source'),
          ref('performance_indicators_dev_section_source'),
          ref('performance_indicators_development_department_source'),
          ref('performance_indicators_enablement_section_source'),
          ref('performance_indicators_engineering_source'),
          ref('performance_indicators_finance_source'),
          ref('performance_indicators_infrastructure_department_source'),
          ref('performance_indicators_marketing_source'),
          ref('performance_indicators_ops_section_source'),
          ref('performance_indicators_people_success_source'),
          ref('performance_indicators_product_source'),
          ref('performance_indicators_quality_department_source'),
          ref('performance_indicators_recruiting_source'),
          ref('performance_indicators_sales_source'),
          ref('performance_indicators_security_department_source'),
          ref('performance_indicators_ux_department_source')
          ]
  ) }}

),

final AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['_dbt_source_relation', 'unique_key']) }} AS performance_indicator_pk,
    *
  FROM unioned
  QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_key ORDER BY valid_from_date) = 1

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@cbraza",
    updated_by="@cbraza",
    created_date="2022-12-23",
    updated_date="2022-12-23"
) }}
