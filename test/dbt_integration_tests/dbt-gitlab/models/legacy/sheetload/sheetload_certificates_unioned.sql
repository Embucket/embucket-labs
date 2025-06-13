{{ config({
    "materialized": "view"
    })
}}

{{ dbt_utils.union_relations(
    relations=[ref('sheetload_ally_certificate'),
               ref('sheetload_values_certificate'),
               ref('sheetload_communication_certificate'),
               ref('sheetload_communication_certificate'),
               ref('sheetload_compensation_certificate')]
) }}
