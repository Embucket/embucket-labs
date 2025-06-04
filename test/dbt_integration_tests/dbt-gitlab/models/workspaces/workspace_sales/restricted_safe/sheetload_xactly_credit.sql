WITH final AS (

  SELECT * 
  FROM {{ ref('sheetload_xactly_credit_source') }}

)
{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-06-16",
    updated_date="2022-06-16"
) }}
