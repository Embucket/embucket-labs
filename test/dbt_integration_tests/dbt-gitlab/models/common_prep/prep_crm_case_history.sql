WITH source AS (

  SELECT *
  FROM {{ ref('sfdc_case_history_source') }}

),

renamed AS (

  SELECT
    source.case_history_id,

    --keys
    source.case_id,
    {{ dbt_utils.generate_surrogate_key(['source.case_history_id']) }} AS dim_crm_case_history_sk,
    source.created_by_id,

    --Field and datatype information
    source.case_field,
    source.data_type,

    --Change values
    source.new_value,
    source.old_value,
    source.field_modified_at,

    --metadata
    source.is_deleted

  FROM source
)

   {{ dbt_audit(
    cte_ref="renamed",
    created_by="@mfleisher",
    updated_by="@mfleisher",
    created_date="2024-06-26",
    updated_date="2024-07-17"
) }}
