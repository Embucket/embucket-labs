{{ config(
    tags=["mnpi_exception"]
) }}

WITH mje_source AS (

    SELECT *
    FROM {{ ref('zuora_revenue_manual_journal_entry_source') }}

), final AS (

    SELECT DISTINCT

      --ids 
      manual_journal_entry_header_id        AS dim_manual_journal_entry_header_id,

      -- header details
      manual_journal_entry_header_name,
      manual_journal_entry_header_description,
      manual_journal_entry_header_category_code,
      manual_journal_entry_header_exchange_rate_type,
      manual_journal_entry_header_status,
      manual_journal_entry_header_type,

      -- metadata
      manual_journal_entry_header_created_by,
      manual_journal_entry_header_created_date,
      manual_journal_entry_header_updated_by,
      manual_journal_entry_header_updated_date

    FROM mje_source

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2021-06-21",
    updated_date="2021-06-21",
    ) 
 }}