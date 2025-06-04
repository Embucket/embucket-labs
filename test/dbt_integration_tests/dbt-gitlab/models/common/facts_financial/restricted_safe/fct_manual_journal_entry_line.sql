WITH mje_source AS (

    SELECT *
    FROM {{ ref('zuora_revenue_manual_journal_entry_source') }}

), final AS (

    SELECT

      -- ids
      manual_journal_entry_line_id                                          AS dim_manual_journal_entry_line_id,
      manual_journal_entry_header_id                                        AS dim_manual_journal_entry_header_id,
      revenue_contract_id                                                   AS dim_revenue_contract_id,
      revenue_contract_line_id                                              AS dim_revenue_contract_line_id,
      doc_line_id,
      set_of_books_id,
      debit_account_code_combination_id,
      credit_account_code_combination_id,

      -- dates
      {{ get_date_id('manual_journal_entry_line_start_date') }}             AS manual_journal_entry_line_start_date_id,
      {{ get_date_id('manual_journal_entry_line_end_date') }}               AS manual_journal_entry_line_end_date_id,
      {{ get_date_id('reversal_period_id') }}                               AS reversal_period_date_id,
      {{ get_date_id('period_id') }}                                        AS manual_journal_entry_period_date_id,
      {{ get_date_id('exchange_rate_date') }}                               AS exchange_rate_date_id,

      -- accounting segments
      debit_segment_1,
      debit_segment_2,
      debit_segment_3,
      debit_segment_4,
      debit_segment_5,
      debit_segment_6,
      debit_segment_7,
      debit_segment_8,
      debit_segment_9,
      debit_segment_10,
      credit_segment_1,
      credit_segment_2,
      credit_segment_3,
      credit_segment_4,
      credit_segment_5,
      credit_segment_6,
      credit_segment_7,
      credit_segment_8,
      credit_segment_9,
      credit_segment_10,

      -- additive fields
      hash_total,
      amount,
      funcional_currency_amount,
      exchange_rate,
      reporting_currency_exchange_rate,
     
     -- flags
      is_revenue_recognition_type,
      is_summary,
      is_manual_reversal,
      is_active,
      is_cost_or_vairable_consideration,
      is_open_interface,
      is_auto_approved,
      is_unbilled,

      -- metadata
      {{ get_date_id('manual_journal_entry_line_created_date') }}           AS manual_journal_entry_line_created_date_id,
      {{ get_date_id('manual_journal_entry_line_updated_date') }}           AS manual_journal_entry_line_updated_date_id

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