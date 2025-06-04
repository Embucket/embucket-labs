{% docs zuora_revenue_accounting_type_source %}

Accounting type for all accounts used in Zuora Revenue.

{% enddocs %}

{% docs zuora_revenue_approval_detail_source %}

Record of approvals required for revenue contracts. Data received from Zuora in YYYYMM format, formatted to YYYYMMDD using the following logic: CONCAT(date_id::VARCHAR,'01')::INT.


{% enddocs %}

{% docs zuora_revenue_book_source %}

Accounting books used in Zuora Revenue.

{% enddocs %}

{% docs zuora_revenue_calendar_source %}

Calendars used for recognizing revenue in Zuora Revenue. Data received from Zuora in YYYYMM format, formatted to YYYYMMDD using the following logic: CONCAT(date_id::VARCHAR,'01')::INT.

{% enddocs %}

{% docs zuora_revenue_manual_journal_entry_source %}

Manual journal entry adjustments to revenue contracts. Data received from Zuora in YYYYMM format, formatted to YYYYMMDD using the following logic: CONCAT(date_id::VARCHAR,'01')::INT.

{% enddocs %}

{% docs zuora_revenue_organization_source %}

The organizations who have revenue to be recognized in Zuora Revenue.

{% enddocs %}

{% docs zuora_revenue_revenue_contract_bill_source %}

Customer bills generated from revenue contracts. Data received from Zuora in YYYYMM format, formatted to YYYYMMDD using the following logic: CONCAT(date_id::VARCHAR,'01')::INT.

{% enddocs %}

{% docs zuora_revenue_revenue_contract_header_source %}

Revenue contract details rolled up from the lines. Data received from Zuora in YYYYMM format, formatted to YYYYMMDD using the following logic: CONCAT(date_id::VARCHAR,'01')::INT.

{% enddocs %}

{% docs zuora_revenue_revenue_contract_hold_source %}

Record of holds placed on revenue contracts. Data received from Zuora in YYYYMM format, formatted to YYYYMMDD using the following logic: CONCAT(date_id::VARCHAR,'01')::INT.

{% enddocs %}

{% docs zuora_revenue_revenue_contract_line_source %}

Line items from a revenue contract. Data received from Zuora in YYYYMM format, formatted to YYYYMMDD using the following logic: CONCAT(date_id::VARCHAR,'01')::INT.

{% enddocs %}

{% docs zuora_revenue_revenue_contract_performance_obligation_source %}

Revenue contracts are split into invoiced items which are further divided into specific performance obligations which must be satisfied in order to recognize the revenue. Data received from Zuora in YYYYMM format, formatted to YYYYMMDD using the following logic: CONCAT(date_id::VARCHAR,'01')::INT.

{% enddocs %}

{% docs zuora_revenue_revenue_contract_schedule_source %}

Timeline for recognizing the revenue from revenue contract line items. Data received from Zuora in YYYYMM format, formatted to YYYYMMDD using the following logic: CONCAT(date_id::VARCHAR,'01')::INT.

{% enddocs %}

{% docs zuora_revenue_revenue_contract_schedule_deleted_source %}

Deleted scheduled previously used to understand when revenue from contracts would be recognized. Data received from Zuora in YYYYMM format, formatted to YYYYMMDD using the following logic: CONCAT(date_id::VARCHAR,'01')::INT.

{% enddocs %}

{% docs zuora_revenue_waterfall_source %}

Displays snapshots of revenue contract line items by period showing how much has been recognized from each line item and what will be recognized in the future. Data received from Zuora in YYYYMM format, formatted to YYYYMMDD using the following logic: CONCAT(date_id::VARCHAR,'01')::INT.

{% enddocs %}