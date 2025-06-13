WITH schedule_source AS (

    SELECT *
    FROM {{ ref('zuora_revenue_revenue_contract_schedule_source') }}

), calendar_source AS (

   SELECT *
   FROM {{ ref('zuora_revenue_calendar_source') }}

), accounting_type_source AS (

   SELECT *
   FROM {{ ref('zuora_revenue_accounting_type_source') }}

), final AS (

    SELECT

       -- ids
       {{ dbt_utils.generate_surrogate_key(['schedule_source.revenue_contract_schedule_id', 'schedule_source.revenue_contract_line_id',
                                   'calendar_source.period_id', 'schedule_source.period_id']) }}                                        AS dim_waterfall_summary_id,
       schedule_source.revenue_contract_schedule_id                                                                                     AS dim_revenue_contract_schedule_id,
       schedule_source.revenue_contract_line_id                                                                                         AS dim_revenue_contract_line_id,
       schedule_source.accounting_type_id                                                                                               AS dim_accounting_type_id,

       -- dates
       {{ get_date_id('calendar_source.period_id') }}                                                                                   AS as_of_period_date_id,
       {{ get_date_id('schedule_source.period_id') }}                                                                                   AS period_date_id,
       {{ get_date_id('schedule_source.posted_period_id') }}                                                                            AS posted_period_date_id,

       schedule_source.amount                                                                                                           AS transactional_amount,
       schedule_source.amount * schedule_source.functional_currency_exchange_rate                                                       AS functional_amount,
       (schedule_source.amount * schedule_source.functional_currency_exchange_rate) * schedule_source.reporting_currency_exchange_rate  AS reporting_amount,

       -- metadata
       {{ get_date_id('schedule_source.revenue_contract_schedule_created_date') }}                                                      AS revenue_contract_schedule_created_date_id,
       schedule_source.revenue_contract_schedule_created_by,
       {{ get_date_id('schedule_source.revenue_contract_schedule_updated_date') }}                                                      AS revenue_contract_schedule_updated_date_id,
       schedule_source.revenue_contract_schedule_updated_by,
       {{ get_date_id('schedule_source.revenue_contract_schedule_updated_date') }}                                                      AS incremental_update_date,
       schedule_source.security_attribute_value

    FROM schedule_source
    INNER JOIN calendar_source 
      ON schedule_source.revenue_contract_schedule_created_period_id <= calendar_source.period_id AND schedule_source.period_id >= calendar_source.period_id
    INNER JOIN accounting_type_source
      ON schedule_source.accounting_type_id = accounting_type_source.accounting_type_id 
   WHERE accounting_type_source.is_waterfall_account = 'Y'

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2021-06-21",
    updated_date="2021-06-21",
    ) 
 }}