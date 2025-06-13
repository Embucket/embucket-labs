{{ config(
    tags=["mnpi_exception"]
) }}

WITH sfdc_opportunity_source AS (

    SELECT *
    FROM {{ ref('prep_crm_opportunity') }}
    WHERE is_live

), sfdc_account_source AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}
    WHERE NOT is_deleted

), dim_date AS (

    SELECT *
    FROM {{ ref('dim_date') }}

), current_fiscal_year AS (

    SELECT last_day_of_fiscal_quarter
    FROM dim_date
    WHERE date_actual = CURRENT_DATE

), alliance_type AS (

    SELECT
      {{ alliance_partner('fulfillment_partner.account_name', 'partner_account.account_name',
                       'sfdc_opportunity_source.close_date', 'sfdc_opportunity_source.partner_track',
                       'sfdc_opportunity_source.resale_partner_track', 'sfdc_opportunity_source.deal_path',
                       'partner_account.is_focus_partner') }}                                                        AS alliance_type,
      {{ alliance_partner_short('fulfillment_partner.account_name', 'partner_account.account_name',
                             'sfdc_opportunity_source.close_date', 'sfdc_opportunity_source.partner_track',
                             'sfdc_opportunity_source.resale_partner_track', 'sfdc_opportunity_source.deal_path',
                             'partner_account.is_focus_partner') }}                                                  AS alliance_type_short,
      MIN(dim_date.first_day_of_fiscal_quarter)                                                                      AS valid_from,
      MAX(dim_date.last_day_of_fiscal_quarter)                                                                       AS valid_to,
      IFF(valid_to >= MAX(current_fiscal_year.last_day_of_fiscal_quarter), TRUE, FALSE)                              AS is_currently_valid
    FROM sfdc_opportunity_source
    LEFT JOIN sfdc_account_source      AS fulfillment_partner
      ON sfdc_opportunity_source.fulfillment_partner = fulfillment_partner.account_id
    LEFT JOIN sfdc_account_source AS partner_account
      ON sfdc_opportunity_source.partner_account = partner_account.account_id
    LEFT JOIN dim_date
      ON dim_date.date_actual = sfdc_opportunity_source.close_date
    LEFT JOIN current_fiscal_year
    WHERE alliance_type IS NOT NULL
    GROUP BY 1, 2

), final AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['alliance_type']) }}  AS dim_alliance_type_id,
      alliance_type                                     AS alliance_type_name,
      alliance_type_short                               AS alliance_type_short_name,
      valid_from,
      valid_to,
      is_currently_valid
    FROM alliance_type

    UNION ALL

    SELECT
      MD5('-1')                                         AS dim_alliance_type_id,
      'Missing alliance_type_name'                      AS alliance_type_name,
      'Missing alliance_type_short_name'                AS alliance_type_short_name,
      '2000-02-01'                                      AS valid_from,
      '2040-01-31'                                      AS valid_to,
      TRUE                                              AS is_currently_valid
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@chrissharp",
    created_date="2021-04-07",
    updated_date="2023-06-23"
) }}
