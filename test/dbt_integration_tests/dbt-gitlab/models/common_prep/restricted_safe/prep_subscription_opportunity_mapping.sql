WITH zuora_account_source AS (
  
    SELECT *
    FROM {{ ref('zuora_account_source') }}
    WHERE is_deleted = 'FALSE'
      AND batch != 'Batch20'
  
), sfdc_opportunity_source AS (
  
    SELECT *
    FROM {{ ref('sfdc_opportunity_source') }}
    WHERE is_deleted = 'FALSE'
      AND stage_name != '10-Duplicate'
  
 ), zuora_subscription_source AS (

    SELECT 
      prep_subscription.*
    FROM {{ ref('prep_subscription') }}
    INNER JOIN zuora_account_source
      ON prep_subscription.dim_billing_account_id = zuora_account_source.account_id
  
), subscription_opps AS (

    SELECT DISTINCT
      dim_subscription_id       AS subscription_id, 
      dim_crm_opportunity_id    AS opportunity_id
    FROM zuora_subscription_source
    WHERE opportunity_id IS NOT NULL
      AND (subscription_created_date >= '2021-04-12'
        OR subscription_sales_type = 'Self-Service')
       
), zuora_rate_plan_source AS (
  
    SELECT *
    FROM  {{ ref('zuora_rate_plan_source') }}
    WHERE is_deleted = 'FALSE'
  
), zuora_rate_plan_charge_source AS (
  
    SELECT 
      zuora_rate_plan_charge_source.*,
      zuora_rate_plan_source.subscription_id
    FROM  {{ ref('zuora_rate_plan_charge_source') }}
    LEFT JOIN zuora_rate_plan_source
      ON zuora_rate_plan_charge_source.rate_plan_id = zuora_rate_plan_source.rate_plan_id
    WHERE zuora_rate_plan_charge_source.is_deleted = 'FALSE'
  
), prep_crm_account AS (
  
    SELECT *
    FROM {{ ref('prep_crm_account') }}
    WHERE is_deleted = 'FALSE'

), zuora_invoice_item_source AS (

    SELECT *
    FROM {{ ref('zuora_invoice_item_source') }}
    WHERE is_deleted = 'FALSE'

), zuora_invoice_source AS (
  
    SELECT *
    FROM {{ ref('zuora_invoice_source') }}
    WHERE is_deleted = 'FALSE'
  
), sfdc_zqu_quote_source AS (

    SELECT *
    FROM {{ ref('sfdc_zqu_quote_source') }}
    WHERE is_deleted = 'FALSE'
      AND sfdc_zqu_quote_source.zqu__primary = 'TRUE'
  
), quote_opps AS (

    SELECT DISTINCT
      sfdc_zqu_quote_source.zqu__zuora_subscription_id  AS subscription_id, 
      sfdc_zqu_quote_source.zqu__opportunity            AS opportunity_id,
      sfdc_opportunity_source.account_id                AS quote_opp_account_id,
      sfdc_opportunity_source.created_date              AS quote_opp_created_date,
      sfdc_opportunity_source.amount                    AS quote_opp_total_contract_value
    FROM sfdc_zqu_quote_source
    INNER JOIN sfdc_opportunity_source
      ON sfdc_zqu_quote_source.zqu__opportunity = sfdc_opportunity_source.opportunity_id
    WHERE sfdc_zqu_quote_source.zqu__opportunity IS NOT NULL
      AND sfdc_zqu_quote_source.zqu__zuora_subscription_id IS NOT NULL

), invoice_opps AS (

    SELECT DISTINCT
      zuora_invoice_item_source.subscription_id,
      zuora_invoice_source.invoice_number,
      SUM(zuora_invoice_item_source.charge_amount)           AS invoice_item_charge_amount,
      SUM(zuora_invoice_item_source.quantity)                AS invoice_item_quantity,
      sfdc_opportunity_source.opportunity_id,
      sfdc_opportunity_source.account_id                     AS invoice_opp_account_id,
      sfdc_opportunity_source.created_date                   AS invoice_opp_created_date,
      sfdc_opportunity_source.amount                         AS invoice_opp_total_contract_value
    FROM zuora_invoice_item_source
    LEFT JOIN zuora_invoice_source
      ON zuora_invoice_item_source.invoice_id = zuora_invoice_source.invoice_id
    INNER JOIN sfdc_opportunity_source
      ON zuora_invoice_source.invoice_number = sfdc_opportunity_source.invoice_number
    WHERE zuora_invoice_source.status = 'Posted'
      AND zuora_invoice_source.invoice_number IS NOT NULL
      AND sfdc_opportunity_source.opportunity_id IS NOT NULL
    GROUP BY 1,2,5,6,7,8

), subscription_quote_number_opps AS (

    SELECT 
      zuora.subscription_id,
      zuora.sfdc_opportunity_id,
      zuora.crm_opportunity_name,
      sfdc_opportunity_source.opportunity_id,
      sfdc_opportunity_source.account_id            AS subscription_quote_number_opp_account_id,
      sfdc_opportunity_source.created_date          AS subscription_quote_number_opp_created_date,
      sfdc_opportunity_source.amount                AS subscription_quote_number_opp_total_contract_value
    FROM {{ ref('zuora_subscription_source') }} zuora
    LEFT JOIN sfdc_zqu_quote_source
      ON zuora.quote_number = sfdc_zqu_quote_source.zqu__number
    INNER JOIN sfdc_opportunity_source
      ON sfdc_zqu_quote_source.zqu__opportunity = sfdc_opportunity_source.opportunity_id

), final AS (

    SELECT DISTINCT
      zuora_subscription_source.dim_subscription_id                                                                                                                                                                                                                                                                                                                                     AS dim_subscription_id,
      zuora_subscription_source.dim_billing_account_id                                                                                                                                                                                                                                                                                                                                  AS dim_billing_account_id,
      zuora_subscription_source.subscription_name                                                                                                                                                                                                                                                                                                                                       AS subscription_name,
      zuora_subscription_source.subscription_sales_type                                                                                                                                                                                                                                                                                                                          AS subscription_sales_type,
      zuora_subscription_source.dim_crm_account_id                                                                                                                                                                                                                                                                                                                                      AS subscription_account_id,
      prep_crm_account.dim_parent_crm_account_id                                                                                                                                                                                                                                                                                                                                        AS subscription_parent_account_id,
      COALESCE(invoice_opps.invoice_opp_account_id, LAG(invoice_opps.invoice_opp_account_id) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.subscription_created_date ORDER BY zuora_subscription_source.subscription_version))                                                                                                 AS invoice_opp_account_id_forward,
      COALESCE(invoice_opps.invoice_opp_account_id, LEAD(invoice_opps.invoice_opp_account_id) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.subscription_created_date ORDER BY zuora_subscription_source.subscription_version))                                                                                                AS invoice_opp_account_id_backward,
      COALESCE(quote_opps.quote_opp_account_id, LAG(quote_opps.quote_opp_account_id) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.subscription_created_date ORDER BY zuora_subscription_source.subscription_version))                                                                                                         AS quote_opp_account_id_forward,
      COALESCE(quote_opps.quote_opp_account_id, LEAD(quote_opps.quote_opp_account_id) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.subscription_created_date ORDER BY zuora_subscription_source.subscription_version))                                                                                                        AS quote_opp_account_id_backward,
      COALESCE(subscription_quote_number_opps.subscription_quote_number_opp_account_id, LAG(subscription_quote_number_opps.subscription_quote_number_opp_account_id) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.subscription_created_date ORDER BY zuora_subscription_source.subscription_version))                         AS subscription_opp_name_opp_account_id_forward,
      COALESCE(subscription_quote_number_opps.subscription_quote_number_opp_account_id, LEAD(subscription_quote_number_opps.subscription_quote_number_opp_account_id) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.subscription_created_date ORDER BY zuora_subscription_source.subscription_version))                        AS subscription_opp_name_opp_account_id_backward,
      zuora_subscription_source.subscription_version                                                                                                                                                                                                                                                                                                                                    AS subscription_version,
      zuora_subscription_source.term_start_date                                                                                                                                                                                                                                                                                                                                         AS term_start_date,
      zuora_subscription_source.term_end_date                                                                                                                                                                                                                                                                                                                                           AS term_end_date,
      zuora_subscription_source.subscription_start_date                                                                                                                                                                                                                                                                                                                                 AS subscription_start_date,
      zuora_subscription_source.subscription_end_date                                                                                                                                                                                                                                                                                                                                   AS subscription_end_date,
      zuora_subscription_source.subscription_status                                                                                                                                                                                                                                                                                                                                     AS subscription_status,
      zuora_subscription_source.subscription_created_date                                                                                                                                                                                                                                                                                                                               AS subscription_created_date, 
      zuora_subscription_source.dim_crm_opportunity_id                                                                                                                                                                                                                                                                                                                                  AS subscription_source_opp_id,
      subscription_opps.opportunity_id                                                                                                                                                                                                                                                                                                                                                  AS subscription_opp_id,
      COALESCE(invoice_opps.opportunity_id, LAG(invoice_opps.opportunity_id) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.subscription_created_date ORDER BY zuora_subscription_source.subscription_version))                                                                                                                 AS invoice_opp_id_forward,
      COALESCE(invoice_opps.opportunity_id, LEAD(invoice_opps.opportunity_id) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.subscription_created_date ORDER BY zuora_subscription_source.subscription_version))                                                                                                                AS invoice_opp_id_backward,
      COALESCE(invoice_opps.opportunity_id, LAG(invoice_opps.opportunity_id) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.term_start_date, zuora_subscription_source.term_end_date ORDER BY zuora_subscription_source.subscription_version))                                                                                  AS invoice_opp_id_forward_term_based,
      COALESCE(invoice_opps.opportunity_id, LEAD(invoice_opps.opportunity_id) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.term_start_date, zuora_subscription_source.term_end_date ORDER BY zuora_subscription_source.subscription_version))                                                                                 AS invoice_opp_id_backward_term_based,
      COALESCE(invoice_opps.opportunity_id, LAG(invoice_opps.opportunity_id) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name ORDER BY zuora_subscription_source.subscription_version))                                                                                                                                                                      AS invoice_opp_id_forward_sub_name,
      invoice_opps.opportunity_id                                                                                                                                                                                                                                                                                                                                                       AS unfilled_invoice_opp_id,
      COALESCE(quote_opps.opportunity_id, LAG(quote_opps.opportunity_id) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.subscription_created_date ORDER BY zuora_subscription_source.subscription_version))                                                                                                                     AS quote_opp_id_forward,
      COALESCE(quote_opps.opportunity_id, LEAD(quote_opps.opportunity_id) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.subscription_created_date ORDER BY zuora_subscription_source.subscription_version))                                                                                                                    AS quote_opp_id_backward,
      COALESCE(quote_opps.opportunity_id, LAG(quote_opps.opportunity_id) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.term_start_date, zuora_subscription_source.term_end_date ORDER BY zuora_subscription_source.subscription_version))                                                                                      AS quote_opp_id_forward_term_based,
      COALESCE(quote_opps.opportunity_id, LEAD(quote_opps.opportunity_id) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.term_start_date, zuora_subscription_source.term_end_date ORDER BY zuora_subscription_source.subscription_version))                                                                                     AS quote_opp_id_backward_term_based,
      COALESCE(quote_opps.opportunity_id, LAG(quote_opps.opportunity_id) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name ORDER BY zuora_subscription_source.subscription_version))                                                                                                                                                                          AS quote_opp_id_forward_sub_name,
      quote_opps.opportunity_id                                                                                                                                                                                                                                                                                                                                                         AS unfilled_quote_opp_id,
      COALESCE(subscription_quote_number_opps.opportunity_id, LAG(subscription_quote_number_opps.opportunity_id) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.subscription_created_date ORDER BY zuora_subscription_source.subscription_version))                                                                             AS subscription_quote_number_opp_id_forward,
      COALESCE(subscription_quote_number_opps.opportunity_id, LEAD(subscription_quote_number_opps.opportunity_id) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.subscription_created_date ORDER BY zuora_subscription_source.subscription_version))                                                                            AS subscription_quote_number_opp_id_backward,
      COALESCE(subscription_quote_number_opps.opportunity_id, LAG(subscription_quote_number_opps.opportunity_id) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.term_start_date, zuora_subscription_source.term_end_date ORDER BY zuora_subscription_source.subscription_version))                                              AS subscription_quote_number_opp_id_forward_term_based,
      COALESCE(subscription_quote_number_opps.opportunity_id, LEAD(subscription_quote_number_opps.opportunity_id) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.term_start_date, zuora_subscription_source.term_end_date ORDER BY zuora_subscription_source.subscription_version))                                             AS subscription_quote_number_opp_id_backward_term_based,
      COALESCE(subscription_quote_number_opps.opportunity_id, LAG(subscription_quote_number_opps.opportunity_id) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name ORDER BY zuora_subscription_source.subscription_version))                                                                                                                                  AS subscription_quote_number_opp_id_forward_sub_name,
      subscription_quote_number_opps.opportunity_id                                                                                                                                                                                                                                                                                                                                     AS unfilled_subscription_quote_number_opp_id,
      CASE
        WHEN zuora_subscription_source.subscription_sales_type = 'Sales-Assisted' 
          THEN COALESCE(subscription_opp_id, 
                        subscription_quote_number_opp_id_forward, subscription_quote_number_opp_id_backward,  
                        invoice_opp_id_forward, invoice_opp_id_backward,
                        quote_opp_id_forward, quote_opp_id_backward,
                        subscription_quote_number_opp_id_backward_term_based,
                        invoice_opp_id_backward_term_based,invoice_opp_id_forward_term_based,
                        quote_opp_id_backward_term_based,quote_opp_id_forward_term_based,
                        subscription_quote_number_opp_id_forward_sub_name, invoice_opp_id_forward_sub_name, quote_opp_id_forward_sub_name
                       ) -- prefer quote number on subscription if sales-assisted
        ELSE COALESCE(subscription_opp_id, 
                      invoice_opp_id_forward, invoice_opp_id_backward, 
                      quote_opp_id_forward, quote_opp_id_backward, 
                      invoice_opp_id_backward_term_based, invoice_opp_id_forward_term_based,
                      quote_opp_id_backward_term_based,quote_opp_id_forward_term_based,
                      invoice_opp_id_forward_sub_name, quote_opp_id_forward_sub_name
                     ) -- don't take quote_number on subscription for self-service
      END                                                                                                                                                                                                                                                                                                                                                                               AS combined_opportunity_id,
      COALESCE(invoice_opps.invoice_opp_created_date, LEAD(invoice_opps.invoice_opp_created_date) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.subscription_created_date ORDER BY zuora_subscription_source.subscription_version))                                                                                            AS invoice_opp_created_date_forward,
      COALESCE(invoice_opps.invoice_opp_created_date, LAG(invoice_opps.invoice_opp_created_date) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.subscription_created_date ORDER BY zuora_subscription_source.subscription_version))                                                                                             AS invoice_opp_created_date_backward,
      COALESCE(quote_opps.quote_opp_created_date, LEAD(quote_opps.quote_opp_created_date) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.subscription_created_date ORDER BY zuora_subscription_source.subscription_version))                                                                                                    AS quote_opp_created_date_forward,
      COALESCE(quote_opps.quote_opp_created_date, LAG(quote_opps.quote_opp_created_date) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.subscription_created_date ORDER BY zuora_subscription_source.subscription_version))                                                                                                     AS quote_opp_created_date_backward,
      COALESCE(invoice_opps.invoice_opp_total_contract_value, LEAD(invoice_opps.invoice_opp_total_contract_value) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.subscription_created_date ORDER BY zuora_subscription_source.subscription_version))                                                                            AS invoice_opp_total_contract_value_forward,
      COALESCE(invoice_opps.invoice_opp_total_contract_value, LAG(invoice_opps.invoice_opp_total_contract_value) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.subscription_created_date ORDER BY zuora_subscription_source.subscription_version))                                                                             AS invoice_opp_total_contract_value_backward,
      COALESCE(quote_opps.quote_opp_total_contract_value, LEAD(quote_opps.quote_opp_total_contract_value) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.subscription_created_date ORDER BY zuora_subscription_source.subscription_version))                                                                                    AS quote_opp_total_contract_value_forward,
      COALESCE(quote_opps.quote_opp_total_contract_value, LAG(quote_opps.quote_opp_total_contract_value) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.subscription_created_date ORDER BY zuora_subscription_source.subscription_version))                                                                                     AS quote_opp_total_contract_value_backward,
      COALESCE(subscription_quote_number_opps.subscription_quote_number_opp_total_contract_value, LEAD(subscription_quote_number_opps.subscription_quote_number_opp_total_contract_value) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.subscription_created_date ORDER BY zuora_subscription_source.subscription_version))    AS subscription_quote_number_opp_total_contract_value_forward,
      COALESCE(subscription_quote_number_opps.subscription_quote_number_opp_total_contract_value, LAG(subscription_quote_number_opps.subscription_quote_number_opp_total_contract_value) IGNORE NULLS OVER (PARTITION BY zuora_subscription_source.subscription_name, zuora_subscription_source.subscription_created_date ORDER BY zuora_subscription_source.subscription_version))     AS subscription_quote_number_opp_total_contract_value_backward,
      invoice_opps.invoice_number                                                                                                                                                                                                                                                                                                                                                       AS invoice_number,
      invoice_opps.invoice_item_charge_amount                                                                                                                                                                                                                                                                                                                                           AS invoice_item_charge_amount,
      invoice_opps.invoice_item_quantity                                                                                                                                                                                                                                                                                                                                                AS invoice_item_quantity
    FROM zuora_subscription_source
    LEFT JOIN subscription_opps
      ON zuora_subscription_source.dim_subscription_id = subscription_opps.subscription_id
    LEFT JOIN invoice_opps
      ON zuora_subscription_source.dim_subscription_id = invoice_opps.subscription_id
    LEFT JOIN quote_opps
      ON zuora_subscription_source.dim_subscription_id = quote_opps.subscription_id
    LEFT JOIN subscription_quote_number_opps
      ON zuora_subscription_source.dim_subscription_id = subscription_quote_number_opps.subscription_id
    LEFT JOIN prep_crm_account
      ON zuora_subscription_source.dim_crm_account_id = prep_crm_account.dim_crm_account_id

), final_subs_opps AS (

    SELECT
      final.*
    FROM final
    INNER JOIN zuora_account_source
      ON final.dim_billing_account_id = zuora_account_source.account_id
    WHERE subscription_created_date >= '2019-02-01'
  
), complete_subs AS (
  
    SELECT
      subscription_name,
      COUNT_IF(combined_opportunity_id IS NOT NULL)                         AS other_count_test,
      SUM(CASE WHEN combined_opportunity_id IS NOT NULL THEN 1 ELSE 0 END)  AS count_test,
      COUNT(dim_subscription_id)                                            AS sub_count
    FROM final_subs_opps
    GROUP BY 1
  
), non_duplicates AS ( -- All subscription_ids that do not have multiple opportunities associated with them
  
    SELECT *
    FROM final_subs_opps
    WHERE dim_subscription_id NOT IN (SELECT dim_subscription_id FROM final GROUP BY dim_subscription_id HAVING COUNT(*) > 1) 
    
), dupes AS ( -- GET ALL SUBSCRIPTION_IDS WITH MULTIPLE OPPORTUNITY_IDS, DUPLICATES (6,620) (4,600 -- with stage_name != '10-duplicate')

    SELECT *
    FROM final_subs_opps
    WHERE dim_subscription_id IN (SELECT dim_subscription_id FROM final GROUP BY dim_subscription_id HAVING COUNT(*) > 1) 

),invoice_item_amount AS (
  
    SELECT 
      dim_invoice_id, 
      invoice_number, 
      dim_subscription_id, 
      SUM(invoice_item_charge_amount) AS invoice_item_charge_amount, 
      AVG(quantity) AS quantity
    FROM {{ ref('fct_invoice_item') }}
    {{ dbt_utils.group_by(n=3) }}

), multiple_opps_on_one_invoice AS (

    SELECT DISTINCT
      ii.dim_subscription_id,
      dupes.subscription_name,
      dupes.subscription_version,
      ii.dim_invoice_id, 
      ii.invoice_number,
      ii.quantity,
      to_varchar(quantity, '999,999,999,999')   AS formatted_quantity,
      trim(
            lower(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            opp.opportunity_name
                        , '\\s+\\|{2}\\s+', '|')
                    , '[ ]{2,}', ' ')
                , '[^A-Za-z0-9|]', '-')
                )
            )                                                                                                           AS opp_name_slugify,
      trim(
            lower(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            formatted_quantity
                        , '\\s+\\|{2}\\s+', '|')
                    , '[ ]{2,}', ' ')
                , '[^A-Za-z0-9|]', '-')
                )
            )                                                                                                           AS formatted_quantity_slugify,
      opp.dim_crm_opportunity_id,
      opp.opportunity_name, 
      fct_opp.amount                                                                                                    AS opportunity_amount, 
      ii.invoice_item_charge_amount, 
      IFF(ROUND(opportunity_amount,2) = ROUND(ii.invoice_item_charge_amount,2),5,0)                                     AS opp_invoice_amount_match, 
      IFF(CONTAINS(opp_name_slugify, formatted_quantity_slugify),5,0)                                                   AS slugify_quantity_name_match,
      IFF(CONTAINS(opportunity_name, formatted_quantity),1,0)                                                           AS formatted_quantity_name_match,  
      opp_invoice_amount_match + slugify_quantity_name_match + formatted_quantity_name_match AS total
    FROM dupes
    INNER JOIN invoice_item_amount ii
      ON dupes.dim_subscription_id = ii.dim_subscription_id
        AND dupes.invoice_number = ii.invoice_number
    INNER JOIN {{ ref('dim_crm_opportunity') }} AS opp
      ON ii.invoice_number = opp.invoice_number
    INNER JOIN {{ ref('fct_crm_opportunity') }} AS fct_opp
      ON opp.dim_crm_opportunity_id = fct_opp.dim_crm_opportunity_id
    WHERE opp.stage_name <> '10-Duplicate'

), multiple_opps_on_one_invoice_matches AS (
  
    SELECT *
    FROM multiple_opps_on_one_invoice
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_subscription_id ORDER BY total DESC) = 1

), dupes_with_amount_matches AS (

    SELECT dupes.*
    FROM dupes
    INNER JOIN multiple_opps_on_one_invoice_matches 
      ON dupes.dim_subscription_id = multiple_opps_on_one_invoice_matches.dim_subscription_id
        AND dupes.unfilled_invoice_opp_id = multiple_opps_on_one_invoice_matches.dim_crm_opportunity_id
    WHERE total > 0

), dupes_without_amount_matches AS (

    SELECT *
    FROM dupes 
    WHERE dim_subscription_id NOT IN (SELECT DISTINCT dim_subscription_id FROM dupes_with_amount_matches) -- 460 non-distinct, 200 distinct
  
), multi_invoice_subs_with_opp_amounts AS (

    SELECT
       dim_subscription_id,
       ROUND(AVG(invoice_item_charge_amount),4)                                                  AS invoice_amount, 
       ROUND(SUM(invoice_opp_total_contract_value_forward),4)                                    AS invoice_opp_amount_forward, 
       ROUND(SUM(invoice_opp_total_contract_value_backward),4)                                   AS invoice_opp_amount_backward,
       ROUND(AVG(quote_opp_total_contract_value_forward),4)                                      AS quote_opp_amount_forward,
       ROUND(AVG(quote_opp_total_contract_value_backward),4)                                     AS quote_opp_amount_backward,
       ROUND(AVG(subscription_quote_number_opp_total_contract_value_forward),4)                  AS subscription_quote_number_opp_amount_forward,
       ROUND(AVG(subscription_quote_number_opp_total_contract_value_backward),4)                 AS subscription_quote_number_opp_amount_backward
    FROM dupes_without_amount_matches
    GROUP BY 1
  
), multi_invoice_subs_with_opp_amounts_that_sum_to_invoice_total AS (
   
    SELECT *
    FROM multi_invoice_subs_with_opp_amounts
    WHERE invoice_amount = invoice_opp_amount_forward
      OR invoice_amount = invoice_opp_amount_backward
  
), multi_invoice_subs_with_opp_amounts_that_sum_to_invoice_total_first_opp AS (
  
    SELECT *
    FROM dupes
    WHERE dim_subscription_id IN (SELECT DISTINCT dim_subscription_id FROM multi_invoice_subs_with_opp_amounts_that_sum_to_invoice_total)
    QUALIFY RANK() OVER (PARTITION BY dim_subscription_id ORDER BY invoice_opp_created_date_forward) = 1
  
), final_matches_part_1 AS (
  
    SELECT *, 'non-duplicates' AS source
    FROM non_duplicates
    
    UNION 
  
    -- for invoices that have multiple subscriptions on the invoice, take the subscription-opportunity mapping where the invoice amount = opportunity amount 
    SELECT *, 'invoice amount matches opp amount' AS source
    FROM dupes_with_amount_matches
  
    UNION
    
    -- for subscriptions spread across multiple invoices where the opp totals match the total across the invoices, take the first opportunity based on the opportunity created date
    SELECT *, 'multi-invoice single sub' AS source
    FROM multi_invoice_subs_with_opp_amounts_that_sum_to_invoice_total_first_opp

), dupes_part_2 AS ( -- the fixes applied to these duplicates are not as strong, so we are peeling them out and applying different solutions

    SELECT *
    FROM dupes
    WHERE dim_subscription_id NOT IN (SELECT DISTINCT dim_subscription_id FROM final_matches_part_1)
  
), self_service_dupes_with_subscription_opp AS (

    SELECT *
    FROM dupes_part_2
    WHERE subscription_sales_type = 'Self-Service'
      AND subscription_opp_id IS NOT NULL
    QUALIFY RANK() OVER (PARTITION BY dim_subscription_id ORDER BY invoice_opp_id_forward, invoice_opp_id_backward, invoice_opp_id_backward_term_based) = 1

), sales_assisted_dupes_with_quote_num_on_sub AS (

    SELECT *
    FROM dupes_part_2
    WHERE subscription_sales_type = 'Sales-Assisted'
      AND COALESCE(subscription_quote_number_opp_id_forward, subscription_quote_number_opp_id_backward, subscription_quote_number_opp_id_backward_term_based) IS NOT NULL
    QUALIFY RANK() OVER (PARTITION BY dim_subscription_id ORDER BY invoice_opp_id_forward, invoice_opp_id_backward, invoice_opp_id_backward_term_based) = 1
  
), dupes_all_raw_sub_options_match AS (
  
    SELECT *
    FROM dupes_part_2
    WHERE unfilled_invoice_opp_id = unfilled_quote_opp_id
      AND unfilled_quote_opp_id = unfilled_subscription_quote_number_opp_id
      AND dim_subscription_id NOT IN (SELECT DISTINCT dim_subscription_id FROM self_service_dupes_with_subscription_opp
                                     UNION
                                     SELECT DISTINCT dim_subscription_id FROM sales_assisted_dupes_with_quote_num_on_sub)

 ), final_matches AS (
   
    SELECT *
    FROM final_matches_part_1
    UNION
  
    -- for self-service dupes, take the most reliable connection (opportunity id on subscription)
    SELECT *, 'self-service' AS source
    FROM self_service_dupes_with_subscription_opp
  
    UNION
   
    -- for sales_assisted dupes, take the most reliable connection (quote number on subscription)
    SELECT *, 'sales-assisted' AS source
    FROM sales_assisted_dupes_with_quote_num_on_sub  
  
    UNION
  
    -- for all dupes, take the subscription-opportunity options where the raw fields (opp on subscription, opp on invoice, and opp on quote number from subscription) match
    SELECT *, 'all matching opps' AS source
    FROM dupes_all_raw_sub_options_match

), final_matches_with_bad_data_flag AS (

    SELECT 
      final_matches.*,
      IFF(len(SPLIT_PART(combined_opportunity_id,'https://gitlab.my.salesforce.com/',2))=0, NULL, SPLIT_PART(combined_opportunity_id,'https://gitlab.my.salesforce.com/',2))    AS opp_id_remove_salesforce_url,
      {{zuora_slugify("combined_opportunity_id") }}                                                                                                                             AS opp_id_slugify,
      opp_name.opportunity_id                                                                                                                                                   AS opp_id_name,
      COALESCE(opp_id_remove_salesforce_url, opp_id_name, IFF(combined_opportunity_id NOT LIKE '0%', opp_id_slugify, combined_opportunity_id))                                  AS combined_oportunity_id_coalesced,
      CASE 
        WHEN subscription_opp_id IS NULL
          AND invoice_opp_id_forward IS NULL
            AND invoice_opp_id_backward IS NULL
              AND invoice_opp_id_forward_term_based IS NULL
                AND invoice_opp_id_backward_term_based IS NULL
                  AND unfilled_invoice_opp_id IS NULL
                    AND quote_opp_id_forward IS NULL
                      AND quote_opp_id_backward IS NULL
                        AND quote_opp_id_forward_term_based IS NULL
                          AND quote_opp_id_backward_term_based IS NULL
                            AND unfilled_quote_opp_id IS NULL
                              AND subscription_quote_number_opp_id_forward IS NULL
                                AND subscription_quote_number_opp_id_backward IS NULL
                                  AND subscription_quote_number_opp_id_forward_term_based IS NULL
                                    AND subscription_quote_number_opp_id_backward_term_based IS NULL
                                      AND subscription_quote_number_opp_id_forward_sub_name IS NULL
                                        AND unfilled_subscription_quote_number_opp_id IS NULL
                                          AND ( invoice_opp_id_forward_sub_name IS NOT NULL
                                                OR subscription_quote_number_opp_id_forward_sub_name IS NOT NULL
                                                OR quote_opp_id_forward_sub_name IS NOT NULL
                                              )
          THEN 1
        ELSE 0
      END                                                                                                           AS is_questionable_opportunity_mapping
    FROM final_matches
    LEFT JOIN {{ ref('sfdc_opportunity_source') }} opp_name
      ON {{ zuora_slugify("final_matches.combined_opportunity_id") }}  = {{ zuora_slugify("opp_name.opportunity_name") }}

), short_oppty_id AS (

SELECT
  opportunity_id              AS long_oppty_id,
  LEFT(opportunity_id,15)     AS short_oppty_id
FROM  {{ ref('sfdc_opportunity_source') }}

), final_matches_with_long_oppty_id AS (

SELECT 
  final_matches_with_bad_data_flag.*,
  short_oppty_id.long_oppty_id        AS dim_crm_opportunity_id
FROM final_matches_with_bad_data_flag
LEFT JOIN short_oppty_id
  ON LEFT(final_matches_with_bad_data_flag.combined_oportunity_id_coalesced,15) = short_oppty_id.short_oppty_id
  
)

{{ dbt_audit(
    cte_ref="final_matches_with_long_oppty_id",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2021-11-10",
    updated_date="2022-01-19"
) }}