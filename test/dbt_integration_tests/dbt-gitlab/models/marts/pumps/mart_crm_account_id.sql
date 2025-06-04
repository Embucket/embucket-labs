WITH all_accounts AS (
  SELECT DISTINCT crm_account_id
  FROM {{ ref('model_mart_crm_account_id') }}

),

pte_scores AS (
  SELECT
    crm_account_id,
    pte_score,
    pte_stars,
    pte_insights,
    pte_uptier_likely
  FROM {{ ref('model_mart_crm_account_id') }}
  WHERE latest_pte_score = TRUE

),

ptc_scores AS (
  SELECT
    crm_account_id,
    ptc_score,
    ptc_stars,
    ptc_insights,
    ptc_downtier_likely
  FROM {{ ref('model_mart_crm_account_id') }}
  WHERE latest_ptc_score = TRUE

)

SELECT
  all_accounts.crm_account_id AS id,
  pte_stars AS pte_score_value__c,
  pte_insights AS pte_insights__c,
  pte_uptier_likely AS pte_likely_to_uptier__c,
  ptc_downtier_likely AS ptc_downtier_likely__c,
  ptc_stars AS ptc_score_value__c,
  ptc_insights AS ptc_insights__c,
  pte_score * 100 AS pte_percent__c,
  ptc_score * 100 AS ptc_percent__c,
  SYSDATE() as updated_at
FROM all_accounts
LEFT JOIN pte_scores
          ON all_accounts.crm_account_id = pte_scores.crm_account_id
LEFT JOIN ptc_scores
          ON all_accounts.crm_account_id = ptc_scores.crm_account_id