WITH zuora_refund_base AS (

	SELECT *
	FROM {{ref('zuora_refund')}}

), zuora_account AS (

	SELECT * FROM {{ref('zuora_account')}}

)

SELECT
  zr.*,
	date_trunc('month',zr.refund_date)::DATE   AS refund_month,
	za.sfdc_entity,
	za.account_name,
	za.account_number,
	za.currency

FROM zuora_refund_base zr
LEFT JOIN zuora_account za
  ON zr.account_id = za.account_id
