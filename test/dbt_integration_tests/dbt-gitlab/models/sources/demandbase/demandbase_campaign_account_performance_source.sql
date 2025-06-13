WITH source AS (

    SELECT *
    FROM {{ source('demandbase', 'campaign_account_performance') }}

), renamed AS (

    SELECT
      jsontext['account_id']::NUMBER                AS account_id,
      jsontext['campaign_id']::NUMBER               AS campaign_id,
      jsontext['click_count']::NUMBER               AS click_count,
      jsontext['click_through_rate']::NUMBER        AS click_through_rate,
      jsontext['effective_cpc_cents']::NUMBER       AS effective_cpc_cents,
      jsontext['effective_cpm_cents']::NUMBER       AS effective_cpm_cents,
      jsontext['effective_spend_cents']::NUMBER     AS effective_spend_cents,
      jsontext['impression_count']::NUMBER          AS impression_count,
      jsontext['is_current_account']::BOOLEAN       AS is_current_account,
      jsontext['page_view_count']::NUMBER           AS page_view_count,
      jsontext['partition_date']::DATE              AS partition_date
    FROM source
    WHERE partition_date = (
        SELECT MAX(jsontext['partition_date']::DATE) 
        FROM source
    )

)

SELECT *
FROM renamed