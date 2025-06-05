

WITH zuora_invoice AS (

    SELECT *
    FROM EMBUCKET.zuora.zuora_invoice_source
    WHERE is_deleted = 'FALSE'

), final_invoice AS (

    SELECT

      --keys
      invoice_id            AS dim_invoice_id,
      invoice_number,

      -- invoice metadata
      status,
      comments,
      includes_one_time,
      includesrecurring,
      includes_usage,
      transferred_to_accounting,

      -- invoice dates
      invoice_date,
      due_date,
      last_email_sent_date,
      posted_date,
      target_date,

      -- metadata
      created_by_id,
      created_date,
      posted_by,
      source,
      source_id,
      updated_by_id,
      updated_date

    FROM zuora_invoice

)

SELECT
      *,
      '@mcooperDD'::VARCHAR       AS created_by,
      '@mcooperDD'::VARCHAR       AS updated_by,
      '2021-01-20'::DATE        AS model_created_date,
      '2021-01-20'::DATE        AS model_updated_date,
      CURRENT_TIMESTAMP()               AS dbt_updated_at,

    

        

            CURRENT_TIMESTAMP()               AS dbt_created_at

        
    

    FROM final_invoice