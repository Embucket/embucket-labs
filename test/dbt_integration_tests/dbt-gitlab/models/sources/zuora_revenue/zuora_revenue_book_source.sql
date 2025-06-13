WITH zuora_revenue_book AS (

    SELECT *
    FROM {{source('zuora_revenue','zuora_revenue_book')}}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY incr_updt_dt DESC) = 1

), renamed AS (

    SELECT 
    
      id ::VARCHAR                              AS book_id,
      name::VARCHAR                             AS book_name,
      description::VARCHAR                      AS book_description,
      type::VARCHAR                             AS book_type,
      rc_prefix::VARCHAR                        AS revenue_contract_prefix,
      CONCAT(open_prd_id::VARCHAR,'01')         AS book_open_period_id,
      start_date::DATETIME                      AS book_start_date,
      end_date::VARCHAR                         AS book_end_date,
      asst_segments::VARCHAR                    AS asset_segment,
      lblty_segments::VARCHAR                   AS liabilty_segment,
      allocation_flag::VARCHAR                  AS is_allocation,
      bndl_expl_flag::VARCHAR                   AS is_bundle_explosion,
      hard_freeze_flag ::VARCHAR                AS is_hard_freeze,
      ltst_enabled_flag::VARCHAR                AS is_ltst_enabled,
      postable_flag::VARCHAR                    AS is_postable,
      primary_book_flag::VARCHAR                AS is_primary_book,
      soft_freeze_flag::VARCHAR                 AS is_soft_freeze,
      client_id::VARCHAR                        AS client_id,
      CONCAT(crtd_prd_id::VARCHAR,'01')         AS book_created_period_id,
      crtd_dt::DATETIME                         AS book_created_date,
      crtd_by::VARCHAR                          AS book_created_by,
      updt_dt::DATETIME                         AS book_updated_date,
      updt_by::VARCHAR                          AS book_updated_by,
      incr_updt_dt::DATETIME                    AS incremental_update_date,
      enabled_flag::VARCHAR                     AS is_enabled

    FROM zuora_revenue_book

)

SELECT *
FROM renamed