WITH source AS (

    SELECT
      dim_parent_crm_account_id::VARCHAR                AS dim_parent_crm_account_id,
      parent_crm_account_sales_segment::VARCHAR         AS dim_parent_crm_account_sales_segment,
      dev_count::FLOAT                                  AS dev_count,
      estimated_capped_lam::FLOAT                       AS estimated_capped_lam,
      valid_from::DATE                                  AS valid_from,
      valid_to_final_date::DATETIME                     AS valid_to
    FROM {{ source('driveload', 'lam_corrections') }}
)

SELECT *
FROM source
