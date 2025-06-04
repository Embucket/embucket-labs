WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_hg_insights_technographics_snapshots') }}
                 
), renamed AS (
    SELECT
      -- ids

      id::VARCHAR                                 AS hg_unique_record_id,
      NAME::VARCHAR                               AS hg_name_id,
      HG_INSIGHTS__HGFORCEID__C::VARCHAR          AS hg_sfdc_id,
      HG_INSIGHTS__HGID__C::VARCHAR               AS hg_id,
      HG_INSIGHTS__ACCOUNT__C::VARCHAR            AS crm_account_id,
      HG_INSIGHTS__LEAD__C::VARCHAR               AS crm_lead_id,
      HG_INSIGHTS__CATEGORYPARENTID__C::VARCHAR   AS hg_parent_category_id,
      HG_INSIGHTS__CATEGORYID__C::VARCHAR         AS hg_category_id,
      HG_INSIGHTS__VENDORID__C::NUMBER            AS hg_vendor_id,
      HG_INSIGHTS__PRODUCTID__C::NUMBER           AS hg_product_id,
      
      -- info
      HG_INSIGHTS__CATEGORYPARENT__C::VARCHAR     AS hg_parent_category, 
      HG_INSIGHTS__CATEGORY__C::VARCHAR           AS hg_category,
      HG_INSIGHTS__VENDOR__C::VARCHAR             AS hg_vendor,
      HG_INSIGHTS__PRODUCT__C::VARCHAR            AS hg_product,
      HG_INSIGHTS__INTENSITY_NUMBER__C::NUMBER    AS hg_intensity,
      HG_INSIGHTS__LOCATIONS__C::NUMBER           AS hg_locations,
                                                  
      -- metadata
      HG_INSIGHTS__DATE_FIRST_VERIFIED__C::DATE   AS hg_date_first_verified,
      HG_INSIGHTS__COUNTRYDATELASTVERIFIED__C::DATE AS hg_date_last_verified,
      createdbyid::VARCHAR                        AS created_by_id,
      createddate::TIMESTAMP                      AS created_date,
      isdeleted::BOOLEAN                          AS is_deleted,
      lastmodifiedbyid::VARCHAR                   AS last_modified_by_id,
      lastmodifieddate::TIMESTAMP                 AS last_modified_date,
      _sdc_received_at::TIMESTAMP                 AS sfdc_received_at,
      _sdc_extracted_at::TIMESTAMP                AS sfdc_extracted_at,
      _sdc_table_version::NUMBER                  AS sfdc_table_version,
      _sdc_batched_at::TIMESTAMP                  AS sfdc_batched_at,
      _sdc_sequence::NUMBER                       AS sfdc_sequence,
      systemmodstamp::TIMESTAMP                   AS system_mod_stamp,
      dbt_valid_from::TIMESTAMP                   AS dbt_valid_from,
      dbt_valid_to::TIMESTAMP                     AS dbt_valid_to

    FROM source
)
SELECT *
FROM renamed
