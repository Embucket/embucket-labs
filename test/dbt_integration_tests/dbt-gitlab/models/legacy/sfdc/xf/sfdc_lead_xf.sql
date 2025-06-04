WITH sfdc_lead AS (

    SELECT
	{{ dbt_utils.star(from=ref('sfdc_lead'), except=["LEAD_EMAIL", "LEAD_NAME"]) }}
    FROM {{ ref('sfdc_lead') }}

), sfdc_record_type as (

    SELECT *
    FROM {{ ref('sfdc_record_type') }}     

), joined as (

    SELECT
      sfdc_lead.*,
      sfdc_record_type.business_process_id,
      sfdc_record_type.record_type_label,
      sfdc_record_type.record_type_description,
      sfdc_record_type.record_type_modifying_object_type
    FROM sfdc_lead
    LEFT JOIN sfdc_record_type
      ON sfdc_lead.record_type_id = sfdc_record_type.record_type_id

)

SELECT *       
FROM joined
