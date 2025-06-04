WITH source AS (
    
    SELECT * 
    FROM {{ source('salesforce', 'group_member') }}

), renamed AS (

       SELECT
      
         --keys
         id AS group_member_id,
         groupid AS group_id,
         userorgroupid AS user_or_group_id,

         --Stitch metadata
         systemmodstamp AS system_mod_stamp,
         _sdc_batched_at AS sdc_batched_at,
         _sdc_extracted_at AS sdc_extracted_at,
         _sdc_received_at AS sdc_received_at,
         _sdc_sequence AS sdc_sequence,
         _sdc_table_version AS sdc_table_version
      
       FROM source
)


SELECT *
FROM renamed

