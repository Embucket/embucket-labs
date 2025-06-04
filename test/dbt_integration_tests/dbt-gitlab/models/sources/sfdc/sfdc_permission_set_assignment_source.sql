WITH source AS (
    
    SELECT * 
    FROM {{ source('salesforce', 'permission_set_assignment') }}

), renamed AS (

       SELECT
      
         --keys
         id AS permission_set_assignment_id,

         --info
         assigneeid AS assignee_id,
         permissionsetgroupid AS permission_set_group_id,
         permissionsetid AS permission_set_id,
         isactive AS is_active,

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

