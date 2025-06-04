WITH source AS (
    
    SELECT * 
    FROM {{ source('salesforce', 'group') }}

), renamed AS (

       SELECT
      
         --keys
         id AS group_id,

         --info
         name AS group_name,
         relatedid AS related_id,
         type AS group_type,
         doesincludebosses AS is_does_include_bosses,

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

