WITH source AS (
    
    SELECT * 
    FROM {{ source('salesforce', 'user_territory_association') }}

), renamed AS (

       SELECT
      
         --keys
         id AS user_territory_association_id,

         --info
         roleinterritory2 as role_in_territory,
         territory2id AS territory_id,
         userid AS user_id,
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