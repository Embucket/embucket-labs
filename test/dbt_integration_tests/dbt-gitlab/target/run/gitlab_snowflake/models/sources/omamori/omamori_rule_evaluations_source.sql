
  
    

create or replace transient table EMBUCKET.omamori.omamori_rule_evaluations_source
    

    
    as (







WITH source AS (
  SELECT
    PARSE_JSON(value) AS json_value,
    -- obtain 'uploaded_at_gcs' from the parquet filename, i.e:
    -- 'entity_data/20230502/20230502T000025-entity_data-from-19700101T000000-until-20230426T193740.parquet'
    TO_TIMESTAMP('20240417T123000', 'YYYYMMDDTHH24MISS') AS uploaded_at_gcs
  FROM
    RAW.omamori.rule_evaluations_external

  
),

renamed AS (
  SELECT
    json_value['id']::INT                                          AS id,
    json_value['rule']::VARCHAR                                    AS rule,
    json_value['result']::VARCHAR                                  AS outcome,
    json_value['elapsed_ms']::INT                                  AS elapsed_ms,
    json_value['throttled_count']::INT                             AS throttled_count,
    json_value['duplicates_removed_count']::INT                    AS duplicates_removed_count,
    (json_value['created_at']::NUMBER(36, 3) / 1000000)::TIMESTAMP AS created_at,
    (json_value['updated_at']::NUMBER(36, 3) / 1000000)::TIMESTAMP AS updated_at,
    uploaded_at_gcs
  FROM source
),

dedupped AS (
  SELECT * FROM renamed
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
)

SELECT * FROM dedupped
    )
;


  