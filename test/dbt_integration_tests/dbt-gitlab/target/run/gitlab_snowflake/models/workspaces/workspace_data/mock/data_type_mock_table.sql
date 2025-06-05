
  
    

create or replace transient table EMBUCKET.workspace_data.data_type_mock_table_v1
    

    
    as (SELECT
  'A'::VARCHAR AS the_varchar,
  0.01::FLOAT AS the_float,
  1::INTEGER AS the_int,
  to_array('A')::ARRAY AS the_array,
  parse_json('{"A":"a","B":"b"}')::VARIANT AS the_variant,
  '2022-04-18'::DATE AS the_date,
  '2022-04-18 01:01:01'::TIMESTAMP AS the_timestamp,
  TRUE::BOOLEAN AS the_boolean


  --  EMBUCKET.mock_source.mock_source_table
    )
;


  