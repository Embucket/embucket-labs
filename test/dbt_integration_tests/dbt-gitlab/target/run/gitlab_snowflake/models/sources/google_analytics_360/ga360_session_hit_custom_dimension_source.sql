
  
    

create or replace transient table EMBUCKET.google_analytics_360.ga360_session_hit_custom_dimension_source
    

    
    as (WITH source AS (

	SELECT *
	FROM EMBUCKET.google_analytics_360_fivetran.session_hit_custom_dimension

), renamed AS (

    SELECT
      --Keys
      visit_id::FLOAT                              AS visit_id,
      visitor_id::VARCHAR                          AS visitor_id,

      --Info
      visit_start_time::TIMESTAMP_TZ               AS visit_start_time,
      hit_number::NUMBER                           AS hit_number,
      index::FLOAT                                 AS dimension_index,
      value::VARCHAR                               AS dimension_value,
      _fivetran_synced::TIMESTAMP_TZ               AS fivetran_synced

    FROM source

)



SELECT *
FROM renamed
    )
;


  