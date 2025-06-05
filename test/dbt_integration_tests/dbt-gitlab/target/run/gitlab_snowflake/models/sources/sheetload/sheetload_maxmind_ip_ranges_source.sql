
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_maxmind_ip_ranges_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.maxmind_ranges

), parsed AS (

    SELECT
      network_start_ip::VARCHAR                                   AS ip_range_first_ip,
      network_last_ip::VARCHAR                                    AS ip_range_last_ip,
      PARSE_IP(null, 'inet')['ip_fields'][0]::NUMBER AS ip_range_first_ip_numeric,
      PARSE_IP(null, 'inet')['ip_fields'][0]::NUMBER  AS ip_range_last_ip_numeric,
      geoname_id::NUMBER                                          AS geoname_id,
      registered_country_geoname_id::NUMBER                       AS registered_country_geoname_id,
      represented_country_geoname_id::NUMBER                      AS represented_country_geoname_id,
      null::BOOLEAN                                 AS is_anonymous_proxy,
      null::BOOLEAN                              AS is_satellite_provider   
    FROM source

)

SELECT *
FROM parsed
    )
;


  