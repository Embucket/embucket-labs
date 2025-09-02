-- Load Snowplow Events Data into Embucket Database
-- This script creates the necessary infrastructure and loads the events_yesterday.csv and events_today.csv data

-- Step 1: Create external volume for local storage
CREATE EXTERNAL VOLUME IF NOT EXISTS local 
STORAGE_LOCATIONS = (
    (NAME = 'local' STORAGE_PROVIDER = 'FILE' STORAGE_BASE_URL = '/app/data')
);

-- Step 2: Use existing database and create schema if needed
USE DATABASE embucket;
CREATE SCHEMA IF NOT EXISTS public_snowplow_manifest;
USE SCHEMA public_snowplow_manifest;

-- Drop existing table if it exists
DROP TABLE IF EXISTS events;

-- Step 3: Create the events table with appropriate data types
CREATE TABLE IF NOT EXISTS events (
    app_id STRING,
    platform STRING,
    etl_tstamp TIMESTAMP_NTZ,
    collector_tstamp TIMESTAMP_NTZ,
    dvce_created_tstamp TIMESTAMP_NTZ,
    event STRING,
    event_id STRING,
    txn_id STRING,
    name_tracker STRING,
    v_tracker STRING,
    v_collector STRING,
    v_etl STRING,
    user_id STRING,
    user_ipaddress STRING,
    user_fingerprint STRING,
    domain_userid STRING,
    domain_sessionidx INTEGER,
    network_userid STRING,
    geo_country STRING,
    geo_region STRING,
    geo_city STRING,
    geo_zipcode STRING,
    geo_latitude DOUBLE,
    geo_longitude DOUBLE,
    geo_region_name STRING,
    ip_isp STRING,
    ip_organization STRING,
    ip_domain STRING,
    ip_netspeed STRING,
    page_url STRING,
    page_title STRING,
    page_referrer STRING,
    page_urlscheme STRING,
    page_urlhost STRING,
    page_urlport INTEGER,
    page_urlpath STRING,
    page_urlquery STRING,
    page_urlfragment STRING,
    refr_urlscheme STRING,
    refr_urlhost STRING,
    refr_urlport INTEGER,
    refr_urlpath STRING,
    refr_urlquery STRING,
    refr_urlfragment STRING,
    refr_medium STRING,
    refr_source STRING,
    refr_term STRING,
    mkt_medium STRING,
    mkt_source STRING,
    mkt_term STRING,
    mkt_content STRING,
    mkt_campaign STRING,
    se_category STRING,
    se_action STRING,
    se_label STRING,
    se_property STRING,
    se_value STRING,
    tr_orderid STRING,
    tr_affiliation STRING,
    tr_total DOUBLE,
    tr_tax DOUBLE,
    tr_shipping DOUBLE,
    tr_city STRING,
    tr_state STRING,
    tr_country STRING,
    ti_orderid STRING,
    ti_sku STRING,
    ti_name STRING,
    ti_category STRING,
    ti_price DOUBLE,
    ti_quantity INTEGER,
    pp_xoffset_min INTEGER,
    pp_xoffset_max INTEGER,
    pp_yoffset_min INTEGER,
    pp_yoffset_max INTEGER,
    useragent STRING,
    br_name STRING,
    br_family STRING,
    br_version STRING,
    br_type STRING,
    br_renderengine STRING,
    br_lang STRING,
    br_features_pdf BOOLEAN,
    br_features_flash BOOLEAN,
    br_features_java BOOLEAN,
    br_features_director BOOLEAN,
    br_features_quicktime BOOLEAN,
    br_features_realplayer BOOLEAN,
    br_features_windowsmedia BOOLEAN,
    br_features_gears BOOLEAN,
    br_features_silverlight BOOLEAN,
    br_cookies BOOLEAN,
    br_colordepth INTEGER,
    br_viewwidth INTEGER,
    br_viewheight INTEGER,
    os_name STRING,
    os_family STRING,
    os_manufacturer STRING,
    os_timezone STRING,
    dvce_type STRING,
    dvce_ismobile BOOLEAN,
    dvce_screenwidth INTEGER,
    dvce_screenheight INTEGER,
    doc_charset STRING,
    doc_width INTEGER,
    doc_height INTEGER,
    tr_currency STRING,
    tr_total_base DOUBLE,
    tr_tax_base DOUBLE,
    tr_shipping_base DOUBLE,
    ti_currency STRING,
    ti_price_base DOUBLE,
    base_currency STRING,
    geo_timezone STRING,
    mkt_clickid STRING,
    mkt_network STRING,
    etl_tags STRING,
    dvce_sent_tstamp TIMESTAMP_NTZ,
    refr_domain_userid STRING,
    refr_dvce_tstamp TIMESTAMP_NTZ,
    domain_sessionid STRING,
    derived_tstamp TIMESTAMP_NTZ,
    event_vendor STRING,
    event_name STRING,
    event_format STRING,
    event_version STRING,
    event_fingerprint STRING,
    true_tstamp TIMESTAMP_NTZ,
    load_tstamp TIMESTAMP_NTZ,
    contexts_com_snowplowanalytics_snowplow_web_page_1 STRING,
    unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1 STRING,
    unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1 STRING,
    contexts_com_iab_snowplow_spiders_and_robots_1 STRING,
    contexts_com_snowplowanalytics_snowplow_ua_parser_context_1 STRING,
    contexts_nl_basjes_yauaa_context_1 STRING,
    unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1 STRING
);

-- Step 4: Load data using COPY INTO with CSV file format
-- Load yesterday's events
COPY INTO events
FROM 'file:///app/data/events_yesterday.csv'
STORAGE_INTEGRATION = local
FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- Load today's events
COPY INTO events
FROM 'file:///app/data/events_today.csv'
STORAGE_INTEGRATION = local
FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- Step 6: Verify the data was loaded
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT event_id) as unique_events,
    COUNT(DISTINCT user_id) as unique_users,
    MIN(collector_tstamp) as earliest_event,
    MAX(collector_tstamp) as latest_event
FROM events;

-- Step 7: Show sample data
SELECT 
    event_id,
    event,
    user_id,
    collector_tstamp,
    page_url,
    geo_country,
    geo_city
FROM events
LIMIT 10; 