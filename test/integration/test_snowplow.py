"""
Snowplow Web Analytics Integration Test

This test validates Snowplow sessionization logic with incremental processing.
Ported from embucket-integration-tests/snowplow.sh and tests/snowplow.sh.

Tests:
- Incremental batch loading (2 batches)
- Session lifecycle management with lookback windows
- Session quarantining (3+ day sessions)
- Late event handling
- Bot filtering
- Engagement metrics calculation
- MERGE operations for incremental updates
"""

import pytest
from pathlib import Path


# =============================================================================
# SQL DDL Functions
# =============================================================================


def create_events_table(exec_fn, test_run_id):
    """Create events table with full Snowplow schema (127 columns)"""
    exec_fn(f"""
        CREATE TABLE embucket.public.events_{test_run_id} (
            -- IMPORTANT: Column order MUST match CSV file order for COPY INTO to work correctly
            -- Core event identifiers
            app_id VARCHAR(255),
            platform VARCHAR(255),
            etl_tstamp TIMESTAMP,
            collector_tstamp TIMESTAMP,
            dvce_created_tstamp TIMESTAMP,
            event VARCHAR(128),
            event_id VARCHAR(36),
            txn_id INTEGER,
            name_tracker VARCHAR(128),
            v_tracker VARCHAR(100),
            v_collector VARCHAR(100),
            v_etl VARCHAR(100),

            -- User identifiers
            user_id VARCHAR(255),
            user_ipaddress VARCHAR(128),
            user_fingerprint VARCHAR(128),
            domain_userid VARCHAR(128),
            domain_sessionidx INTEGER,
            network_userid VARCHAR(128),

            -- Geo location
            geo_country VARCHAR(2),
            geo_region VARCHAR(3),
            geo_city VARCHAR(75),
            geo_zipcode VARCHAR(15),
            geo_latitude DOUBLE PRECISION,
            geo_longitude DOUBLE PRECISION,
            geo_region_name VARCHAR(100),

            -- IP information
            ip_isp VARCHAR(100),
            ip_organization VARCHAR(128),
            ip_domain VARCHAR(128),
            ip_netspeed VARCHAR(100),

            -- Page URL components
            page_url TEXT,
            page_title VARCHAR(2000),
            page_referrer TEXT,
            page_urlscheme VARCHAR(16),
            page_urlhost VARCHAR(255),
            page_urlport INTEGER,
            page_urlpath VARCHAR(3000),
            page_urlquery VARCHAR(6000),
            page_urlfragment VARCHAR(3000),

            -- Referrer URL components
            refr_urlscheme VARCHAR(16),
            refr_urlhost VARCHAR(255),
            refr_urlport INTEGER,
            refr_urlpath VARCHAR(6000),
            refr_urlquery VARCHAR(6000),
            refr_urlfragment VARCHAR(3000),
            refr_medium VARCHAR(25),
            refr_source VARCHAR(50),
            refr_term VARCHAR(255),

            -- Marketing parameters
            mkt_medium VARCHAR(255),
            mkt_source VARCHAR(255),
            mkt_term VARCHAR(255),
            mkt_content VARCHAR(500),
            mkt_campaign VARCHAR(255),

            -- Structured event fields
            se_category VARCHAR(1000),
            se_action VARCHAR(1000),
            se_label VARCHAR(4096),
            se_property VARCHAR(1000),
            se_value DOUBLE PRECISION,

            -- Ecommerce transaction
            tr_orderid VARCHAR(255),
            tr_affiliation VARCHAR(255),
            tr_total DOUBLE PRECISION,
            tr_tax DOUBLE PRECISION,
            tr_shipping DOUBLE PRECISION,
            tr_city VARCHAR(255),
            tr_state VARCHAR(255),
            tr_country VARCHAR(255),

            -- Ecommerce transaction item
            ti_orderid VARCHAR(255),
            ti_sku VARCHAR(255),
            ti_name VARCHAR(255),
            ti_category VARCHAR(255),
            ti_price DOUBLE PRECISION,
            ti_quantity INTEGER,

            -- Page ping
            pp_xoffset_min INTEGER,
            pp_xoffset_max INTEGER,
            pp_yoffset_min INTEGER,
            pp_yoffset_max INTEGER,

            -- Browser information
            useragent VARCHAR(1000),
            br_name VARCHAR(50),
            br_family VARCHAR(50),
            br_version VARCHAR(50),
            br_type VARCHAR(50),
            br_renderengine VARCHAR(50),
            br_lang VARCHAR(255),
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
            br_colordepth VARCHAR(12),
            br_viewwidth INTEGER,
            br_viewheight INTEGER,

            -- Operating system
            os_name VARCHAR(50),
            os_family VARCHAR(50),
            os_manufacturer VARCHAR(50),
            os_timezone VARCHAR(255),

            -- Device information
            dvce_type VARCHAR(50),
            dvce_ismobile BOOLEAN,
            dvce_screenwidth INTEGER,
            dvce_screenheight INTEGER,

            -- Document
            doc_charset VARCHAR(128),
            doc_width INTEGER,
            doc_height INTEGER,

            -- Ecommerce currency fields
            tr_currency VARCHAR(3),
            tr_total_base DOUBLE PRECISION,
            tr_tax_base DOUBLE PRECISION,
            tr_shipping_base DOUBLE PRECISION,
            ti_currency VARCHAR(3),
            ti_price_base DOUBLE PRECISION,

            -- Base currency
            base_currency VARCHAR(3),

            -- Geo timezone (different from os_timezone)
            geo_timezone VARCHAR(64),

            -- Marketing additional fields
            mkt_clickid VARCHAR(255),
            mkt_network VARCHAR(64),

            -- ETL
            etl_tags VARCHAR(500),

            -- Device sent timestamp
            dvce_sent_tstamp TIMESTAMP,

            -- Referrer additional fields
            refr_domain_userid VARCHAR(128),
            refr_dvce_tstamp TIMESTAMP,

            -- Session
            domain_sessionid VARCHAR(128),

            -- Derived timestamp
            derived_tstamp TIMESTAMP,

            -- Event schema
            event_vendor VARCHAR(1000),
            event_name VARCHAR(1000),
            event_format VARCHAR(128),
            event_version VARCHAR(20),
            event_fingerprint VARCHAR(128),

            -- True timestamp
            true_tstamp TIMESTAMP,

            -- Load timestamp
            load_tstamp TIMESTAMP,

            -- Context columns (JSON/TEXT) - order matters!
            contexts_com_snowplowanalytics_snowplow_web_page_1_0_0 TEXT,
            unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1_0_0 TEXT,
            unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1_0_0 TEXT,
            contexts_com_iab_snowplow_spiders_and_robots_1_0_0 TEXT,
            contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0 TEXT,
            contexts_nl_basjes_yauaa_context_1_0_0 TEXT
        );
    """)


def create_incremental_manifest(exec_fn, test_run_id):
    """Create table to track incremental processing state"""
    exec_fn(f"""
        CREATE TABLE embucket.public.snowplow_web_incremental_manifest_{test_run_id} (
            model TEXT,
            last_success TIMESTAMP
        );
    """)


def create_quarantined_sessions(exec_fn, test_run_id):
    """Create table to track sessions exceeding max_session_days"""
    exec_fn(f"""
        CREATE TABLE embucket.public.snowplow_web_base_quarantined_sessions_{test_run_id} (
            session_identifier TEXT
        );
    """)


def create_new_event_limits(exec_fn, test_run_id):
    """Create table to store time window for processing"""
    exec_fn(f"""
        CREATE TABLE embucket.public.snowplow_web_base_new_event_limits_{test_run_id} (
            lower_limit TIMESTAMP,
            upper_limit TIMESTAMP
        );
    """)


def create_sessions_lifecycle_manifest(exec_fn, test_run_id):
    """Create manifest to track session start/end across runs"""
    exec_fn(f"""
        CREATE TABLE embucket.public.snowplow_web_base_sessions_lifecycle_manifest_{test_run_id} (
            session_identifier TEXT,
            user_identifier TEXT,
            start_tstamp TIMESTAMP,
            end_tstamp TIMESTAMP
        );
    """)


def create_sessions_table(exec_fn, test_run_id):
    """Create final sessions output table (65 columns)"""
    exec_fn(f"""
        CREATE TABLE embucket.public.snowplow_web_sessions_{test_run_id} (
            -- Core identifiers
            app_id VARCHAR(255),
            platform VARCHAR(255),
            domain_sessionid VARCHAR(128) PRIMARY KEY,
            original_domain_sessionid VARCHAR(128),
            domain_sessionidx INTEGER,

            -- Timestamps
            start_tstamp TIMESTAMP,
            end_tstamp TIMESTAMP,

            -- User identifiers
            user_id VARCHAR(255),
            domain_userid VARCHAR(128),
            original_domain_userid VARCHAR(128),
            stitched_user_id VARCHAR(255),
            network_userid VARCHAR(128),

            -- Engagement metrics
            page_views INTEGER,
            engaged_time_in_s INTEGER,
            total_events INTEGER,
            is_engaged BOOLEAN,
            absolute_time_in_s INTEGER,

            -- First page attributes
            first_page_title VARCHAR(2000),
            first_page_url TEXT,
            first_page_urlscheme VARCHAR(16),
            first_page_urlhost VARCHAR(255),
            first_page_urlpath VARCHAR(3000),
            first_page_urlquery VARCHAR(6000),
            first_page_urlfragment VARCHAR(3000),

            -- Last page attributes
            last_page_title VARCHAR(2000),
            last_page_url TEXT,
            last_page_urlscheme VARCHAR(16),
            last_page_urlhost VARCHAR(255),
            last_page_urlpath VARCHAR(3000),
            last_page_urlquery VARCHAR(6000),
            last_page_urlfragment VARCHAR(3000),

            -- Referrer attributes
            referrer TEXT,
            refr_urlscheme VARCHAR(16),
            refr_urlhost VARCHAR(255),
            refr_urlpath VARCHAR(6000),
            refr_urlquery VARCHAR(6000),
            refr_urlfragment VARCHAR(3000),
            refr_medium VARCHAR(25),
            refr_source VARCHAR(50),
            refr_term VARCHAR(255),

            -- Marketing parameters
            mkt_medium VARCHAR(255),
            mkt_source VARCHAR(255),
            mkt_term VARCHAR(255),
            mkt_content VARCHAR(500),
            mkt_campaign VARCHAR(255),
            mkt_clickid VARCHAR(255),
            mkt_network VARCHAR(64),
            mkt_source_platform VARCHAR(255),
            default_channel_group VARCHAR(255),

            -- Geo attributes (first event)
            geo_country VARCHAR(2),
            geo_region VARCHAR(3),
            geo_region_name VARCHAR(100),
            geo_city VARCHAR(75),
            geo_zipcode VARCHAR(15),
            geo_latitude DOUBLE PRECISION,
            geo_longitude DOUBLE PRECISION,
            geo_timezone VARCHAR(64),

            -- Geo attributes (last event)
            last_geo_country VARCHAR(2),
            last_geo_region_name VARCHAR(100),
            last_geo_city VARCHAR(75),

            -- Device/Browser attributes
            user_ipaddress VARCHAR(128),
            useragent VARCHAR(1000),
            br_renderengine VARCHAR(50),
            br_lang VARCHAR(255),
            os_timezone VARCHAR(255),
            screen_resolution VARCHAR(50)
        );
    """)


# =============================================================================
# Data Loading Functions
# =============================================================================


def load_events_batch(exec_fn, test_run_id, batch_num):
    """Load a batch of events from CSV file"""
    # Get the absolute path to the test data
    test_file_path = f"""s3://embucket-testdata/dbt_snowplow_data/snowplow_web_events{batch_num}.csv"""

    exec_fn(f"""
        COPY INTO embucket.public.events_{test_run_id}
        FROM '{test_file_path}'
        FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
    """)


def load_expected_sessions(exec_fn, test_run_id):
    """Load expected sessions from CSV file for comparison"""
    # Create table for expected sessions
    exec_fn(f"""
        CREATE TABLE embucket.public.snowplow_web_sessions_expected_{test_run_id} (
            -- Core identifiers (1-5)
            app_id VARCHAR(255),
            platform VARCHAR(255),
            domain_sessionid VARCHAR(128) PRIMARY KEY,
            original_domain_sessionid VARCHAR(128),
            domain_sessionidx INTEGER,

            -- Timestamps (6-7)
            start_tstamp TIMESTAMP,
            end_tstamp TIMESTAMP,

            -- User identifiers (8-12)
            user_id VARCHAR(255),
            domain_userid VARCHAR(128),
            original_domain_userid VARCHAR(128),
            stitched_user_id VARCHAR(255),
            network_userid VARCHAR(128),

            -- Engagement metrics (13-18)
            page_views INTEGER,
            engaged_time_in_s INTEGER,
            event_counts VARCHAR(10000),
            total_events INTEGER,
            is_engaged BOOLEAN,
            absolute_time_in_s INTEGER,

            -- First page attributes (19-25)
            first_page_title VARCHAR(2000),
            first_page_url TEXT,
            first_page_urlscheme VARCHAR(16),
            first_page_urlhost VARCHAR(255),
            first_page_urlpath VARCHAR(3000),
            first_page_urlquery VARCHAR(6000),
            first_page_urlfragment VARCHAR(3000),

            -- Last page attributes (26-32)
            last_page_title VARCHAR(2000),
            last_page_url TEXT,
            last_page_urlscheme VARCHAR(16),
            last_page_urlhost VARCHAR(255),
            last_page_urlpath VARCHAR(3000),
            last_page_urlquery VARCHAR(6000),
            last_page_urlfragment VARCHAR(3000),

            -- Referrer attributes (33-41)
            referrer TEXT,
            refr_urlscheme VARCHAR(16),
            refr_urlhost VARCHAR(255),
            refr_urlpath VARCHAR(6000),
            refr_urlquery VARCHAR(6000),
            refr_urlfragment VARCHAR(3000),
            refr_medium VARCHAR(25),
            refr_source VARCHAR(50),
            refr_term VARCHAR(255),

            -- Marketing parameters (42-50)
            mkt_medium VARCHAR(255),
            mkt_source VARCHAR(255),
            mkt_term VARCHAR(255),
            mkt_content VARCHAR(500),
            mkt_campaign VARCHAR(255),
            mkt_clickid VARCHAR(255),
            mkt_network VARCHAR(64),
            mkt_source_platform VARCHAR(255),
            default_channel_group VARCHAR(255),

            -- Geo attributes (51-65)
            geo_country VARCHAR(2),
            geo_region VARCHAR(3),
            geo_region_name VARCHAR(100),
            geo_city VARCHAR(75),
            geo_zipcode VARCHAR(15),
            geo_latitude DOUBLE PRECISION,
            geo_longitude DOUBLE PRECISION,
            geo_timezone VARCHAR(64),
            geo_country_name VARCHAR(255),
            geo_continent VARCHAR(255),
            last_geo_country VARCHAR(2),
            last_geo_region_name VARCHAR(100),
            last_geo_city VARCHAR(75),
            last_geo_country_name VARCHAR(255),
            last_geo_continent VARCHAR(255),

            -- Device/Browser attributes (66-73)
            user_ipaddress VARCHAR(128),
            useragent VARCHAR(1000),
            br_renderengine VARCHAR(50),
            br_lang VARCHAR(255),
            br_lang_name VARCHAR(255),
            last_br_lang VARCHAR(255),
            last_br_lang_name VARCHAR(255),
            os_timezone VARCHAR(255),

            -- IAB enrichment (74-77)
            category VARCHAR(255),
            primary_impact VARCHAR(255),
            reason VARCHAR(255),
            spider_or_robot BOOLEAN,

            -- UA Parser enrichment (78-89)
            useragent_family VARCHAR(255),
            useragent_major VARCHAR(50),
            useragent_minor VARCHAR(50),
            useragent_patch VARCHAR(50),
            useragent_version VARCHAR(255),
            os_family VARCHAR(255),
            os_major VARCHAR(50),
            os_minor VARCHAR(50),
            os_patch VARCHAR(50),
            os_patch_minor VARCHAR(50),
            os_version VARCHAR(255),
            device_family VARCHAR(255),

            -- YAUAA enrichment (90-111)
            device_class VARCHAR(255),
            device_category VARCHAR(255),
            screen_resolution VARCHAR(50),
            agent_class VARCHAR(255),
            agent_name VARCHAR(255),
            agent_name_version VARCHAR(255),
            agent_name_version_major VARCHAR(255),
            agent_version VARCHAR(255),
            agent_version_major VARCHAR(255),
            device_brand VARCHAR(255),
            device_name VARCHAR(255),
            device_version VARCHAR(255),
            layout_engine_class VARCHAR(255),
            layout_engine_name VARCHAR(255),
            layout_engine_name_version VARCHAR(255),
            layout_engine_name_version_major VARCHAR(255),
            layout_engine_version VARCHAR(255),
            layout_engine_version_major VARCHAR(255),
            operating_system_class VARCHAR(255),
            operating_system_name VARCHAR(255),
            operating_system_name_version VARCHAR(255),
            operating_system_version VARCHAR(255),

            -- Conversion metrics (112-119)
            cv_view_page_volume INTEGER,
            cv_view_page_events VARCHAR(10000),
            cv_view_page_values VARCHAR(10000),
            cv_view_page_total DOUBLE PRECISION,
            cv_view_page_first_conversion TIMESTAMP,
            cv_view_page_converted BOOLEAN,
            cv__all_volume INTEGER,
            cv__all_total DOUBLE PRECISION,

            -- Passthrough fields (120-121)
            event_id VARCHAR(36),
            event_id2 VARCHAR(36)
        );
    """)

    # Get the absolute path to the expected data
    expected_csv_path = (
        "s3://embucket-testdata/dbt_snowplow_data/snowplow_web_sessions_expected.csv"
    )
    # Load CSV data
    exec_fn(f"""
        COPY INTO embucket.public.snowplow_web_sessions_expected_{test_run_id}
        FROM '{expected_csv_path}'
        FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
    """)


# =============================================================================
# Processing Functions
# =============================================================================


def populate_new_event_limits(exec_fn, test_run_id):
    """Calculate time window for processing (with lookback for incremental runs)"""
    exec_fn(f"""
        CREATE OR REPLACE TABLE embucket.public.snowplow_web_base_new_event_limits_{test_run_id} AS
        SELECT
            CASE
                -- First run: use earliest event timestamp
                WHEN (SELECT COUNT(*) FROM embucket.public.snowplow_web_incremental_manifest_{test_run_id}
                      WHERE model = 'snowplow_web_base_sessions_lifecycle_manifest') = 0
                THEN (SELECT MIN(collector_tstamp) FROM embucket.public.events_{test_run_id})
                -- Incremental run: use last_success - 6 hours lookback
                ELSE DATEADD(hour, -6,
                    (SELECT last_success FROM embucket.public.snowplow_web_incremental_manifest_{test_run_id}
                     WHERE model = 'snowplow_web_base_sessions_lifecycle_manifest'))
            END as lower_limit,
            -- upper_limit is always the latest event timestamp
            (SELECT MAX(collector_tstamp) FROM embucket.public.events_{test_run_id}) as upper_limit;
    """)


def merge_sessions_lifecycle_manifest(exec_fn, test_run_id):
    """MERGE sessions into lifecycle manifest (extend existing, add new)"""
    exec_fn(f"""
        MERGE INTO embucket.public.snowplow_web_base_sessions_lifecycle_manifest_{test_run_id} AS target
        USING (
            SELECT
                COALESCE(e.domain_sessionid, NULL) as session_identifier,
                MAX(COALESCE(e.domain_userid, NULL)) as user_identifier,
                MIN(e.collector_tstamp) as start_tstamp,
                MAX(e.collector_tstamp) as end_tstamp
            FROM embucket.public.events_{test_run_id} e
            CROSS JOIN embucket.public.snowplow_web_base_new_event_limits_{test_run_id} limits
            WHERE e.domain_sessionid IS NOT NULL
                -- Time window filter: only process events within the calculated window
                AND e.collector_tstamp >= limits.lower_limit
                AND e.collector_tstamp <= limits.upper_limit
                -- Exclude quarantined sessions
                AND NOT EXISTS (
                    SELECT 1 FROM embucket.public.snowplow_web_base_quarantined_sessions_{test_run_id} q
                    WHERE q.session_identifier = e.domain_sessionid
                )
            GROUP BY e.domain_sessionid
        ) AS source
        ON target.session_identifier = source.session_identifier
        -- Only update sessions that haven't exceeded max_session_days (3 days)
        WHEN MATCHED AND target.end_tstamp < DATEADD(day, 3, target.start_tstamp) THEN
            UPDATE SET
                -- Keep existing user_identifier if available, otherwise use new one
                user_identifier = COALESCE(target.user_identifier, source.user_identifier),
                -- Extend session backwards if late events arrive with earlier timestamps
                start_tstamp = LEAST(target.start_tstamp, source.start_tstamp),
                -- Extend session forwards with new events, but cap at max_session_days
                end_tstamp = LEAST(
                    DATEADD(day, 3, target.start_tstamp),
                    GREATEST(target.end_tstamp, source.end_tstamp)
                )
        WHEN NOT MATCHED THEN
            INSERT (session_identifier, user_identifier, start_tstamp, end_tstamp)
            VALUES (
                source.session_identifier,
                source.user_identifier,
                source.start_tstamp,
                -- Cap new sessions at max_session_days from the start
                LEAST(source.end_tstamp, DATEADD(day, 3, source.start_tstamp))
            );
    """)


def update_incremental_manifest(exec_fn, test_run_id):
    """Update manifest with latest processing timestamp"""
    exec_fn(f"""
        MERGE INTO embucket.public.snowplow_web_incremental_manifest_{test_run_id} AS target
        USING (
            SELECT
                'snowplow_web_base_sessions_lifecycle_manifest' as model,
                (SELECT upper_limit FROM embucket.public.snowplow_web_base_new_event_limits_{test_run_id}) as last_success
        ) AS source
        ON target.model = source.model
        WHEN MATCHED THEN
            UPDATE SET last_success = source.last_success
        WHEN NOT MATCHED THEN
            INSERT (model, last_success)
            VALUES (source.model, source.last_success);
    """)


def detect_quarantined_sessions(exec_fn, test_run_id):
    """Find and quarantine sessions exceeding 3 days"""
    exec_fn(f"""
        MERGE INTO embucket.public.snowplow_web_base_quarantined_sessions_{test_run_id} AS target
        USING (
            -- Find sessions that have hit the max_session_days limit (3 days)
            SELECT session_identifier
            FROM embucket.public.snowplow_web_base_sessions_lifecycle_manifest_{test_run_id}
            WHERE end_tstamp >= DATEADD(day, 3, start_tstamp)
        ) AS source
        ON target.session_identifier = source.session_identifier
        WHEN NOT MATCHED THEN
            INSERT (session_identifier)
            VALUES (source.session_identifier);
    """)


def create_sessions_this_run(exec_fn, test_run_id):
    """Select sessions to process in this run"""
    exec_fn(f"""
        CREATE OR REPLACE TABLE embucket.public.snowplow_web_base_sessions_this_run_{test_run_id} AS
        SELECT
            s.session_identifier,
            s.user_identifier,
            s.start_tstamp,
            -- Cap end_tstamp to upper_limit when backfilling to avoid processing future events
            CASE
                WHEN s.end_tstamp > limits.upper_limit
                THEN limits.upper_limit
                ELSE s.end_tstamp
            END as end_tstamp
        FROM embucket.public.snowplow_web_base_sessions_lifecycle_manifest_{test_run_id} s
        CROSS JOIN embucket.public.snowplow_web_base_new_event_limits_{test_run_id} limits
        WHERE
            -- General window: session start must be after (lower_limit - max_session_days)
            -- This captures sessions that started before the window but may extend into it
            s.start_tstamp >= DATEADD(day, -3, limits.lower_limit)
            AND s.start_tstamp <= limits.upper_limit
            -- Exclude sessions completely outside the window
            -- (either start after upper_limit OR end before lower_limit)
            AND NOT (
                s.start_tstamp > limits.upper_limit
                OR s.end_tstamp < limits.lower_limit
            );
    """)


def create_events_this_run(exec_fn, test_run_id):
    """Filter and prepare events for sessions being processed"""
    exec_fn(f"""
        CREATE OR REPLACE TABLE embucket.public.snowplow_web_base_events_this_run_{test_run_id} AS
        WITH base_query AS (
            WITH identified_events AS (
                SELECT
                    COALESCE(e.domain_sessionid, NULL) as session_identifier,
                    -- Extract page_view_id from JSON context
                    -- NULLIF converts string 'null' to SQL NULL
                    NULLIF(TRY_PARSE_JSON(e.contexts_com_snowplowanalytics_snowplow_web_page_1_0_0)[0]:id::VARCHAR, 'null') as page_view_id,
                    e.*
                FROM embucket.public.events_{test_run_id} e
            )
            SELECT
                a.*,
                b.user_identifier,
                b.start_tstamp as session_start_tstamp
            FROM identified_events a
            INNER JOIN embucket.public.snowplow_web_base_sessions_this_run_{test_run_id} b
                ON a.session_identifier = b.session_identifier
            CROSS JOIN embucket.public.snowplow_web_base_new_event_limits_{test_run_id} limits
            WHERE
                -- Filter to page_view and page_ping events with page_view_id
                a.event IN ('page_view', 'page_ping')
                AND a.page_view_id IS NOT NULL
                -- Cap session at max_session_days (3 days) from start
                AND a.collector_tstamp <= DATEADD(day, 3, b.start_tstamp)
                -- Late event filtering: device sent time within 3 days of creation
                AND a.dvce_sent_tstamp <= DATEADD(day, 3, a.dvce_created_tstamp)
                -- Time window filters
                AND a.collector_tstamp >= limits.lower_limit
                AND a.collector_tstamp <= limits.upper_limit
                AND a.collector_tstamp >= b.start_tstamp
            -- Deduplicate by event_id (keep first occurrence)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY a.event_id
                ORDER BY a.collector_tstamp, a.dvce_created_tstamp
            ) = 1
        )
        SELECT
            a.page_view_id,
            a.session_identifier as domain_sessionid,
            a.domain_sessionid as original_domain_sessionid,
            a.user_identifier as domain_userid,
            a.domain_userid as original_domain_userid,
            a.session_start_tstamp,
            a.* EXCLUDE(
                page_view_id,
                session_identifier,
                domain_sessionid,
                domain_userid,
                user_identifier,
                session_start_tstamp,
                contexts_com_snowplowanalytics_snowplow_web_page_1_0_0
            )
        FROM base_query a;
    """)


def build_sessions_from_events(exec_fn, test_run_id):
    """Aggregate events into session records (core sessionization logic)"""
    exec_fn(f"""
        CREATE OR REPLACE TABLE embucket.public.snowplow_web_sessions_this_run_{test_run_id} AS
        WITH session_firsts AS (
            -- Get first event attributes for each session
            SELECT
                domain_sessionid,
                event,
                app_id,
                platform,
                domain_sessionidx,
                user_id,
                domain_userid,
                network_userid,

                -- First page attributes
                page_title AS first_page_title,
                page_url AS first_page_url,
                page_urlscheme AS first_page_urlscheme,
                page_urlhost AS first_page_urlhost,
                page_urlpath AS first_page_urlpath,
                page_urlquery AS first_page_urlquery,
                page_urlfragment AS first_page_urlfragment,

                -- Referrer attributes (from first event)
                page_referrer AS referrer,
                refr_urlscheme,
                refr_urlhost,
                refr_urlpath,
                refr_urlquery,
                refr_urlfragment,
                refr_medium,
                refr_source,
                refr_term,

                -- Marketing parameters (from first event)
                mkt_medium,
                mkt_source,
                mkt_term,
                mkt_content,
                mkt_campaign,
                mkt_clickid,
                mkt_network,

                -- Extract mkt_source_platform from URL query
                REGEXP_SUBSTR(page_urlquery, 'utm_source_platform=([^?&#]*)', 1, 1, 'e', 1) AS mkt_source_platform,

                -- Geo attributes (first event)
                geo_country,
                geo_region,
                geo_region_name,
                geo_city,
                geo_zipcode,
                geo_latitude,
                geo_longitude,
                geo_timezone,

                -- Device/Browser attributes (first event)
                user_ipaddress,
                useragent,
                br_renderengine,
                br_lang,
                os_timezone,
                CONCAT(COALESCE(dvce_screenwidth, ''), 'x', COALESCE(dvce_screenheight, '')) AS screen_resolution
            FROM embucket.public.snowplow_web_base_events_this_run_{test_run_id}
            WHERE domain_sessionid IS NOT NULL
                -- Bot filtering: exclude known bot/crawler user agents
                AND NOT RLIKE(LOWER(useragent), '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|pingdombot|phantomjs|yandexbot|twitterbot|a_archiver|facebookexternalhit|bingbot|bingpreview|googlebot|baiduspider|360(spider|user-agent)|semalt).*')
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY domain_sessionid
                ORDER BY collector_tstamp, dvce_created_tstamp, event_id
            ) = 1
        ),
        session_lasts AS (
            -- Get last page view attributes for each session
            SELECT
                domain_sessionid,

                -- Last page attributes
                page_title AS last_page_title,
                page_url AS last_page_url,
                page_urlscheme AS last_page_urlscheme,
                page_urlhost AS last_page_urlhost,
                page_urlpath AS last_page_urlpath,
                page_urlquery AS last_page_urlquery,
                page_urlfragment AS last_page_urlfragment,

                -- Last geo attributes
                geo_country AS last_geo_country,
                geo_region_name AS last_geo_region_name,
                geo_city AS last_geo_city
            FROM embucket.public.snowplow_web_base_events_this_run_{test_run_id}
            WHERE domain_sessionid IS NOT NULL
                AND event = 'page_view'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY domain_sessionid
                ORDER BY collector_tstamp DESC, dvce_created_tstamp DESC, event_id DESC
            ) = 1
        ),
        session_aggs AS (
            -- Aggregate metrics for each session
            SELECT
                domain_sessionid,
                MIN(collector_tstamp) AS start_tstamp,
                MAX(collector_tstamp) AS end_tstamp,
                COUNT(*) AS total_events,
                -- Count page views by page_view_id
                COUNT(DISTINCT CASE WHEN event = 'page_view' THEN page_view_id END) AS page_views,
                TIMESTAMPDIFF(SECOND, MIN(collector_tstamp), MAX(collector_tstamp)) AS absolute_time_in_s,
                -- Calculate engaged time from page_ping heartbeats
                -- Formula: 10 * (unique_10s_buckets - unique_pages) + (unique_pages * 5)
                -- This calculates engaged time based on 10-second page_ping intervals
                (10 * (
                    COUNT(DISTINCT CASE
                        WHEN event = 'page_ping' AND page_view_id IS NOT NULL
                        THEN page_view_id || CAST(FLOOR(DATE_PART('epoch_second', dvce_created_tstamp) / 10) AS TEXT)
                        ELSE NULL
                    END) -
                    COUNT(DISTINCT CASE
                        WHEN event = 'page_ping' AND page_view_id IS NOT NULL
                        THEN page_view_id
                        ELSE NULL
                    END)
                )) +
                (COUNT(DISTINCT CASE
                    WHEN event = 'page_ping' AND page_view_id IS NOT NULL
                    THEN page_view_id
                    ELSE NULL
                END) * 5) AS engaged_time_in_s
            FROM embucket.public.snowplow_web_base_events_this_run_{test_run_id}
            WHERE domain_sessionid IS NOT NULL
            GROUP BY domain_sessionid
        )
        SELECT
            -- Core identifiers
            f.app_id,
            f.platform,
            f.domain_sessionid,
            f.domain_sessionid AS original_domain_sessionid,
            f.domain_sessionidx,

            -- Timestamps
            -- Adjust start timestamp: subtract 5 seconds when first event is page_ping (min visit length)
            CASE
                WHEN f.event = 'page_ping' THEN DATEADD(second, -5, a.start_tstamp)
                ELSE a.start_tstamp
            END AS start_tstamp,
            a.end_tstamp,

            -- User identifiers
            f.user_id,
            f.domain_userid,
            f.domain_userid AS original_domain_userid,
            f.user_id AS stitched_user_id,
            f.network_userid,

            -- Engagement metrics
            a.page_views,
            a.engaged_time_in_s,
            a.total_events,
            -- is_engaged: 2+ page views OR 10+ seconds engaged time
            (a.page_views >= 2 OR a.engaged_time_in_s >= 10) AS is_engaged,
            a.absolute_time_in_s,

            -- First page attributes
            f.first_page_title,
            f.first_page_url,
            f.first_page_urlscheme,
            f.first_page_urlhost,
            f.first_page_urlpath,
            f.first_page_urlquery,
            f.first_page_urlfragment,

            -- Last page attributes (fallback to first if no last)
            COALESCE(l.last_page_title, f.first_page_title) AS last_page_title,
            COALESCE(l.last_page_url, f.first_page_url) AS last_page_url,
            COALESCE(l.last_page_urlscheme, f.first_page_urlscheme) AS last_page_urlscheme,
            COALESCE(l.last_page_urlhost, f.first_page_urlhost) AS last_page_urlhost,
            COALESCE(l.last_page_urlpath, f.first_page_urlpath) AS last_page_urlpath,
            COALESCE(l.last_page_urlquery, f.first_page_urlquery) AS last_page_urlquery,
            COALESCE(l.last_page_urlfragment, f.first_page_urlfragment) AS last_page_urlfragment,

            -- Referrer attributes
            f.referrer,
            f.refr_urlscheme,
            f.refr_urlhost,
            f.refr_urlpath,
            f.refr_urlquery,
            f.refr_urlfragment,
            f.refr_medium,
            f.refr_source,
            f.refr_term,

            -- Marketing parameters
            f.mkt_medium,
            f.mkt_source,
            f.mkt_term,
            f.mkt_content,
            f.mkt_campaign,
            f.mkt_clickid,
            f.mkt_network,
            f.mkt_source_platform,
            'Unassigned' AS default_channel_group,  -- Simplified: requires complex GA4 logic

            -- Geo attributes (first event)
            f.geo_country,
            f.geo_region,
            f.geo_region_name,
            f.geo_city,
            f.geo_zipcode,
            f.geo_latitude,
            f.geo_longitude,
            f.geo_timezone,

            -- Geo attributes (last event)
            COALESCE(l.last_geo_country, f.geo_country) AS last_geo_country,
            COALESCE(l.last_geo_region_name, f.geo_region_name) AS last_geo_region_name,
            COALESCE(l.last_geo_city, f.geo_city) AS last_geo_city,

            -- Device/Browser attributes
            f.user_ipaddress,
            f.useragent,
            f.br_renderengine,
            f.br_lang,
            f.os_timezone,
            f.screen_resolution
        FROM session_firsts f
        LEFT JOIN session_lasts l ON f.domain_sessionid = l.domain_sessionid
        LEFT JOIN session_aggs a ON f.domain_sessionid = a.domain_sessionid;
    """)


def merge_sessions_into_final_table(exec_fn, test_run_id):
    """MERGE sessions_this_run into final sessions table"""
    exec_fn(f"""
        MERGE INTO embucket.public.snowplow_web_sessions_{test_run_id} AS target
        USING embucket.public.snowplow_web_sessions_this_run_{test_run_id} AS source
        ON target.domain_sessionid = source.domain_sessionid
        WHEN MATCHED THEN
            UPDATE SET
                app_id = source.app_id,
                platform = source.platform,
                original_domain_sessionid = source.original_domain_sessionid,
                domain_sessionidx = source.domain_sessionidx,
                start_tstamp = source.start_tstamp,
                end_tstamp = source.end_tstamp,
                user_id = source.user_id,
                domain_userid = source.domain_userid,
                original_domain_userid = source.original_domain_userid,
                stitched_user_id = source.stitched_user_id,
                network_userid = source.network_userid,
                page_views = source.page_views,
                engaged_time_in_s = source.engaged_time_in_s,
                total_events = source.total_events,
                is_engaged = source.is_engaged,
                absolute_time_in_s = source.absolute_time_in_s,
                first_page_title = source.first_page_title,
                first_page_url = source.first_page_url,
                first_page_urlscheme = source.first_page_urlscheme,
                first_page_urlhost = source.first_page_urlhost,
                first_page_urlpath = source.first_page_urlpath,
                first_page_urlquery = source.first_page_urlquery,
                first_page_urlfragment = source.first_page_urlfragment,
                last_page_title = source.last_page_title,
                last_page_url = source.last_page_url,
                last_page_urlscheme = source.last_page_urlscheme,
                last_page_urlhost = source.last_page_urlhost,
                last_page_urlpath = source.last_page_urlpath,
                last_page_urlquery = source.last_page_urlquery,
                last_page_urlfragment = source.last_page_urlfragment,
                referrer = source.referrer,
                refr_urlscheme = source.refr_urlscheme,
                refr_urlhost = source.refr_urlhost,
                refr_urlpath = source.refr_urlpath,
                refr_urlquery = source.refr_urlquery,
                refr_urlfragment = source.refr_urlfragment,
                refr_medium = source.refr_medium,
                refr_source = source.refr_source,
                refr_term = source.refr_term,
                mkt_medium = source.mkt_medium,
                mkt_source = source.mkt_source,
                mkt_term = source.mkt_term,
                mkt_content = source.mkt_content,
                mkt_campaign = source.mkt_campaign,
                mkt_clickid = source.mkt_clickid,
                mkt_network = source.mkt_network,
                mkt_source_platform = source.mkt_source_platform,
                default_channel_group = source.default_channel_group,
                geo_country = source.geo_country,
                geo_region = source.geo_region,
                geo_region_name = source.geo_region_name,
                geo_city = source.geo_city,
                geo_zipcode = source.geo_zipcode,
                geo_latitude = source.geo_latitude,
                geo_longitude = source.geo_longitude,
                geo_timezone = source.geo_timezone,
                last_geo_country = source.last_geo_country,
                last_geo_region_name = source.last_geo_region_name,
                last_geo_city = source.last_geo_city,
                user_ipaddress = source.user_ipaddress,
                useragent = source.useragent,
                br_renderengine = source.br_renderengine,
                br_lang = source.br_lang,
                os_timezone = source.os_timezone,
                screen_resolution = source.screen_resolution
        WHEN NOT MATCHED THEN
            INSERT (
                app_id, platform, domain_sessionid, original_domain_sessionid, domain_sessionidx,
                start_tstamp, end_tstamp,
                user_id, domain_userid, original_domain_userid, stitched_user_id, network_userid,
                page_views, engaged_time_in_s, total_events, is_engaged, absolute_time_in_s,
                first_page_title, first_page_url, first_page_urlscheme, first_page_urlhost,
                first_page_urlpath, first_page_urlquery, first_page_urlfragment,
                last_page_title, last_page_url, last_page_urlscheme, last_page_urlhost,
                last_page_urlpath, last_page_urlquery, last_page_urlfragment,
                referrer, refr_urlscheme, refr_urlhost, refr_urlpath, refr_urlquery, refr_urlfragment,
                refr_medium, refr_source, refr_term,
                mkt_medium, mkt_source, mkt_term, mkt_content, mkt_campaign, mkt_clickid, mkt_network,
                mkt_source_platform, default_channel_group,
                geo_country, geo_region, geo_region_name, geo_city, geo_zipcode,
                geo_latitude, geo_longitude, geo_timezone,
                last_geo_country, last_geo_region_name, last_geo_city,
                user_ipaddress, useragent, br_renderengine, br_lang, os_timezone, screen_resolution
            )
            VALUES (
                source.app_id, source.platform, source.domain_sessionid, source.original_domain_sessionid, source.domain_sessionidx,
                source.start_tstamp, source.end_tstamp,
                source.user_id, source.domain_userid, source.original_domain_userid, source.stitched_user_id, source.network_userid,
                source.page_views, source.engaged_time_in_s, source.total_events, source.is_engaged, source.absolute_time_in_s,
                source.first_page_title, source.first_page_url, source.first_page_urlscheme, source.first_page_urlhost,
                source.first_page_urlpath, source.first_page_urlquery, source.first_page_urlfragment,
                source.last_page_title, source.last_page_url, source.last_page_urlscheme, source.last_page_urlhost,
                source.last_page_urlpath, source.last_page_urlquery, source.last_page_urlfragment,
                source.referrer, source.refr_urlscheme, source.refr_urlhost, source.refr_urlpath, source.refr_urlquery, source.refr_urlfragment,
                source.refr_medium, source.refr_source, source.refr_term,
                source.mkt_medium, source.mkt_source, source.mkt_term, source.mkt_content, source.mkt_campaign, source.mkt_clickid, source.mkt_network,
                source.mkt_source_platform, source.default_channel_group,
                source.geo_country, source.geo_region, source.geo_region_name, source.geo_city, source.geo_zipcode,
                source.geo_latitude, source.geo_longitude, source.geo_timezone,
                source.last_geo_country, source.last_geo_region_name, source.last_geo_city,
                source.user_ipaddress, source.useragent, source.br_renderengine, source.br_lang, source.os_timezone, source.screen_resolution
            );
    """)


# =============================================================================
# Test Function
# =============================================================================


def test_snowplow_incremental_sessionization(embucket_exec, test_run_id):
    """
    Test Snowplow web analytics sessionization with incremental batch processing.

    This test validates:
    - Incremental event loading in 2 batches
    - Session lifecycle management with 6-hour lookback
    - Session extension across batches
    - Quarantining of long-running sessions (3+ days)
    - Bot filtering and late event handling
    - Engagement metrics calculation
    - Final output matches expected results
    """

    print("\n" + "=" * 80)
    print("SNOWPLOW INCREMENTAL SESSIONIZATION TEST")
    print("=" * 80)

    # =========================================================================
    # SETUP: Create all required tables
    # =========================================================================
    print("\n--- Setup: Creating tables ---")
    create_events_table(embucket_exec, test_run_id)
    create_incremental_manifest(embucket_exec, test_run_id)
    create_quarantined_sessions(embucket_exec, test_run_id)
    create_new_event_limits(embucket_exec, test_run_id)
    create_sessions_lifecycle_manifest(embucket_exec, test_run_id)
    create_sessions_table(embucket_exec, test_run_id)
    print("✓ All tables created")

    # =========================================================================
    # BATCH 1: Initial Load
    # =========================================================================
    print("\n" + "=" * 80)
    print("BATCH 1: Initial Load")
    print("=" * 80)

    print("\n--- Loading first batch of events ---")
    load_events_batch(embucket_exec, test_run_id, 1)
    result = embucket_exec(
        f"SELECT COUNT(*) as event_count FROM embucket.public.events_{test_run_id}"
    )
    print(f"✓ Event count: {result[0][0]}")

    print("\n--- Calculating time window (first run) ---")
    populate_new_event_limits(embucket_exec, test_run_id)
    result = embucket_exec(
        f"SELECT lower_limit, upper_limit FROM embucket.public.snowplow_web_base_new_event_limits_{test_run_id}"
    )
    print(f"✓ Time window: {result[0][0]} to {result[0][1]}")

    print("\n--- Merging sessions into lifecycle manifest ---")
    merge_sessions_lifecycle_manifest(embucket_exec, test_run_id)
    result = embucket_exec(
        f"SELECT COUNT(*) as session_count FROM embucket.public.snowplow_web_base_sessions_lifecycle_manifest_{test_run_id}"
    )
    print(f"✓ Sessions in lifecycle manifest: {result[0][0]}")

    print("\n--- Detecting quarantined sessions ---")
    detect_quarantined_sessions(embucket_exec, test_run_id)
    result = embucket_exec(
        f"SELECT COUNT(*) as quarantined_count FROM embucket.public.snowplow_web_base_quarantined_sessions_{test_run_id}"
    )
    print(f"✓ Quarantined sessions: {result[0][0]}")

    print("\n--- Updating incremental manifest ---")
    update_incremental_manifest(embucket_exec, test_run_id)
    result = embucket_exec(
        f"SELECT model, last_success FROM embucket.public.snowplow_web_incremental_manifest_{test_run_id}"
    )
    print(f"✓ Last success: {result[0][1]}")

    print("\n--- Selecting sessions to process ---")
    create_sessions_this_run(embucket_exec, test_run_id)
    result = embucket_exec(
        f"SELECT COUNT(*) as count FROM embucket.public.snowplow_web_base_sessions_this_run_{test_run_id}"
    )
    print(f"✓ Sessions this run: {result[0][0]}")

    print("\n--- Filtering events for sessions ---")
    create_events_this_run(embucket_exec, test_run_id)
    result = embucket_exec(
        f"SELECT COUNT(*) as count FROM embucket.public.snowplow_web_base_events_this_run_{test_run_id}"
    )
    print(f"✓ Events this run: {result[0][0]}")

    print("\n--- Aggregating events into session records ---")
    build_sessions_from_events(embucket_exec, test_run_id)
    result = embucket_exec(
        f"SELECT COUNT(*) as count FROM embucket.public.snowplow_web_sessions_this_run_{test_run_id}"
    )
    print(f"✓ Sessions built: {result[0][0]}")

    print("\n--- Merging sessions into final table ---")
    merge_sessions_into_final_table(embucket_exec, test_run_id)
    result = embucket_exec(
        f"SELECT COUNT(*) as session_count FROM embucket.public.snowplow_web_sessions_{test_run_id}"
    )
    print(f"✓ Total sessions in final table: {result[0][0]}")

    # =========================================================================
    # BATCH 2: Incremental Update
    # =========================================================================
    print("\n" + "=" * 80)
    print("BATCH 2: Incremental Update")
    print("=" * 80)

    print("\n--- Loading second batch of events ---")
    load_events_batch(embucket_exec, test_run_id, 2)
    result = embucket_exec(
        f"SELECT COUNT(*) as event_count FROM embucket.public.events_{test_run_id}"
    )
    print(f"✓ Total event count: {result[0][0]}")

    print("\n--- Recalculating time window (with 6-hour lookback) ---")
    populate_new_event_limits(embucket_exec, test_run_id)
    result = embucket_exec(
        f"SELECT lower_limit, upper_limit FROM embucket.public.snowplow_web_base_new_event_limits_{test_run_id}"
    )
    print(f"✓ Time window: {result[0][0]} to {result[0][1]}")

    print("\n--- Incremental MERGE: extending existing sessions + adding new ---")
    merge_sessions_lifecycle_manifest(embucket_exec, test_run_id)
    result = embucket_exec(
        f"SELECT COUNT(*) as session_count FROM embucket.public.snowplow_web_base_sessions_lifecycle_manifest_{test_run_id}"
    )
    print(f"✓ Sessions in lifecycle manifest: {result[0][0]}")

    print("\n--- Detecting quarantined sessions ---")
    detect_quarantined_sessions(embucket_exec, test_run_id)
    result = embucket_exec(
        f"SELECT COUNT(*) as quarantined_count FROM embucket.public.snowplow_web_base_quarantined_sessions_{test_run_id}"
    )
    print(f"✓ Quarantined sessions: {result[0][0]}")

    print("\n--- Updating incremental manifest ---")
    update_incremental_manifest(embucket_exec, test_run_id)
    result = embucket_exec(
        f"SELECT model, last_success FROM embucket.public.snowplow_web_incremental_manifest_{test_run_id}"
    )
    print(f"✓ Last success: {result[0][1]}")

    print("\n--- Selecting sessions to process (incremental) ---")
    create_sessions_this_run(embucket_exec, test_run_id)
    result = embucket_exec(
        f"SELECT COUNT(*) as count FROM embucket.public.snowplow_web_base_sessions_this_run_{test_run_id}"
    )
    print(f"✓ Sessions this run: {result[0][0]}")

    print("\n--- Filtering events for sessions (incremental) ---")
    create_events_this_run(embucket_exec, test_run_id)
    result = embucket_exec(
        f"SELECT COUNT(*) as count FROM embucket.public.snowplow_web_base_events_this_run_{test_run_id}"
    )
    print(f"✓ Events this run: {result[0][0]}")

    print("\n--- Aggregating events into session records (incremental) ---")
    build_sessions_from_events(embucket_exec, test_run_id)
    result = embucket_exec(
        f"SELECT COUNT(*) as count FROM embucket.public.snowplow_web_sessions_this_run_{test_run_id}"
    )
    print(f"✓ Sessions built: {result[0][0]}")

    print("\n--- Merging sessions into final table (incremental) ---")
    merge_sessions_into_final_table(embucket_exec, test_run_id)
    result = embucket_exec(
        f"SELECT COUNT(*) as session_count FROM embucket.public.snowplow_web_sessions_{test_run_id}"
    )
    print(f"✓ Total sessions in final table: {result[0][0]}")

    # =========================================================================
    # FINAL VERIFICATION
    # =========================================================================
    print("\n" + "=" * 80)
    print("FINAL VERIFICATION")
    print("=" * 80)

    # Load expected sessions from CSV into database
    print("\n--- Loading expected sessions from CSV ---")
    load_expected_sessions(embucket_exec, test_run_id)

    # Get session counts
    print("\n--- Comparing session counts ---")
    computed_count = embucket_exec(f"""
        SELECT COUNT(*) as count
        FROM embucket.public.snowplow_web_sessions_{test_run_id}
    """)[0][0]

    expected_count = embucket_exec(f"""
        SELECT COUNT(*) as count
        FROM embucket.public.snowplow_web_sessions_expected_{test_run_id}
    """)[0][0]

    print(f"✓ Computed: {computed_count} sessions")
    print(f"✓ Expected: {expected_count} sessions")

    assert computed_count == expected_count, (
        f"Session count mismatch: computed={computed_count}, expected={expected_count}"
    )
    print(f"✓ Session counts match: {computed_count}")

    # Compare session IDs using SQL EXCEPT
    print("\n--- Comparing session identifiers ---")

    # Find sessions only in computed (unexpected sessions)
    only_in_computed = embucket_exec(f"""
        SELECT domain_sessionid
        FROM embucket.public.snowplow_web_sessions_{test_run_id}
        EXCEPT
        SELECT domain_sessionid
        FROM embucket.public.snowplow_web_sessions_expected_{test_run_id}
    """)

    # Find sessions only in expected (missing sessions)
    only_in_expected = embucket_exec(f"""
        SELECT domain_sessionid
        FROM embucket.public.snowplow_web_sessions_expected_{test_run_id}
        EXCEPT
        SELECT domain_sessionid
        FROM embucket.public.snowplow_web_sessions_{test_run_id}
    """)

    if only_in_computed:
        session_ids = [row[0] for row in only_in_computed]
        print(f"⚠ Sessions only in computed: {session_ids}")
    if only_in_expected:
        session_ids = [row[0] for row in only_in_expected]
        print(f"⚠ Sessions only in expected: {session_ids}")

    assert len(only_in_computed) == 0, (
        f"Unexpected sessions in computed: {[row[0] for row in only_in_computed]}"
    )
    assert len(only_in_expected) == 0, (
        f"Missing sessions in computed: {[row[0] for row in only_in_expected]}"
    )
    print(f"✓ All session IDs match")

    print("\n" + "=" * 80)
    print("TEST PASSED: Snowplow sessionization working correctly!")
    print("=" * 80)
