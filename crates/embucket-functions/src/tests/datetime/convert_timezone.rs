use crate::test_query;

// Basic timezone conversion tests
test_query!(
    convert_timezone_basic_utc_to_est,
    r#"SELECT
        '2024-01-15T12:00:00Z'::TIMESTAMP AS utc_time,
        CONVERT_TIMEZONE('America/New_York', '2024-01-15T12:00:00Z'::TIMESTAMP) AS est_time"#,
    snapshot_path = "convert_timezone"
);

test_query!(
    convert_timezone_basic_utc_to_pst,
    r#"SELECT
        '2024-01-15T12:00:00Z'::TIMESTAMP AS utc_time,
        CONVERT_TIMEZONE('America/Los_Angeles', '2024-01-15T12:00:00Z'::TIMESTAMP) AS pst_time"#,
    snapshot_path = "convert_timezone"
);

// Test with explicit source timezone (3-argument form)
test_query!(
    convert_timezone_explicit_source,
    r#"SELECT
        '2024-01-15T12:00:00'::TIMESTAMP AS source_time,
        CONVERT_TIMEZONE('America/New_York', 'America/Los_Angeles', '2024-01-15T12:00:00'::TIMESTAMP) AS converted_time"#,
    snapshot_path = "convert_timezone"
);

test_query!(
    convert_timezone_utc_to_multiple_zones,
    r#"SELECT
        '2024-06-15T15:30:00Z'::TIMESTAMP AS utc_time,
        CONVERT_TIMEZONE('America/New_York', '2024-06-15T15:30:00Z'::TIMESTAMP) AS new_york,
        CONVERT_TIMEZONE('America/Los_Angeles', '2024-06-15T15:30:00Z'::TIMESTAMP) AS los_angeles,
        CONVERT_TIMEZONE('Europe/London', '2024-06-15T15:30:00Z'::TIMESTAMP) AS london,
        CONVERT_TIMEZONE('Asia/Tokyo', '2024-06-15T15:30:00Z'::TIMESTAMP) AS tokyo"#,
    snapshot_path = "convert_timezone"
);

// Test daylight saving time transitions
test_query!(
    convert_timezone_dst_spring_forward,
    r#"-- Spring forward: 2024-03-10 02:00 AM EST becomes 03:00 AM EDT
    WITH dst_times AS (
        SELECT '2024-03-10T06:00:00Z'::TIMESTAMP AS utc_before  -- 1 AM EST
        UNION ALL SELECT '2024-03-10T07:00:00Z'::TIMESTAMP      -- 3 AM EDT (2 AM skipped)
        UNION ALL SELECT '2024-03-10T08:00:00Z'::TIMESTAMP      -- 4 AM EDT
    )
    SELECT
        utc_before AS utc_time,
        CONVERT_TIMEZONE('America/New_York', utc_before) AS eastern_time
    FROM dst_times
    ORDER BY utc_before"#,
    snapshot_path = "convert_timezone"
);

test_query!(
    convert_timezone_dst_fall_back,
    r#"-- Fall back: 2024-11-03 02:00 AM EDT becomes 01:00 AM EST
    WITH dst_times AS (
        SELECT '2024-11-03T05:00:00Z'::TIMESTAMP AS utc_before  -- 1 AM EDT
        UNION ALL SELECT '2024-11-03T06:00:00Z'::TIMESTAMP      -- 1 AM EST (repeated hour)
        UNION ALL SELECT '2024-11-03T07:00:00Z'::TIMESTAMP      -- 2 AM EST
    )
    SELECT
        utc_before AS utc_time,
        CONVERT_TIMEZONE('America/New_York', utc_before) AS eastern_time
    FROM dst_times
    ORDER BY utc_before"#,
    snapshot_path = "convert_timezone"
);

// Test different timestamp precisions
test_query!(
    convert_timezone_different_precisions,
    r#"SELECT
        '2024-01-15T12:00:00'::TIMESTAMP AS seconds_precision,
        CONVERT_TIMEZONE('America/New_York', '2024-01-15T12:00:00'::TIMESTAMP) AS converted_seconds,
        '2024-01-15T12:00:00.123'::TIMESTAMP AS milliseconds_precision,
        CONVERT_TIMEZONE('America/New_York', '2024-01-15T12:00:00.123'::TIMESTAMP) AS converted_milliseconds,
        '2024-01-15T12:00:00.123456'::TIMESTAMP AS microseconds_precision,
        CONVERT_TIMEZONE('America/New_York', '2024-01-15T12:00:00.123456'::TIMESTAMP) AS converted_microseconds"#,
    snapshot_path = "convert_timezone"
);

// Test timezone conversions between non-UTC zones
test_query!(
    convert_timezone_non_utc_conversions,
    r#"SELECT
        '2024-07-15T14:30:00'::TIMESTAMP AS source_time,
        CONVERT_TIMEZONE('America/New_York', 'Europe/London', '2024-07-15T14:30:00'::TIMESTAMP) AS ny_to_london,
        CONVERT_TIMEZONE('Europe/London', 'Asia/Tokyo', '2024-07-15T14:30:00'::TIMESTAMP) AS london_to_tokyo,
        CONVERT_TIMEZONE('Asia/Tokyo', 'America/Los_Angeles', '2024-07-15T14:30:00'::TIMESTAMP) AS tokyo_to_la"#,
    snapshot_path = "convert_timezone"
);

// Test with table data
test_query!(
    convert_timezone_table_data,
    r#"WITH event_times AS (
        SELECT 'Meeting Start' AS event, '2024-01-15T09:00:00Z'::TIMESTAMP AS utc_time
        UNION ALL SELECT 'Lunch Break', '2024-01-15T12:00:00Z'::TIMESTAMP
        UNION ALL SELECT 'Meeting End', '2024-01-15T17:00:00Z'::TIMESTAMP
        UNION ALL SELECT 'Dinner', '2024-01-15T19:30:00Z'::TIMESTAMP
    )
    SELECT
        event,
        utc_time,
        CONVERT_TIMEZONE('America/New_York', utc_time) AS eastern_time,
        CONVERT_TIMEZONE('America/Los_Angeles', utc_time) AS pacific_time,
        CONVERT_TIMEZONE('Europe/London', utc_time) AS london_time
    FROM event_times
    ORDER BY utc_time"#,
    snapshot_path = "convert_timezone"
);

// Test edge cases around midnight
test_query!(
    convert_timezone_midnight_edge_cases,
    r#"WITH midnight_times AS (
        SELECT '2024-01-15T00:00:00Z'::TIMESTAMP AS utc_midnight
        UNION ALL SELECT '2024-01-15T23:59:59Z'::TIMESTAMP AS utc_almost_midnight
        UNION ALL SELECT '2024-01-16T00:00:00Z'::TIMESTAMP AS utc_next_day
    )
    SELECT
        utc_midnight AS utc_time,
        CONVERT_TIMEZONE('America/New_York', utc_midnight) AS eastern_time,
        CONVERT_TIMEZONE('Asia/Tokyo', utc_midnight) AS tokyo_time
    FROM midnight_times
    ORDER BY utc_midnight"#,
    snapshot_path = "convert_timezone"
);

// Test year boundary conversions
test_query!(
    convert_timezone_year_boundary,
    r#"WITH year_boundary AS (
        SELECT '2023-12-31T23:00:00Z'::TIMESTAMP AS utc_time, 'New Year Eve' AS description
        UNION ALL SELECT '2024-01-01T00:00:00Z'::TIMESTAMP, 'New Year UTC'
        UNION ALL SELECT '2024-01-01T05:00:00Z'::TIMESTAMP, 'New Year EST'
    )
    SELECT
        description,
        utc_time,
        CONVERT_TIMEZONE('America/New_York', utc_time) AS eastern_time,
        CONVERT_TIMEZONE('Asia/Tokyo', utc_time) AS tokyo_time
    FROM year_boundary
    ORDER BY utc_time"#,
    snapshot_path = "convert_timezone"
);

// Test common business timezone conversions
test_query!(
    convert_timezone_business_hours,
    r#"-- Business hours conversion: 9 AM EST to various timezones
    WITH business_hours AS (
        SELECT '2024-03-15T14:00:00Z'::TIMESTAMP AS utc_time, '9 AM EST' AS description
        UNION ALL SELECT '2024-03-15T17:00:00Z'::TIMESTAMP, '12 PM EST'
        UNION ALL SELECT '2024-03-15T22:00:00Z'::TIMESTAMP, '5 PM EST'
    )
    SELECT
        description,
        utc_time,
        CONVERT_TIMEZONE('America/New_York', utc_time) AS eastern,
        CONVERT_TIMEZONE('America/Chicago', utc_time) AS central,
        CONVERT_TIMEZONE('America/Denver', utc_time) AS mountain,
        CONVERT_TIMEZONE('America/Los_Angeles', utc_time) AS pacific
    FROM business_hours
    ORDER BY utc_time"#,
    snapshot_path = "convert_timezone"
);

// Test international timezone conversions
test_query!(
    convert_timezone_international,
    r#"SELECT
        '2024-06-15T12:00:00Z'::TIMESTAMP AS utc_noon,
        CONVERT_TIMEZONE('Europe/London', '2024-06-15T12:00:00Z'::TIMESTAMP) AS london,
        CONVERT_TIMEZONE('Europe/Paris', '2024-06-15T12:00:00Z'::TIMESTAMP) AS paris,
        CONVERT_TIMEZONE('Europe/Berlin', '2024-06-15T12:00:00Z'::TIMESTAMP) AS berlin,
        CONVERT_TIMEZONE('Asia/Tokyo', '2024-06-15T12:00:00Z'::TIMESTAMP) AS tokyo,
        CONVERT_TIMEZONE('Asia/Shanghai', '2024-06-15T12:00:00Z'::TIMESTAMP) AS shanghai,
        CONVERT_TIMEZONE('Australia/Sydney', '2024-06-15T12:00:00Z'::TIMESTAMP) AS sydney"#,
    snapshot_path = "convert_timezone"
);

// Test roundtrip conversions
test_query!(
    convert_timezone_roundtrip,
    r#"WITH original_time AS (
        SELECT '2024-01-15T12:00:00'::TIMESTAMP AS source_time
    )
    SELECT
        source_time,
        CONVERT_TIMEZONE('UTC', 'America/New_York', source_time) AS utc_to_est,
        CONVERT_TIMEZONE('America/New_York', 'UTC',
            CONVERT_TIMEZONE('UTC', 'America/New_York', source_time)) AS roundtrip_utc
    FROM original_time"#,
    snapshot_path = "convert_timezone"
);

// Test extreme timezone offsets
test_query!(
    convert_timezone_extreme_offsets,
    r#"SELECT
        '2024-01-15T12:00:00Z'::TIMESTAMP AS utc_time,
        CONVERT_TIMEZONE('Pacific/Kiritimati', '2024-01-15T12:00:00Z'::TIMESTAMP) AS plus_14_hours,
        CONVERT_TIMEZONE('Pacific/Niue', '2024-01-15T12:00:00Z'::TIMESTAMP) AS minus_11_hours,
        CONVERT_TIMEZONE('Pacific/Chatham', '2024-01-15T12:00:00Z'::TIMESTAMP) AS plus_12_45_minutes"#,
    snapshot_path = "convert_timezone"
);

// Test timezone conversions during different seasons
test_query!(
    convert_timezone_seasonal_differences,
    r#"WITH seasonal_times AS (
        SELECT '2024-01-15T12:00:00Z'::TIMESTAMP AS winter_time, 'Winter' AS season
        UNION ALL SELECT '2024-04-15T12:00:00Z'::TIMESTAMP, 'Spring'
        UNION ALL SELECT '2024-07-15T12:00:00Z'::TIMESTAMP, 'Summer'
        UNION ALL SELECT '2024-10-15T12:00:00Z'::TIMESTAMP, 'Fall'
    )
    SELECT
        season,
        winter_time AS utc_time,
        CONVERT_TIMEZONE('America/New_York', winter_time) AS eastern_time,
        CONVERT_TIMEZONE('Europe/London', winter_time) AS london_time,
        CONVERT_TIMEZONE('Australia/Sydney', winter_time) AS sydney_time
    FROM seasonal_times
    ORDER BY winter_time"#,
    snapshot_path = "convert_timezone"
);

// Test historical timezone conversions
test_query!(
    convert_timezone_historical_dates,
    r#"WITH historical_dates AS (
        SELECT '1970-01-01T00:00:00Z'::TIMESTAMP AS unix_epoch, 'Unix Epoch' AS description
        UNION ALL SELECT '1969-07-20T20:17:00Z'::TIMESTAMP, 'Moon Landing'
        UNION ALL SELECT '2000-01-01T00:00:00Z'::TIMESTAMP, 'Y2K'
        UNION ALL SELECT '2001-09-11T08:46:00Z'::TIMESTAMP, '9/11 First Impact'
    )
    SELECT
        description,
        unix_epoch AS utc_time,
        CONVERT_TIMEZONE('America/New_York', unix_epoch) AS eastern_time,
        CONVERT_TIMEZONE('America/Los_Angeles', unix_epoch) AS pacific_time
    FROM historical_dates
    ORDER BY unix_epoch"#,
    snapshot_path = "convert_timezone"
);

// Test timezone conversions for financial markets
test_query!(
    convert_timezone_financial_markets,
    r#"-- Financial market opening times in UTC converted to local times
    WITH market_opens AS (
        SELECT '2024-03-15T14:30:00Z'::TIMESTAMP AS utc_time, 'NYSE Open (9:30 AM EST)' AS market
        UNION ALL SELECT '2024-03-15T08:00:00Z'::TIMESTAMP, 'LSE Open (8:00 AM GMT)'
        UNION ALL SELECT '2024-03-15T00:00:00Z'::TIMESTAMP, 'TSE Open (9:00 AM JST)'
        UNION ALL SELECT '2024-03-15T22:00:00Z'::TIMESTAMP, 'ASX Open (9:00 AM AEDT)'
    )
    SELECT
        market,
        utc_time,
        CONVERT_TIMEZONE('America/New_York', utc_time) AS new_york_time,
        CONVERT_TIMEZONE('Europe/London', utc_time) AS london_time,
        CONVERT_TIMEZONE('Asia/Tokyo', utc_time) AS tokyo_time,
        CONVERT_TIMEZONE('Australia/Sydney', utc_time) AS sydney_time
    FROM market_opens
    ORDER BY utc_time"#,
    snapshot_path = "convert_timezone"
);

// Test timezone conversions with microsecond precision
test_query!(
    convert_timezone_microsecond_precision,
    r#"SELECT
        '2024-01-15T12:30:45.123456Z'::TIMESTAMP AS utc_microseconds,
        CONVERT_TIMEZONE('America/New_York', '2024-01-15T12:30:45.123456Z'::TIMESTAMP) AS est_microseconds,
        CONVERT_TIMEZONE('Europe/Berlin', '2024-01-15T12:30:45.123456Z'::TIMESTAMP) AS berlin_microseconds,
        CONVERT_TIMEZONE('Asia/Tokyo', '2024-01-15T12:30:45.123456Z'::TIMESTAMP) AS tokyo_microseconds"#,
    snapshot_path = "convert_timezone"
);

// Test timezone conversions during leap year
test_query!(
    convert_timezone_leap_year,
    r#"WITH leap_year_dates AS (
        SELECT '2024-02-28T12:00:00Z'::TIMESTAMP AS feb_28, 'Feb 28 (leap year)' AS description
        UNION ALL SELECT '2024-02-29T12:00:00Z'::TIMESTAMP, 'Feb 29 (leap day)'
        UNION ALL SELECT '2024-03-01T12:00:00Z'::TIMESTAMP, 'Mar 1 (after leap day)'
        UNION ALL SELECT '2023-02-28T12:00:00Z'::TIMESTAMP, 'Feb 28 (non-leap year)'
    )
    SELECT
        description,
        feb_28 AS utc_time,
        CONVERT_TIMEZONE('America/New_York', feb_28) AS eastern_time,
        CONVERT_TIMEZONE('Europe/Paris', feb_28) AS paris_time
    FROM leap_year_dates
    ORDER BY feb_28"#,
    snapshot_path = "convert_timezone"
);

// Test timezone conversions with batch processing
test_query!(
    convert_timezone_batch_processing,
    r#"-- Simulate batch processing of timestamps
    WITH event_log AS (
        SELECT 1 AS event_id, '2024-01-15T08:00:00Z'::TIMESTAMP AS event_time, 'System Start' AS event_type
        UNION ALL SELECT 2, '2024-01-15T12:30:00Z'::TIMESTAMP, 'User Login'
        UNION ALL SELECT 3, '2024-01-15T14:15:00Z'::TIMESTAMP, 'Data Processing'
        UNION ALL SELECT 4, '2024-01-15T16:45:00Z'::TIMESTAMP, 'Report Generation'
        UNION ALL SELECT 5, '2024-01-15T20:00:00Z'::TIMESTAMP, 'System Backup'
        UNION ALL SELECT 6, '2024-01-15T23:30:00Z'::TIMESTAMP, 'Maintenance Window'
    )
    SELECT
        event_id,
        event_type,
        event_time AS utc_time,
        CONVERT_TIMEZONE('America/New_York', event_time) AS local_time_ny,
        CONVERT_TIMEZONE('Europe/London', event_time) AS local_time_london,
        CONVERT_TIMEZONE('Asia/Singapore', event_time) AS local_time_singapore
    FROM event_log
    ORDER BY event_id"#,
    snapshot_path = "convert_timezone"
);
