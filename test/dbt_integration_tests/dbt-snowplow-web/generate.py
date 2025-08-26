#!/usr/bin/env python3
"""
Corrected script to generate Snowplow event data with exact format needed for embucket upload.
Uses lowercase headers and 136 columns matching events_yesterday.csv structure.
Supports both CSV and Parquet output formats.
"""

import csv
import uuid
import random
import json
import argparse
from datetime import datetime, timedelta
import os
import sys

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False


def generate_enhanced_event_data(num_events=10):
    """Generate enhanced Snowplow event data with variability matching gen_events.py patterns."""
    
    events = []
    
    # Enhanced data patterns with more variety (based on gen_events.py)
    countries = ['US', 'CA', 'GB', 'DE', 'FR', 'JP', 'AU', 'BR', 'IN', 'MX']
    cities = ['New York', 'London', 'Berlin', 'Toronto', 'Paris', 'Tokyo', 'Sydney', 'São Paulo', 'Mumbai', 'Mexico City']
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1',
        'Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36'
    ]
    event_types = ['page_view', 'page_ping', 'unstruct', 'struct', 'web_vitals', 'cmp_visible', 'consent_preferences']
    pages = [
        'https://example.com/home',
        'https://example.com/products', 
        'https://example.com/about',
        'https://example.com/contact',
        'https://example.com/blog'
    ]
    
    for i in range(num_events):
        # Generate timestamps
        base_time = datetime.now() - timedelta(days=1, hours=random.randint(0, 23))
        collector_tstamp = base_time
        dvce_created_tstamp = base_time - timedelta(seconds=random.randint(1, 5))
        etl_tstamp = base_time + timedelta(seconds=random.randint(1, 3))
        
        # Generate basic identifiers
        event_id = str(uuid.uuid4())
        domain_userid = str(uuid.uuid4())
        domain_sessionid = str(uuid.uuid4())
        network_userid = str(uuid.uuid4())
        
        # Select random values
        country = random.choice(countries)
        city = random.choice(cities)
        user_agent = random.choice(user_agents)
        event_type = random.choice(event_types)
        page_url = random.choice(pages)
        
        # Generate context fields with variability (based on gen_events.py patterns)
        # Web page context with unique ID
        web_page_context = json.dumps([{'id': str(uuid.uuid4())}])
        
        # UA parser context based on user agent
        device_family = 'iPhone' if 'iPhone' in user_agent else ('Android' if 'Android' in user_agent else 'Desktop')
        os_family = 'iOS' if 'iPhone' in user_agent else ('Android' if 'Android' in user_agent else 'Windows')
        browser_family = 'Safari' if 'Safari' in user_agent else 'Chrome'
        ua_context = json.dumps([{
            'deviceFamily': device_family,
            'osFamily': os_family,
            'useragentFamily': browser_family
        }])
        
        # IAB spiders and robots context
        iab_context = json.dumps([{'category': 'BROWSER', 'spiderOrRobot': False}])
        
        # YAUAA context based on user agent
        device_class = 'Phone' if 'Mobile' in user_agent or 'iPhone' in user_agent else 'Desktop'
        yauaa_context = json.dumps([{
            'agentClass': 'Browser', 
            'deviceClass': device_class
        }])
        
        # Web vitals context (for some events)
        web_vitals_context = ''
        if event_type in ['web_vitals', 'page_view']:
            web_vitals_context = json.dumps([{
                'cls': round(random.uniform(0.01, 0.1), 3),
                'fcp': random.randint(100, 500),
                'fid': random.randint(10, 100),
                'inp': random.randint(10, 100),
                'lcp': random.randint(1000, 3000),
                'navigation_type': 'navigate',
                'ttfb': random.randint(50, 300)
            }])
        
        # Create event row with all 136 columns in exact order
        event = [
            'website',  # app_id
            'web',      # platform
            etl_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # etl_tstamp
            collector_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # collector_tstamp
            dvce_created_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # dvce_created_tstamp
            event_type,  # event
            event_id,   # event_id
            '',         # txn_id
            'eng.gcp-dev1',  # name_tracker
            'js-2.17.2',     # v_tracker
            'ssc-2.1.2-googlepubsub',  # v_collector
            'beam-enrich-1.4.2-rc1-common-1.4.2-rc1',  # v_etl
            '',         # user_id
            f'{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}',  # user_ipaddress
            str(uuid.uuid4()),  # user_fingerprint
            domain_userid,      # domain_userid
            '1',               # domain_sessionidx
            network_userid,    # network_userid
            country,           # geo_country
            '',               # geo_region
            city,             # geo_city
            '',               # geo_zipcode
            str(random.uniform(-90, 90)),   # geo_latitude
            str(random.uniform(-180, 180)), # geo_longitude
            '',               # geo_region_name
            'ISP Provider',   # ip_isp
            'ISP Provider',   # ip_organization
            'isp.net',        # ip_domain
            '',               # ip_netspeed
            page_url,  # page_url
            'Example Page',          # page_title
            'https://www.google.com/',  # page_referrer
            'https',                    # page_urlscheme
            'example.com',              # page_urlhost
            '443',                      # page_urlport
            '/',                        # page_urlpath
            '',                         # page_urlquery
            '',                         # page_urlfragment
            'https',                    # refr_urlscheme
            'www.google.com',           # refr_urlhost
            '443',                      # refr_urlport
            '/',                        # refr_urlpath
            '',                         # refr_urlquery
            '',                         # refr_urlfragment
            'search',                   # refr_medium
            'Google',                   # refr_source
            '',                         # refr_term
            '',                         # mkt_medium
            '',                         # mkt_source
            '',                         # mkt_term
            '',                         # mkt_content
            '',                         # mkt_campaign
            '',                         # se_category
            '',                         # se_action
            '',                         # se_label
            '',                         # se_property
            '',                         # se_value
            '',                         # tr_orderid
            '',                         # tr_affiliation
            '',                         # tr_total
            '',                         # tr_tax
            '',                         # tr_shipping
            '',                         # tr_city
            '',                         # tr_state
            '',                         # tr_country
            '',                         # ti_orderid
            '',                         # ti_sku
            '',                         # ti_name
            '',                         # ti_category
            '',                         # ti_price
            '',                         # ti_quantity
            '',                         # pp_xoffset_min
            '',                         # pp_xoffset_max
            '',                         # pp_yoffset_min
            '',                         # pp_yoffset_max
            user_agent,                 # useragent
            '',                         # br_name
            '',                         # br_family
            '',                         # br_version
            '',                         # br_type
            '',                         # br_renderengine
            'en-US',                    # br_lang
            '',                         # br_features_pdf
            '',                         # br_features_flash
            '',                         # br_features_java
            '',                         # br_features_director
            '',                         # br_features_quicktime
            '',                         # br_features_realplayer
            '',                         # br_features_windowsmedia
            '',                         # br_features_gears
            '',                         # br_features_silverlight
            'TRUE',                     # br_cookies
            '24',                       # br_colordepth
            str(random.randint(800, 1920)),   # br_viewwidth
            str(random.randint(600, 1080)),   # br_viewheight
            '',                         # os_name
            '',                         # os_family
            '',                         # os_manufacturer
            'America/New_York',         # os_timezone
            '',                         # dvce_type
            'TRUE' if 'Mobile' in user_agent or 'iPhone' in user_agent else 'FALSE',  # dvce_ismobile
            str(random.randint(800, 1920)),   # dvce_screenwidth
            str(random.randint(600, 1080)),   # dvce_screenheight
            'UTF-8',                    # doc_charset
            str(random.randint(800, 1920)),   # doc_width
            str(random.randint(600, 1080)),   # doc_height
            '',                         # tr_currency
            '',                         # tr_total_base
            '',                         # tr_tax_base
            '',                         # tr_shipping_base
            '',                         # ti_currency
            '',                         # ti_price_base
            '',                         # base_currency
            'America/New_York',         # geo_timezone
            '',                         # mkt_clickid
            '',                         # mkt_network
            '',                         # etl_tags
            dvce_created_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # dvce_sent_tstamp
            '',                         # refr_domain_userid
            '',                         # refr_dvce_tstamp
            domain_sessionid,           # domain_sessionid
            collector_tstamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],     # derived_tstamp
            'com.snowplowanalytics.snowplow',  # event_vendor
            event_type,                        # event_name
            'jsonschema',                      # event_format
            '1-0-0',                          # event_version
            str(uuid.uuid4()),                # event_fingerprint
            '',                               # true_tstamp
            '',                               # load_tstamp
            web_page_context,                 # contexts_com_snowplowanalytics_snowplow_web_page_1
            '',                               # unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1
            '',                               # unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1
            iab_context,                      # contexts_com_iab_snowplow_spiders_and_robots_1
            ua_context,                       # contexts_com_snowplowanalytics_snowplow_ua_parser_context_1
            yauaa_context,                    # contexts_nl_basjes_yauaa_context_1
            web_vitals_context,               # unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1
        ]
        
        events.append(event)
    
    return events


def get_column_headers():
    """Get the exact 136 column headers matching embucket format."""
    return [
        'app_id', 'platform', 'etl_tstamp', 'collector_tstamp', 'dvce_created_tstamp', 'event', 'event_id', 'txn_id',
        'name_tracker', 'v_tracker', 'v_collector', 'v_etl', 'user_id', 'user_ipaddress', 'user_fingerprint',
        'domain_userid', 'domain_sessionidx', 'network_userid', 'geo_country', 'geo_region', 'geo_city', 'geo_zipcode',
        'geo_latitude', 'geo_longitude', 'geo_region_name', 'ip_isp', 'ip_organization', 'ip_domain', 'ip_netspeed',
        'page_url', 'page_title', 'page_referrer', 'page_urlscheme', 'page_urlhost', 'page_urlport', 'page_urlpath',
        'page_urlquery', 'page_urlfragment', 'refr_urlscheme', 'refr_urlhost', 'refr_urlport', 'refr_urlpath',
        'refr_urlquery', 'refr_urlfragment', 'refr_medium', 'refr_source', 'refr_term', 'mkt_medium', 'mkt_source',
        'mkt_term', 'mkt_content', 'mkt_campaign', 'se_category', 'se_action', 'se_label', 'se_property', 'se_value',
        'tr_orderid', 'tr_affiliation', 'tr_total', 'tr_tax', 'tr_shipping', 'tr_city', 'tr_state', 'tr_country',
        'ti_orderid', 'ti_sku', 'ti_name', 'ti_category', 'ti_price', 'ti_quantity', 'pp_xoffset_min', 'pp_xoffset_max',
        'pp_yoffset_min', 'pp_yoffset_max', 'useragent', 'br_name', 'br_family', 'br_version', 'br_type', 'br_renderengine',
        'br_lang', 'br_features_pdf', 'br_features_flash', 'br_features_java', 'br_features_director', 'br_features_quicktime',
        'br_features_realplayer', 'br_features_windowsmedia', 'br_features_gears', 'br_features_silverlight', 'br_cookies',
        'br_colordepth', 'br_viewwidth', 'br_viewheight', 'os_name', 'os_family', 'os_manufacturer', 'os_timezone',
        'dvce_type', 'dvce_ismobile', 'dvce_screenwidth', 'dvce_screenheight', 'doc_charset', 'doc_width', 'doc_height',
        'tr_currency', 'tr_total_base', 'tr_tax_base', 'tr_shipping_base', 'ti_currency', 'ti_price_base', 'base_currency',
        'geo_timezone', 'mkt_clickid', 'mkt_network', 'etl_tags', 'dvce_sent_tstamp', 'refr_domain_userid', 'refr_dvce_tstamp',
        'domain_sessionid', 'derived_tstamp', 'event_vendor', 'event_name', 'event_format', 'event_version', 'event_fingerprint',
        'true_tstamp', 'load_tstamp', 'contexts_com_snowplowanalytics_snowplow_web_page_1',
        'unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1', 'unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1',
        'contexts_com_iab_snowplow_spiders_and_robots_1', 'contexts_com_snowplowanalytics_snowplow_ua_parser_context_1',
        'contexts_nl_basjes_yauaa_context_1', 'unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1'
    ]


def write_events_csv(filename, events):
    """Write events to CSV with exact 136 column headers."""
    headers = get_column_headers()
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(headers)
        writer.writerows(events)
    
    print(f"✓ Generated {len(events):,} events in {filename} (CSV)")
    print(f"✓ File has {len(headers)} columns (matching embucket format)")


def write_events_parquet(filename, events):
    """Write events to Parquet format."""
    if not PARQUET_AVAILABLE:
        print("❌ Error: PyArrow not available. Install with: pip install pyarrow")
        sys.exit(1)
    
    headers = get_column_headers()
    
    # Create PyArrow table
    # Convert list of lists to dictionary of columns
    data = {}
    for i, header in enumerate(headers):
        data[header] = [row[i] for row in events]
    
    # Create PyArrow schema with appropriate types
    schema_fields = []
    for header in headers:
        if header in ['etl_tstamp', 'collector_tstamp', 'dvce_created_tstamp', 'dvce_sent_tstamp', 'derived_tstamp']:
            schema_fields.append(pa.field(header, pa.timestamp('ms')))
        else:
            schema_fields.append(pa.field(header, pa.string()))
    
    schema = pa.schema(schema_fields)
    
    # Convert timestamp strings to proper timestamps for timestamp columns
    for header in headers:
        if header in ['etl_tstamp', 'collector_tstamp', 'dvce_created_tstamp', 'dvce_sent_tstamp', 'derived_tstamp']:
            # Convert timestamp strings to datetime objects, handle empty strings
            timestamp_data = []
            for val in data[header]:
                if val and val.strip():
                    try:
                        timestamp_data.append(datetime.strptime(val, '%Y-%m-%d %H:%M:%S.%f'))
                    except ValueError:
                        timestamp_data.append(None)
                else:
                    timestamp_data.append(None)
            data[header] = timestamp_data
    
    # Create PyArrow table
    table = pa.table(data, schema=schema)
    
    # Write to Parquet
    pq.write_table(table, filename)
    
    print(f"✓ Generated {len(events):,} events in {filename} (Parquet)")
    print(f"✓ File has {len(headers)} columns (matching embucket format)")


def write_events(filename, events, format_type='csv'):
    """Write events in specified format (csv or parquet)."""
    if format_type.lower() == 'parquet':
        write_events_parquet(filename, events)
    else:
        write_events_csv(filename, events)


def main():
    parser = argparse.ArgumentParser(description='Generate corrected Snowplow event dataset for embucket')
    parser.add_argument('--events', '-n', type=int, default=10, 
                       help='Number of events to generate (default: 10)')
    parser.add_argument('--output', '-o', type=str, default='test_events.csv',
                       help='Output filename (default: test_events.csv)')
    parser.add_argument('--format', '-f', type=str, choices=['csv', 'parquet'], default='csv',
                       help='Output format: csv or parquet (default: csv)')
    
    args = parser.parse_args()
    
    # Auto-detect format from filename if not specified
    if args.format == 'csv' and args.output.lower().endswith('.parquet'):
        args.format = 'parquet'
    elif args.format == 'parquet' and args.output.lower().endswith('.csv'):
        args.format = 'csv'
    
    # Check PyArrow availability for Parquet format
    if args.format == 'parquet' and not PARQUET_AVAILABLE:
        print("❌ Error: PyArrow not available for Parquet output. Install with:")
        print("   pip install pyarrow")
        print("Falling back to CSV format...")
        args.format = 'csv'
        if not args.output.lower().endswith('.csv'):
            args.output = args.output.rsplit('.', 1)[0] + '.csv'
    
    print(f"""
🔧 Corrected Snowplow Dataset Generator
======================================
Events to generate: {args.events:,}
Output file: {args.output}
Output format: {args.format.upper()}
Schema: 136 columns, lowercase headers, embucket-compatible
""")
    
    # Generate the dataset
    events = generate_enhanced_event_data(args.events)
    
    # Write in specified format
    write_events(args.output, events, args.format)
    
    if args.format == 'csv':
        upload_command = f"""  snow sql -c local -q "COPY INTO embucket.public_snowplow_manifest.events FROM 'file:///Users/ramp/vcs/embucket/test/dbt_integration_tests/dbt-snowplow-web/{args.output}' FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1) ON_ERROR = 'CONTINUE';"""
    else:
        upload_command = f"""  snow sql -c local -q "COPY INTO embucket.public_snowplow_manifest.events FROM 'file:///Users/ramp/vcs/embucket/test/dbt_integration_tests/dbt-snowplow-web/{args.output}' FILE_FORMAT = (TYPE = PARQUET) ON_ERROR = 'CONTINUE';"""
    
    print(f"""
✅ Dataset generation complete!

File: {args.output}
Format: {args.format.upper()}
Size: {len(events):,} events
Columns: 136 (matching embucket requirements)

Ready to test upload with:
{upload_command}
""")


if __name__ == "__main__":
    main()