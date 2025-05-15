import requests
import csv
import tempfile
import os

url = "http://localhost:3000"

database = "embucket"
schema = "public"


def bootstrap(catalog, schema):
    response = requests.get(f"{url}/v1/metastore/databases")
    response.raise_for_status()

    wh_list = [wh for wh in response.json() if wh["ident"] == database]
    if wh_list:
        return

    ### VOLUME
    response = requests.post(
        f"{url}/v1/metastore/volumes",
        json={
            "ident": "test",
            "type": "file",
            "path": f"{os.getcwd()}/data",
        },
        # json={
        #     "ident": "test",
        #     "type": "s3",
        #     "bucket": "acmecom-lakehouse",
        #     "region": "us-east-2",
        #     "credentials": {
        #         "credential_type": "access_key",
        #         "aws-access-key-id": "xxx",
        #         "aws-secret-access-key": "xxx",
        #     },
        # },
        # json={
        #     "ident": "test",
        #     "type": "s3",
        #     "bucket": "acmecom-lakehouse",
        #     "endpoint": "http://localhost:9000",
        #     "credentials": {
        #         "credential_type": "access_key",
        #         "aws-access-key-id": "minioadmin",
        #         "aws-secret-access-key": "minioadmin",
        #     },
        # },
    )
    response.raise_for_status()

    ## DATABASE
    response = requests.post(
        f"{url}/v1/metastore/databases",
        json={
            "volume": "test",
            "ident": database,
        },
    )
    response.raise_for_status()

    ## SCHEMA
    response = requests.post(
        f"{url}/v1/metastore/databases/{database}/schemas",
        json={
            "ident": {
                "schema": schema,
                "database": database,
            },
        },
    )
    response.raise_for_status()


mobile_query = """CREATE OR REPLACE TABLE {database}.{schema}.{table} 
(
APP_ID VARCHAR(255),
PLATFORM VARCHAR(255),
ETL_TSTAMP TIMESTAMP_NTZ(9),
COLLECTOR_TSTAMP TIMESTAMP_NTZ(9) NOT NULL,
DVCE_CREATED_TSTAMP TIMESTAMP_NTZ(9),
EVENT VARCHAR(128),
EVENT_ID VARCHAR(36) NOT NULL,
TXN_ID NUMBER(38,0),
NAME_TRACKER VARCHAR(128),
V_TRACKER VARCHAR(100),
V_COLLECTOR VARCHAR(100) NOT NULL,
V_ETL VARCHAR(100) NOT NULL,
USER_ID VARCHAR(255),
USER_IPADDRESS VARCHAR(128),
USER_FINGERPRINT VARCHAR(128),
DOMAIN_USERID VARCHAR(128),
DOMAIN_SESSIONIDX NUMBER(38,0),
NETWORK_USERID VARCHAR(128),
GEO_COUNTRY VARCHAR(2),
GEO_REGION VARCHAR(3),
GEO_CITY VARCHAR(75),
GEO_ZIPCODE VARCHAR(15),
GEO_LATITUDE FLOAT,
GEO_LONGITUDE FLOAT,
GEO_REGION_NAME VARCHAR(100),
IP_ISP VARCHAR(100),
IP_ORGANIZATION VARCHAR(128),
IP_DOMAIN VARCHAR(128),
IP_NETSPEED VARCHAR(100),
PAGE_URL VARCHAR(4096),
PAGE_TITLE VARCHAR(2000),
PAGE_REFERRER VARCHAR(4096),
PAGE_URLSCHEME VARCHAR(16),
PAGE_URLHOST VARCHAR(255),
PAGE_URLPORT NUMBER(38,0),
PAGE_URLPATH VARCHAR(3000),
PAGE_URLQUERY VARCHAR(6000),
PAGE_URLFRAGMENT VARCHAR(3000),
REFR_URLSCHEME VARCHAR(16),
REFR_URLHOST VARCHAR(255),
REFR_URLPORT NUMBER(38,0),
REFR_URLPATH VARCHAR(6000),
REFR_URLQUERY VARCHAR(6000),
REFR_URLFRAGMENT VARCHAR(3000),
REFR_MEDIUM VARCHAR(25),
REFR_SOURCE VARCHAR(50),
REFR_TERM VARCHAR(255),
MKT_MEDIUM VARCHAR(255),
MKT_SOURCE VARCHAR(255),
MKT_TERM VARCHAR(255),
MKT_CONTENT VARCHAR(500),
MKT_CAMPAIGN VARCHAR(255),
SE_CATEGORY VARCHAR(1000),
SE_ACTION VARCHAR(1000),
SE_LABEL VARCHAR(4096),
SE_PROPERTY VARCHAR(1000),
SE_VALUE FLOAT,
TR_ORDERID VARCHAR(255),
TR_AFFILIATION VARCHAR(255),
TR_TOTAL NUMBER(18,2),
TR_TAX NUMBER(18,2),
TR_SHIPPING NUMBER(18,2),
TR_CITY VARCHAR(255),
TR_STATE VARCHAR(255),
TR_COUNTRY VARCHAR(255),
TI_ORDERID VARCHAR(255),
TI_SKU VARCHAR(255),
TI_NAME VARCHAR(255),
TI_CATEGORY VARCHAR(255),
TI_PRICE NUMBER(18,2),
TI_QUANTITY NUMBER(38,0),
PP_XOFFSET_MIN NUMBER(38,0),
PP_XOFFSET_MAX NUMBER(38,0),
PP_YOFFSET_MIN NUMBER(38,0),
PP_YOFFSET_MAX NUMBER(38,0),
USERAGENT VARCHAR(1000),
BR_NAME VARCHAR(50),
BR_FAMILY VARCHAR(50),
BR_VERSION VARCHAR(50),
BR_TYPE VARCHAR(50),
BR_RENDERENGINE VARCHAR(50),
BR_LANG VARCHAR(255),
BR_FEATURES_PDF BOOLEAN,
BR_FEATURES_FLASH BOOLEAN,
BR_FEATURES_JAVA BOOLEAN,
BR_FEATURES_DIRECTOR BOOLEAN,
BR_FEATURES_QUICKTIME BOOLEAN,
BR_FEATURES_REALPLAYER BOOLEAN,
BR_FEATURES_WINDOWSMEDIA BOOLEAN,
BR_FEATURES_GEARS BOOLEAN,
BR_FEATURES_SILVERLIGHT BOOLEAN,
BR_COOKIES BOOLEAN,
BR_COLORDEPTH VARCHAR(12),
BR_VIEWWIDTH NUMBER(38,0),
BR_VIEWHEIGHT NUMBER(38,0),
OS_NAME VARCHAR(50),
OS_FAMILY VARCHAR(50),
OS_MANUFACTURER VARCHAR(50),
OS_TIMEZONE VARCHAR(255),
DVCE_TYPE VARCHAR(50),
DVCE_ISMOBILE BOOLEAN,
DVCE_SCREENWIDTH NUMBER(38,0),
DVCE_SCREENHEIGHT NUMBER(38,0),
DOC_CHARSET VARCHAR(128),
DOC_WIDTH NUMBER(38,0),
DOC_HEIGHT NUMBER(38,0),
TR_CURRENCY VARCHAR(3),
TR_TOTAL_BASE NUMBER(18,2),
TR_TAX_BASE NUMBER(18,2),
TR_SHIPPING_BASE NUMBER(18,2),
TI_CURRENCY VARCHAR(3),
TI_PRICE_BASE NUMBER(18,2),
BASE_CURRENCY VARCHAR(3),
GEO_TIMEZONE VARCHAR(64),
MKT_CLICKID VARCHAR(128),
MKT_NETWORK VARCHAR(64),
ETL_TAGS VARCHAR(500),
DVCE_SENT_TSTAMP TIMESTAMP_NTZ(9),
REFR_DOMAIN_USERID VARCHAR(128),
REFR_DVCE_TSTAMP TIMESTAMP_NTZ(9),
DOMAIN_SESSIONID VARCHAR(128),
DERIVED_TSTAMP TIMESTAMP_NTZ(9),
EVENT_VENDOR VARCHAR(1000),
EVENT_NAME VARCHAR(1000),
EVENT_FORMAT VARCHAR(128),
EVENT_VERSION VARCHAR(128),
EVENT_FINGERPRINT VARCHAR(128),
TRUE_TSTAMP TIMESTAMP_NTZ(9),
LOAD_TSTAMP TIMESTAMP_NTZ(9),
CONTEXTS_COM_SNOWPLOWANALYTICS_MOBILE_SCREEN_1 VARCHAR,
CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_CLIENT_SESSION_1 VARCHAR,
CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_GEOLOCATION_CONTEXT_1 VARCHAR,
CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_MOBILE_CONTEXT_1 VARCHAR,
CONTEXTS_COM_SNOWPLOWANALYTICS_MOBILE_APPLICATION_1 VARCHAR,
UNSTRUCT_EVENT_COM_SNOWPLOWANALYTICS_MOBILE_SCREEN_VIEW_1 VARCHAR,
constraint EVENT_ID_PK primary key (EVENT_ID)
);"""


web_table = """create or replace TABLE {database}.{schema}.{table} 
(
APP_ID TEXT,
PLATFORM TEXT,
ETL_TSTAMP TIMESTAMP_NTZ(9),
COLLECTOR_TSTAMP TIMESTAMP_NTZ(9) NOT NULL,
DVCE_CREATED_TSTAMP TIMESTAMP_NTZ(9),
EVENT TEXT,
EVENT_ID TEXT,
TXN_ID NUMBER(38,0),
NAME_TRACKER TEXT,
V_TRACKER TEXT,
V_COLLECTOR TEXT,
V_ETL TEXT,
USER_ID TEXT,
USER_IPADDRESS TEXT,
USER_FINGERPRINT TEXT,
DOMAIN_USERID TEXT,
DOMAIN_SESSIONIDX NUMBER(38,0),
NETWORK_USERID TEXT,
GEO_COUNTRY TEXT,
GEO_REGION TEXT,
GEO_CITY TEXT,
GEO_ZIPCODE TEXT,
GEO_LATITUDE FLOAT,
GEO_LONGITUDE FLOAT,
GEO_REGION_NAME TEXT,
IP_ISP TEXT,
IP_ORGANIZATION TEXT,
IP_DOMAIN TEXT,
IP_NETSPEED TEXT,
PAGE_URL TEXT,
PAGE_TITLE TEXT,
PAGE_REFERRER TEXT,
PAGE_URLSCHEME TEXT,
PAGE_URLHOST TEXT,
PAGE_URLPORT NUMBER(38,0),
PAGE_URLPATH TEXT,
PAGE_URLQUERY TEXT,
PAGE_URLFRAGMENT TEXT,
REFR_URLSCHEME TEXT,
REFR_URLHOST TEXT,
REFR_URLPORT NUMBER(38,0),
REFR_URLPATH TEXT,
REFR_URLQUERY TEXT,
REFR_URLFRAGMENT TEXT,
REFR_MEDIUM TEXT,
REFR_SOURCE TEXT,
REFR_TERM TEXT,
MKT_MEDIUM TEXT,
MKT_SOURCE TEXT,
MKT_TERM TEXT,
MKT_CONTENT TEXT,
MKT_CAMPAIGN TEXT,
SE_CATEGORY TEXT,
SE_ACTION TEXT,
SE_LABEL TEXT,
SE_PROPERTY TEXT,
SE_VALUE FLOAT,
TR_ORDERID TEXT,
TR_AFFILIATION TEXT,
TR_TOTAL NUMBER(18,2),
TR_TAX NUMBER(18,2),
TR_SHIPPING NUMBER(18,2),
TR_CITY TEXT,
TR_STATE TEXT,
TR_COUNTRY TEXT,
TI_ORDERID TEXT,
TI_SKU TEXT,
TI_NAME TEXT,
TI_CATEGORY TEXT,
TI_PRICE NUMBER(18,2),
TI_QUANTITY NUMBER(38,0),
PP_XOFFSET_MIN NUMBER(38,0),
PP_XOFFSET_MAX NUMBER(38,0),
PP_YOFFSET_MIN NUMBER(38,0),
PP_YOFFSET_MAX NUMBER(38,0),
USERAGENT TEXT,
BR_NAME TEXT,
BR_FAMILY TEXT,
BR_VERSION TEXT,
BR_TYPE TEXT,
BR_RENDERENGINE TEXT,
BR_LANG TEXT,
BR_FEATURES_PDF BOOLEAN,
BR_FEATURES_FLASH BOOLEAN,
BR_FEATURES_JAVA BOOLEAN,
BR_FEATURES_DIRECTOR BOOLEAN,
BR_FEATURES_QUICKTIME BOOLEAN,
BR_FEATURES_REALPLAYER BOOLEAN,
BR_FEATURES_WINDOWSMEDIA BOOLEAN,
BR_FEATURES_GEARS BOOLEAN,
BR_FEATURES_SILVERLIGHT BOOLEAN,
BR_COOKIES BOOLEAN,
BR_COLORDEPTH TEXT,
BR_VIEWWIDTH NUMBER(38,0),
BR_VIEWHEIGHT NUMBER(38,0),
OS_NAME TEXT,
OS_FAMILY TEXT,
OS_MANUFACTURER TEXT,
OS_TIMEZONE TEXT,
DVCE_TYPE TEXT,
DVCE_ISMOBILE BOOLEAN,
DVCE_SCREENWIDTH NUMBER(38,0),
DVCE_SCREENHEIGHT NUMBER(38,0),
DOC_CHARSET TEXT,
DOC_WIDTH NUMBER(38,0),
DOC_HEIGHT NUMBER(38,0),
TR_CURRENCY TEXT,
TR_TOTAL_BASE NUMBER(18,2),
TR_TAX_BASE NUMBER(18,2),
TR_SHIPPING_BASE NUMBER(18,2),
TI_CURRENCY TEXT,
TI_PRICE_BASE NUMBER(18,2),
BASE_CURRENCY TEXT,
GEO_TIMEZONE TEXT,
MKT_CLICKID TEXT,
MKT_NETWORK TEXT,
ETL_TAGS TEXT,
DVCE_SENT_TSTAMP TIMESTAMP_NTZ(9),
REFR_DOMAIN_USERID TEXT,
REFR_DVCE_TSTAMP TIMESTAMP_NTZ(9),
DOMAIN_SESSIONID TEXT,
DERIVED_TSTAMP TIMESTAMP_NTZ(9),
EVENT_VENDOR TEXT,
EVENT_NAME TEXT,
EVENT_FORMAT TEXT,
EVENT_VERSION TEXT,
EVENT_FINGERPRINT TEXT,
TRUE_TSTAMP TIMESTAMP_NTZ(9),
LOAD_TSTAMP TIMESTAMP_NTZ(9),
CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_UA_PARSER_CONTEXT_1 TEXT,
CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_WEB_PAGE_1 TEXT,
CONTEXTS_COM_IAB_SNOWPLOW_SPIDERS_AND_ROBOTS_1 TEXT,
CONTEXTS_NL_BASJES_YAUAA_CONTEXT_1 TEXT
);"""

hits_query = """CREATE TABLE {database}.{schema}.{table}
(
    "WatchID" BIGINT NOT NULL,
    "JavaEnable" INT NOT NULL,
    "Title" TEXT NOT NULL,
    "GoodEvent" INT NOT NULL,
    "EventTime" TIMESTAMP NOT NULL,
    "EventDate" Date NOT NULL,
    "CounterID" INTEGER NOT NULL,
    "ClientIP" INTEGER NOT NULL,
    "RegionID" INTEGER NOT NULL,
    "UserID" BIGINT NOT NULL,
    "CounterClass" INT NOT NULL,
    "OS" INT NOT NULL,
    "UserAgent" INT NOT NULL,
    "URL" TEXT NOT NULL,
    "Referer" TEXT NOT NULL,
    "IsRefresh" INT NOT NULL,
    "RefererCategoryID" INT NOT NULL,
    "RefererRegionID" INTEGER NOT NULL,
    "URLCategoryID" INT NOT NULL,
    "URLRegionID" INTEGER NOT NULL,
    "ResolutionWidth" INT NOT NULL,
    "ResolutionHeight" INT NOT NULL,
    "ResolutionDepth" INT NOT NULL,
    "FlashMajor" INT NOT NULL,
    "FlashMinor" INT NOT NULL,
    "FlashMinor2" TEXT NOT NULL,
    "NetMajor" INT NOT NULL,
    "NetMinor" INT NOT NULL,
    "UserAgentMajor" INT NOT NULL,
    "UserAgentMinor" STRING NOT NULL,
    "CookieEnable" INT NOT NULL,
    "JavascriptEnable" INT NOT NULL,
    "IsMobile" INT NOT NULL,
    "MobilePhone" INT NOT NULL,
    "MobilePhoneModel" TEXT NOT NULL,
    "Params" TEXT NOT NULL,
    "IPNetworkID" INTEGER NOT NULL,
    "TraficSourceID" INT NOT NULL,
    "SearchEngineID" INT NOT NULL,
    "SearchPhrase" TEXT NOT NULL,
    "AdvEngineID" INT NOT NULL,
    "IsArtifical" INT NOT NULL,
    "WindowClientWidth" INT NOT NULL,
    "WindowClientHeight" INT NOT NULL,
    "ClientTimeZone" INT NOT NULL,
    "ClientEventTime" TIMESTAMP NOT NULL,
    "SilverlightVersion1" INT NOT NULL,
    "SilverlightVersion2" INT NOT NULL,
    "SilverlightVersion3" INTEGER NOT NULL,
    "SilverlightVersion4" INT NOT NULL,
    "PageCharset" TEXT NOT NULL,
    "CodeVersion" INTEGER NOT NULL,
    "IsLink" INT NOT NULL,
    "IsDownload" INT NOT NULL,
    "IsNotBounce" INT NOT NULL,
    "FUniqID" BIGINT NOT NULL,
    "OriginalURL" TEXT NOT NULL,
    "HID" INTEGER NOT NULL,
    "IsOldCounter" INT NOT NULL,
    "IsEvent" INT NOT NULL,
    "IsParameter" INT NOT NULL,
    "DontCountHits" INT NOT NULL,
    "WithHash" INT NOT NULL,
    "HitColor" STRING NOT NULL,
    "LocalEventTime" TIMESTAMP NOT NULL,
    "Age" INT NOT NULL,
    "Sex" INT NOT NULL,
    "Income" INT NOT NULL,
    "Interests" INT NOT NULL,
    "Robotness" INT NOT NULL,
    "RemoteIP" INTEGER NOT NULL,
    "WindowName" INTEGER NOT NULL,
    "OpenerName" INTEGER NOT NULL,
    "HistoryLength" INT NOT NULL,
    "BrowserLanguage" TEXT NOT NULL,
    "BrowserCountry" TEXT NOT NULL,
    "SocialNetwork" TEXT NOT NULL,
    "SocialAction" TEXT NOT NULL,
    "HTTPError" INT NOT NULL,
    "SendTiming" INTEGER NOT NULL,
    "DNSTiming" INTEGER NOT NULL,
    "ConnectTiming" INTEGER NOT NULL,
    "ResponseStartTiming" INTEGER NOT NULL,
    "ResponseEndTiming" INTEGER NOT NULL,
    "FetchTiming" INTEGER NOT NULL,
    "SocialSourceNetworkID" INT NOT NULL,
    "SocialSourcePage" TEXT NOT NULL,
    "ParamPrice" BIGINT NOT NULL,
    "ParamOrderID" TEXT NOT NULL,
    "ParamCurrency" TEXT NOT NULL,
    "ParamCurrencyID" INT NOT NULL,
    "OpenstatServiceName" TEXT NOT NULL,
    "OpenstatCampaignID" TEXT NOT NULL,
    "OpenstatAdID" TEXT NOT NULL,
    "OpenstatSourceID" TEXT NOT NULL,
    "UTMSource" TEXT NOT NULL,
    "UTMMedium" TEXT NOT NULL,
    "UTMCampaign" TEXT NOT NULL,
    "UTMContent" TEXT NOT NULL,
    "UTMTerm" TEXT NOT NULL,
    "FromTag" TEXT NOT NULL,
    "HasGCLID" INT NOT NULL,
    "RefererHash" BIGINT NOT NULL,
    "URLHash" BIGINT NOT NULL,
    "CLID" INTEGER NOT NULL
);"""


def upload(database, schema, table, filename):
    # UPLOAD DATA
    # Read and process the CSV file
    with open(filename, mode="r", newline="") as infile, tempfile.NamedTemporaryFile(
        mode="w+", newline="", suffix=".csv", delete=False
    ) as tmpfile:
        reader = csv.DictReader(infile)
        data = [
            {k: v.replace('"', "").replace("'", '"') for k, v in row.items()}
            for row in reader
        ]
        writer = csv.DictWriter(tmpfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(data)
        tmpfile.flush()
        tmp_filename = os.path.basename(tmpfile.name)

        # Print header and first row

        # Prepare file for upload from the temporary file
        file_name = tmp_filename
        files = [("uploadFile", (file_name, open(tmpfile.name, "rb"), "text/csv"))]
        response = requests.post(
            f"{url}/ui/databases/{database}/schemas/{schema}/tables/{table}/rows",
            files=files,
            params={
                "header": "true",
            },
        )
        response.raise_for_status()


def create_table(database, schema, table, query):
    import snowflake.connector

    USER = os.getenv("SNOWFLAKE_USER", "xxx")
    PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "yyy")
    ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "acc")
    DATABASE = os.getenv("SNOWFLAKE_DATABASE", database)
    SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", schema)
    WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "")

    con = snowflake.connector.connect(
        host=os.getenv("SNOWFLAKE_HOST", "localhost"),
        port=os.getenv("SNOWFLAKE_PORT", 3000),
        protocol=os.getenv("SNOWFLAKE_PROTOCOL", "http"),
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        session_parameters={
            "QUERY_TAG": "dbt-testing",
        },
    )

    cursor = con.cursor()
    cursor.execute(query)


if __name__ == "__main__":
    bootstrap(database, schema)

    for table, q, filename in [
        # ("hits2", hits_query, "hits.parquet"),
        ("events_iceberg", web_table, "Web_Analytics_sample_events.csv"),
        # ("events_mobile", mobile_query, "Mobile_sample_events.csv"),
    ]:
        create_table(
            database,
            schema,
            table,
            q.format(database=database, schema=schema, table=table),
        )
        upload(database, schema, table, filename)
