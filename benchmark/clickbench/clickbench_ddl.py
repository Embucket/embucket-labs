import os

from .clickbench_table_names import parametrize_clickbench_statements

# ClickBench DDL statement with parametrized table name
_CLICKBENCH_DDL_RAW = [
    (
        "hits",
        """
        -- Snowflake-like DDL for ClickBench hits table
        CREATE OR REPLACE TABLE {HITS_TABLE} (
          WatchID BIGINT,
          JavaEnable SMALLINT,
          Title VARCHAR,
          GoodEvent SMALLINT,
          EventTime BIGINT,
          EventDate SMALLINT,
          CounterID INTEGER,
          ClientIP INTEGER,
          RegionID INTEGER,
          UserID BIGINT,
          CounterClass SMALLINT,
          OS SMALLINT,
          UserAgent SMALLINT,
          URL VARCHAR,
          Referer VARCHAR,
          IsRefresh SMALLINT,
          RefererCategoryID SMALLINT,
          RefererRegionID INTEGER,
          URLCategoryID SMALLINT,
          URLRegionID INTEGER,
          ResolutionWidth SMALLINT,
          ResolutionHeight SMALLINT,
          ResolutionDepth SMALLINT,
          FlashMajor SMALLINT,
          FlashMinor SMALLINT,
          FlashMinor2 VARCHAR,
          NetMajor SMALLINT,
          NetMinor SMALLINT,
          UserAgentMajor SMALLINT,
          UserAgentMinor VARCHAR,
          CookieEnable SMALLINT,
          JavascriptEnable SMALLINT,
          IsMobile SMALLINT,
          MobilePhone SMALLINT,
          MobilePhoneModel VARCHAR,
          Params VARCHAR,
          IPNetworkID INTEGER,
          TraficSourceID SMALLINT,
          SearchEngineID SMALLINT,
          SearchPhrase VARCHAR,
          AdvEngineID SMALLINT,
          IsArtifical SMALLINT,
          WindowClientWidth SMALLINT,
          WindowClientHeight SMALLINT,
          ClientTimeZone SMALLINT,
          ClientEventTime BIGINT,
          SilverlightVersion1 SMALLINT,
          SilverlightVersion2 SMALLINT,
          SilverlightVersion3 INTEGER,
          SilverlightVersion4 SMALLINT,
          PageCharset VARCHAR,
          CodeVersion INTEGER,
          IsLink SMALLINT,
          IsDownload SMALLINT,
          IsNotBounce SMALLINT,
          FUniqID BIGINT,
          OriginalURL VARCHAR,
          HID INTEGER,
          IsOldCounter SMALLINT,
          IsEvent SMALLINT,
          IsParameter SMALLINT,
          DontCountHits SMALLINT,
          WithHash SMALLINT,
          HitColor VARCHAR,
          LocalEventTime BIGINT,
          Age SMALLINT,
          Sex SMALLINT,
          Income SMALLINT,
          Interests SMALLINT,
          Robotness SMALLINT,
          RemoteIP INTEGER,
          WindowName INTEGER,
          OpenerName INTEGER,
          HistoryLength SMALLINT,
          BrowserLanguage VARCHAR,
          BrowserCountry VARCHAR,
          SocialNetwork VARCHAR,
          SocialAction VARCHAR,
          HTTPError SMALLINT,
          SendTiming INTEGER,
          DNSTiming INTEGER,
          ConnectTiming INTEGER,
          ResponseStartTiming INTEGER,
          ResponseEndTiming INTEGER,
          FetchTiming INTEGER,
          SocialSourceNetworkID SMALLINT,
          SocialSourcePage VARCHAR,
          ParamPrice BIGINT,
          ParamOrderID VARCHAR,
          ParamCurrency VARCHAR,
          ParamCurrencyID SMALLINT,
          OpenstatServiceName VARCHAR,
          OpenstatCampaignID VARCHAR,
          OpenstatAdID VARCHAR,
          OpenstatSourceID VARCHAR,
          UTMSource VARCHAR,
          UTMMedium VARCHAR,
          UTMCampaign VARCHAR,
          UTMContent VARCHAR,
          UTMTerm VARCHAR,
          FromTag VARCHAR,
          HasGCLID SMALLINT,
          RefererHash BIGINT,
          URLHash BIGINT,
          CLID INTEGER
        );
        """
    ),
]


def parametrize_clickbench_ddl(fully_qualified_names_for_embucket):
    """
    Replace table name placeholders in ClickBench DDL statements with actual table names.

    Args:
        fully_qualified_names_for_embucket (bool): Required. If True, use EMBUCKET_DATABASE.EMBUCKET_SCHEMA.tablename format.
                                                   If False, use just the default table names.

    Returns:
        list: A list of (table_name, parametrized_ddl) tuples.
    """
    return parametrize_clickbench_statements(_CLICKBENCH_DDL_RAW, fully_qualified_names_for_embucket)
