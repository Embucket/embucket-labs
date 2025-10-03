import os

from .clickbench_table_names import parametrize_clickbench_statements

# ClickBench DDL statements with replacements compared to Snowflake syntax (https://github.com/ClickHouse/ClickBench/blob/main/snowflake/create.sql)
_CLICKBENCH_DDL_RAW = [
    (
        "hits",
        """
        -- Snowflake-like DDL for ClickBench hits table
            CREATE OR REPLACE TABLE {HITS_TABLE}
    (
        WatchID BIGINT NOT NULL,
        JavaEnable SMALLINT NOT NULL,
        Title VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        GoodEvent SMALLINT NOT NULL,
        EventTime TIMESTAMP NOT NULL,
        EventDate Date NOT NULL,
        CounterID INTEGER NOT NULL,
        ClientIP INTEGER NOT NULL,
        RegionID INTEGER NOT NULL,
        UserID BIGINT NOT NULL,
        CounterClass SMALLINT NOT NULL,
        OS SMALLINT NOT NULL,
        UserAgent SMALLINT NOT NULL,
        URL VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        Referer VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        IsRefresh SMALLINT NOT NULL,
        RefererCategoryID SMALLINT NOT NULL,
        RefererRegionID INTEGER NOT NULL,
        URLCategoryID SMALLINT NOT NULL,
        URLRegionID INTEGER NOT NULL,
        ResolutionWidth SMALLINT NOT NULL,
        ResolutionHeight SMALLINT NOT NULL,
        ResolutionDepth SMALLINT NOT NULL,
        FlashMajor SMALLINT NOT NULL,
        FlashMinor SMALLINT NOT NULL,
        FlashMinor2 VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        NetMajor SMALLINT NOT NULL,
        NetMinor SMALLINT NOT NULL,
        UserAgentMajor SMALLINT NOT NULL,
        UserAgentMinor VARCHAR(255) NOT NULL,
        CookieEnable SMALLINT NOT NULL,
        JavascriptEnable SMALLINT NOT NULL,
        IsMobile SMALLINT NOT NULL,
        MobilePhone SMALLINT NOT NULL,
        MobilePhoneModel VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        Params VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        IPNetworkID INTEGER NOT NULL,
        TraficSourceID SMALLINT NOT NULL,
        SearchEngineID SMALLINT NOT NULL,
        SearchPhrase VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        AdvEngineID SMALLINT NOT NULL,
        IsArtifical SMALLINT NOT NULL,
        WindowClientWidth SMALLINT NOT NULL,
        WindowClientHeight SMALLINT NOT NULL,
        ClientTimeZone SMALLINT NOT NULL,
        ClientEventTime TIMESTAMP NOT NULL,
        SilverlightVersion1 SMALLINT NOT NULL,
        SilverlightVersion2 SMALLINT NOT NULL,
        SilverlightVersion3 INTEGER NOT NULL,
        SilverlightVersion4 SMALLINT NOT NULL,
        PageCharset VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        CodeVersion INTEGER NOT NULL,
        IsLink SMALLINT NOT NULL,
        IsDownload SMALLINT NOT NULL,
        IsNotBounce SMALLINT NOT NULL,
        FUniqID BIGINT NOT NULL,
        OriginalURL VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        HID INTEGER NOT NULL,
        IsOldCounter SMALLINT NOT NULL,
        IsEvent SMALLINT NOT NULL,
        IsParameter SMALLINT NOT NULL,
        DontCountHits SMALLINT NOT NULL,
        WithHash SMALLINT NOT NULL,
        HitColor CHAR NOT NULL,
        LocalEventTime TIMESTAMP NOT NULL,
        Age SMALLINT NOT NULL,
        Sex SMALLINT NOT NULL,
        Income SMALLINT NOT NULL,
        Interests SMALLINT NOT NULL,
        Robotness SMALLINT NOT NULL,
        RemoteIP INTEGER NOT NULL,
        WindowName INTEGER NOT NULL,
        OpenerName INTEGER NOT NULL,
        HistoryLength SMALLINT NOT NULL,
        BrowserLanguage VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        BrowserCountry VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        SocialNetwork VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        SocialAction VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        HTTPError SMALLINT NOT NULL,
        SendTiming INTEGER NOT NULL,
        DNSTiming INTEGER NOT NULL,
        ConnectTiming INTEGER NOT NULL,
        ResponseStartTiming INTEGER NOT NULL,
        ResponseEndTiming INTEGER NOT NULL,
        FetchTiming INTEGER NOT NULL,
        SocialSourceNetworkID SMALLINT NOT NULL,
        SocialSourcePage VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        ParamPrice BIGINT NOT NULL,
        ParamOrderID VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        ParamCurrency VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        ParamCurrencyID SMALLINT NOT NULL,
        OpenstatServiceName VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        OpenstatCampaignID VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        OpenstatAdID VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        OpenstatSourceID VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        UTMSource VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        UTMMedium VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        UTMCampaign VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        UTMContent VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        UTMTerm VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        FromTag VARCHAR,       -- TEXT NOT NULL replaced by VARCHAR
        HasGCLID SMALLINT NOT NULL,
        RefererHash BIGINT NOT NULL,
        URLHash BIGINT NOT NULL,
        CLID INTEGER NOT NULL,
        PRIMARY KEY (CounterID, EventDate, UserID, EventTime, WatchID)
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
