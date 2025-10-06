import os
import re

from .clickbench_table_names import parametrize_clickbench_statements

# Original ClickBench queries with parametrized table names
_CLICKBENCH_QUERIES_RAW = [
    ("clickbench-q1", "SELECT COUNT(*) FROM {HITS_TABLE}"),
    ("clickbench-q2", "SELECT COUNT(*) FROM {HITS_TABLE} WHERE AdvEngineID <> 0"),
    ("clickbench-q3", "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM {HITS_TABLE}"),
    ("clickbench-q4", "SELECT AVG(UserID) FROM {HITS_TABLE}"),
    ("clickbench-q5", "SELECT COUNT(DISTINCT UserID) FROM {HITS_TABLE}"),
    ("clickbench-q6", "SELECT COUNT(DISTINCT SearchPhrase) FROM {HITS_TABLE}"),
    ("clickbench-q7", "SELECT MIN(EventDate), MAX(EventDate) FROM {HITS_TABLE}"),
    ("clickbench-q8", "SELECT AdvEngineID, COUNT(*) FROM {HITS_TABLE} WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC"),
    ("clickbench-q9", "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM {HITS_TABLE} GROUP BY RegionID ORDER BY u DESC LIMIT 10"),
    ("clickbench-q10", "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM {HITS_TABLE} GROUP BY RegionID ORDER BY c DESC LIMIT 10"),
    ("clickbench-q11", "SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM {HITS_TABLE} WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10"),
    ("clickbench-q12", "SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM {HITS_TABLE} WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10"),
    ("clickbench-q13", "SELECT SearchPhrase, COUNT(*) AS c FROM {HITS_TABLE} WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10"),
    ("clickbench-q14", "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM {HITS_TABLE} WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10"),
    ("clickbench-q15", "SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM {HITS_TABLE} WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10"),
    ("clickbench-q16", "SELECT UserID, COUNT(*) FROM {HITS_TABLE} GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10"),
    ("clickbench-q17", "SELECT UserID, SearchPhrase, COUNT(*) FROM {HITS_TABLE} GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10"),
    ("clickbench-q18", "SELECT UserID, SearchPhrase, COUNT(*) FROM {HITS_TABLE} GROUP BY UserID, SearchPhrase LIMIT 10"),
    ("clickbench-q19", "SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM {HITS_TABLE} GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10"),
    ("clickbench-q20", "SELECT UserID FROM {HITS_TABLE} WHERE UserID = 435090932899640449"),
    ("clickbench-q21", "SELECT COUNT(*) FROM {HITS_TABLE} WHERE URL LIKE '%google%'"),
    ("clickbench-q22", "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM {HITS_TABLE} WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10"),
    ("clickbench-q23", "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM {HITS_TABLE} WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10"),
    ("clickbench-q24", "SELECT * FROM {HITS_TABLE} WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10"),
    ("clickbench-q25", "SELECT SearchPhrase FROM {HITS_TABLE} WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10"),
    ("clickbench-q26", "SELECT SearchPhrase FROM {HITS_TABLE} WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10"),
    ("clickbench-q27", "SELECT SearchPhrase FROM {HITS_TABLE} WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10"),
    ("clickbench-q28", "SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM {HITS_TABLE} WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25"),
    ("clickbench-q29", "SELECT REGEXP_REPLACE(Referer, '^https?://(www\\.)?([^/]+)/.*$', '\\2') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM {HITS_TABLE} WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25"),
    ("clickbench-q30", "SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(ResolutionWidth + 3), SUM(ResolutionWidth + 4), SUM(ResolutionWidth + 5), SUM(ResolutionWidth + 6), SUM(ResolutionWidth + 7), SUM(ResolutionWidth + 8), SUM(ResolutionWidth + 9), SUM(ResolutionWidth + 10), SUM(ResolutionWidth + 11), SUM(ResolutionWidth + 12), SUM(ResolutionWidth + 13), SUM(ResolutionWidth + 14), SUM(ResolutionWidth + 15), SUM(ResolutionWidth + 16), SUM(ResolutionWidth + 17), SUM(ResolutionWidth + 18), SUM(ResolutionWidth + 19), SUM(ResolutionWidth + 20), SUM(ResolutionWidth + 21), SUM(ResolutionWidth + 22), SUM(ResolutionWidth + 23), SUM(ResolutionWidth + 24), SUM(ResolutionWidth + 25), SUM(ResolutionWidth + 26), SUM(ResolutionWidth + 27), SUM(ResolutionWidth + 28), SUM(ResolutionWidth + 29), SUM(ResolutionWidth + 30), SUM(ResolutionWidth + 31), SUM(ResolutionWidth + 32), SUM(ResolutionWidth + 33), SUM(ResolutionWidth + 34), SUM(ResolutionWidth + 35), SUM(ResolutionWidth + 36), SUM(ResolutionWidth + 37), SUM(ResolutionWidth + 38), SUM(ResolutionWidth + 39), SUM(ResolutionWidth + 40), SUM(ResolutionWidth + 41), SUM(ResolutionWidth + 42), SUM(ResolutionWidth + 43), SUM(ResolutionWidth + 44), SUM(ResolutionWidth + 45), SUM(ResolutionWidth + 46), SUM(ResolutionWidth + 47), SUM(ResolutionWidth + 48), SUM(ResolutionWidth + 49), SUM(ResolutionWidth + 50), SUM(ResolutionWidth + 51), SUM(ResolutionWidth + 52), SUM(ResolutionWidth + 53), SUM(ResolutionWidth + 54), SUM(ResolutionWidth + 55), SUM(ResolutionWidth + 56), SUM(ResolutionWidth + 57), SUM(ResolutionWidth + 58), SUM(ResolutionWidth + 59), SUM(ResolutionWidth + 60), SUM(ResolutionWidth + 61), SUM(ResolutionWidth + 62), SUM(ResolutionWidth + 63), SUM(ResolutionWidth + 64), SUM(ResolutionWidth + 65), SUM(ResolutionWidth + 66), SUM(ResolutionWidth + 67), SUM(ResolutionWidth + 68), SUM(ResolutionWidth + 69), SUM(ResolutionWidth + 70), SUM(ResolutionWidth + 71), SUM(ResolutionWidth + 72), SUM(ResolutionWidth + 73), SUM(ResolutionWidth + 74), SUM(ResolutionWidth + 75), SUM(ResolutionWidth + 76), SUM(ResolutionWidth + 77), SUM(ResolutionWidth + 78), SUM(ResolutionWidth + 79), SUM(ResolutionWidth + 80), SUM(ResolutionWidth + 81), SUM(ResolutionWidth + 82), SUM(ResolutionWidth + 83), SUM(ResolutionWidth + 84), SUM(ResolutionWidth + 85), SUM(ResolutionWidth + 86), SUM(ResolutionWidth + 87), SUM(ResolutionWidth + 88), SUM(ResolutionWidth + 89) FROM {HITS_TABLE}"),
    ("clickbench-q31", "SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM {HITS_TABLE} WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10"),
    ("clickbench-q32", "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM {HITS_TABLE} WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10"),
    ("clickbench-q33", "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM {HITS_TABLE} GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10"),
    ("clickbench-q34", "SELECT URL, COUNT(*) AS c FROM {HITS_TABLE} GROUP BY URL ORDER BY c DESC LIMIT 10"),
    ("clickbench-q35", "SELECT 1, URL, COUNT(*) AS c FROM {HITS_TABLE} GROUP BY 1, URL ORDER BY c DESC LIMIT 10"),
    ("clickbench-q36", "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM {HITS_TABLE} GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10"),
    ("clickbench-q37", "SELECT URL, COUNT(*) AS PageViews FROM {HITS_TABLE} WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10"),
    ("clickbench-q38", "SELECT Title, COUNT(*) AS PageViews FROM {HITS_TABLE} WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10"),
    ("clickbench-q39", "SELECT URL, COUNT(*) AS PageViews FROM {HITS_TABLE} WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000"),
    ("clickbench-q40", "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM {HITS_TABLE} WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000"),
    ("clickbench-q41", "SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM {HITS_TABLE} WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100"),
    ("clickbench-q42", "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM {HITS_TABLE} WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000"),
    ("clickbench-q43", "SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM {HITS_TABLE} WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000"),
]


def parametrize_clickbench_queries(fully_qualified_names_for_embucket):
    """
    Replace table name placeholders in ClickBench queries with actual table names.

    Args:
        fully_qualified_names_for_embucket (bool): Required. If True, use EMBUCKET_DATABASE.EMBUCKET_SCHEMA.tablename format.
                                                   If False, use just the default table names.

    Returns:
        list: A list of (query_name, parametrized_query) tuples.
    """
    return parametrize_clickbench_statements(_CLICKBENCH_QUERIES_RAW, fully_qualified_names_for_embucket)
