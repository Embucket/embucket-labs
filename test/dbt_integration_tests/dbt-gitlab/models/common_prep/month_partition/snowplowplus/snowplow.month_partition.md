{% docs snowplow_gitlab_events %}
This is the base table for snowplow events captured by GitLab's infrastructure. The data is written to S3 as a TSV, so this model simply reorders the columns and unpacks the unstructured events.

The query mainly selects some columns and some basic transformations. The column `web_page_id` requires some specific work. This `web_page_id` is contained in the contexts column. The contexts column is a JSON object that looks like this:

```
{
  "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
  "data": [
    {
      "schema": "iglu:com.gitlab/gitlab_standard/jsonschema/1-0-3",
      "data": {
        "environment": "production",
        "source": "gitlab-javascript"
      }
    },
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0",
      "data": {
        "id": "58797d16-270e-4d82-9904-81b541b97866"
      }
    },
    {
      "schema": "iglu:org.w3/PerformanceTiming/jsonschema/1-0-0",
      "data": {
        "navigationStart": 1613824527074,
        "unloadEventStart": 1613824528821,
        "unloadEventEnd": 1613824528821,
        "redirectStart": 1613824527089,
        "redirectEnd": 1613824528047,
        "fetchStart": 1613824528047,
        "domainLookupStart": 1613824528047,
        "domainLookupEnd": 1613824528047,
        "connectStart": 1613824528047,
        "connectEnd": 1613824528047,
        "secureConnectionStart": 0,
        "requestStart": 1613824528055,
        "responseStart": 1613824528793,
        "responseEnd": 1613824528800,
        "domLoading": 1613824528833,
        "domInteractive": 1613824529081,
        "domContentLoadedEventStart": 1613824529511,
        "domContentLoadedEventEnd": 1613824529526,
        "domComplete": 1613824529695,
        "loadEventStart": 1613824529695,
        "loadEventEnd": 1613824529695
      }
    }
  ]
}
```

The context JSON we are interested in is the `web_page` context:

```
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0",
      "data": {
        "id": "58797d16-270e-4d82-9904-81b541b97866"
      }
```


We then need to extract the `id` from the data blob to retrieve the `web_page_id`.

{% enddocs %}
