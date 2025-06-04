{% docs email_domain_type %}

This macro classifies email_domains in 4 buckets:

1. **Bulk load or list purchase or spam impacted**: Based on the lead_source.
1. **Personal email domain**: Based on full and partial matching using a list of personal email domains returned by the get_personal_email_domain_list macro.
1. **Business email domain**: All non NULL email_domains that do not classify as Bulk or Personal.
1. **Missing email domain**: Empty email_domains.

{% enddocs %}

{% docs it_job_title_hierarchy %}

This macro maps a job title to the IT job title hierarchy. It works by doing string matching on the job title and categorizing them into 3 buckets:

1. **IT Decision Makers**: CIO, CTO, VP of IT, ...
2. **IT Managers**: Manager of IT. Manager of Procurement, ...
3. **IT Individual contributors**: Software Developer, Application Developer, IT programmer, ...

These buckets are only for IT, information systems, engineering, ... Everything else gets a NULL value assigned to it.

This macro uses the pad_column macro to "pad" the job title field with spaces and discard unrelated pattern matching.

An example of this is the matching for the job title of `IT Manager`. The string pattern for it `%it%manager%` also gets unrelated matches like `Junior Digital Project Manager` or `Supplier Quality Section Manager`. To overcome this problem, the job title field is "padded" with spaces to the both sides of the string and the string pattern changed `% it%manager%`. This way the previous unrelated job titles would not match.

{% enddocs %}

{% docs map_marketing_channel_path %}
This macro maps channel path to the marketing channel name.
{% enddocs %}

{% docs monthly_all_time_metric_calc %}

This macro is used to create a monthly metric value for all-time service ping metrics. It does this by partitioning on the dim_installation_id and metrics path, ordering the partition by ping created at timestamp, and fetching the prior month all-time metric value using a LAG function. The macro then subtracts the 2 numbers to get a monthly metric value.

{% enddocs %}

{% docs partner_category %}

Creates a partner category from Sales Qualified Source and Resale Partner Name to be used in Sales Funnel reporting.

{% enddocs %}

{% docs macro_prep_snowplow_unnested_events_all %}

This macro is used for transforming and processing Snowplow event data. It takes a unioned view of unnested Snowplow event data as input and performs additional transformations or operations on that data.

The primary purpose of this macro is to provide flexibility in unioning the required number of monthly partitions of Snowplow event data and applying the same transformation logic to the unioned data. This approach avoids duplication of transformation logic across different models that may require different date ranges or partitions of data.

For example, the `prep_snowplow_unnested_events_all` model requires 800 days of data, which could be covered by unioning multiple monthly partitions. In contrast, the `prep_snowplow_unnested_events_all_30` model only requires 30 days of data, necessitating the union of a smaller number of monthly partitions. However, both models can leverage the `macro_prep_snowplow_unnested_events_all` macro to apply the same transformation logic to their respective unioned data sets.

{% enddocs %}

{% docs utm_campaign_parsing %}

Parses the new Marketing utm_campaign parameter into its consituent pieces, to be used in marketing reporting.

{% enddocs %}

{% docs utm_content_parsing %}

Parses the new Marketing utm_content parameter into its consituent pieces, to be used in marketing reporting.

{% enddocs %}
