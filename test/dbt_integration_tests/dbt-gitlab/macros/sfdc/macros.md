{% docs growth_type%}
This macro buckets Order Type and ARR Basis into four different categories: Add-On Growth, Growth on Renewal, Contraction on Renewal, Lost on Renewal.
{% enddocs %}

{% docs sales_hierarchy_sales_segment_cleaning%}
This macro is a CASE WHEN statement that applies formatting to the sales hierarchy sales segment taken from both the user and opportunity objects to form the live and stamped sales hierarchy.
{% enddocs %}

{% docs sales_qualified_source_cleaning %}
This macro applies correct mapping to the sales_qualified_source field.
* BDR -> SDR
* Channel Generated -> Partner Generated
{% enddocs %}

{% docs sales_segment_cleaning %}
This macro applies proper formatting to sales segment data with the end result being one of SMB, Mid-Market, Strategic, Large or Unknown.
{% enddocs %}

{% docs sfdc_account_fields%}
This macro stores all of the shared logic between the live and snapshot crm account models. It takes two values for the model_type variable ('snapshot', 'live') to create either the snapshot or live view of a model from the `source` models. Whenever a new field is added to the live `_source` model, it will need to be added to the `_snapshots_source` model as well to maintain continuity between the live and snapshot models.
{% enddocs %}

{% docs sfdc_deal_size %}
This macro buckets a unit into a deal size (Small, Medium, Big, or Jumbo) based on an inputted value.
{% enddocs %}

{% docs sfdc_source_buckets %}
This macro is a CASE WHEN statement that groups the lead sources into new marketing-defined buckets. @rkohnke is the DRI on any changes made to this macro.
{% enddocs %}

{% docs sfdc_user_fields%}
This macro stores all of the shared logic between the live and snapshot crm user models. It takes two values for the model_type variable ('snapshot', 'live') to create either the snapshot or live view of a model from the `source` models. Whenever a new field is added to the live `_source` model, it will need to be added to the `_snapshots_source` model as well to maintain continuity between the live and snapshot models.
{% enddocs %}

{% docs sales_funnel_text_slugify %}
This macro is a removes spaces, special characters, and capital letters from text fields taken from a sheetload filled out by the Finance team to complete the sales funnel targets.
{% enddocs %}

{% docs sales_qualified_source_grouped %}
This macro overwrites the sales qualified source to standardize on current business terminology and groups other sales qualifed sources into a common category.
* BDR -> SDR
* Channel Generated -> Partner Generated
* LIKE ANY ('Web%', 'Missing%', 'Other') -> Web Direct Generated
{% enddocs %}

{% docs sqs_bucket_engagement %}
This macro groups opportunities into partner generated or co-sell based on the sales qualified source.
* If Partner Generated -> Partner Sourced
* If not Partner Generated -> 'Co-sell'
{% enddocs %}

{% docs deal_path_cleaning %}
This macro applies correct mapping to the deal_path field based on current business terminology.
* If Channel -> Partner
{% enddocs %}


{% docs integrated_budget_holder %}
This macro applies correct mapping to the campaign budget_holder field based on current business terminology, it combines data from online and offline touchpoints. See issue https://gitlab.com/gitlab-com/marketing/marketing-strategy-performance/-/issues/1473 and this MR https://gitlab.com/gitlab-data/analytics/-/merge_requests/11520#note_2310568015 for mapping.
{% enddocs %}


{% docs date_spine_7th_day %}
This macro generates a date spine that provides weekly snapshots (every 7 days) throughout a quarter, extending up to day 84. For the final week of the quarter, the duration varies: it can span 8 days for Q2, Q3, and Q4, while for Q1, it may be 5 or 6 days, depending on the year.
{% enddocs %}


{% docs partner_marketing_task_subject_cleaning %}
Standardizes the cleaning for task/event cleaning to strip out PII from the task/event subject fields, leaving only SAFE data.
{% enddocs %}


{% docs mart_crm_opportunity_snapshot_fields %}
This macro manages all the fields that need to be included in the opportunity snapshot marts (daily snapshot and 7th day weekly snapshot). This ensures parity between the two models at all times.
fct_table parameter is required to determine the name of the fact table it should pull the fields from.
{% enddocs %}

{% docs bizible_touchpoint_offer_type %}
This macro groups various attributes of Bizible Touchpoints into specific offer type groupings. 
{% enddocs %}

{% docs mart_arr_with_zero_dollar_charges_source %}
This macro manages the source creation of the models mart_arr_with_zero_dollar_charges and mart_arr_weekly_with_zero_dollar_charges
{% enddocs %}

{% docs mart_arr_with_zero_dollar_charges_fields %}
This macro manages all the fields that are to be included in the mrr marts (monthly and weekly). This ensures that any columns that are in mart_arr_with_zero_dollar_charges should be kept up to date in mart_arr_weekly_with_zero_dollar_charges as well.
The fct_table parameter is required to determine the name of the fact table it should pull the fields from.
{% enddocs %}