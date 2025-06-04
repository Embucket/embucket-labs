{% docs pump_marketing_contact %}

A copy of mart_marketing_contact for sending to Marketo for use in email campaigns. New user cohorts should be added by creating a flag in mart_marketing_contact and then adding the flag to this pump_marketing_contact model.

User Cohorts Included:

PQL Users via is_pql = TRUE flag in mart_marketing_contact.

{% enddocs %}

{% docs pump_subscription_product_usage %}

A copy of `subscription_product_usage_data` model for sending to Salesforce

{% enddocs %}

{% docs pump_disaster_relief_fund %}
This table pulls data from the employee directory and populated a list of employee hires and terminations to be used by an external partner to validate request for disaster relief funds.

{% enddocs %}

{% docs pump_daily_data_science_scores %}

This table contains PtE & PtC predicted scores that will automatically uploaded to Salesforce via Openprise.

{% enddocs %}

{% docs pump_churn_forecasting_scores %}

This table contains Churn Forecasting predicted scores that will be automatically uploaded to Salesforce via Openprise.

{% enddocs %}

{% docs pump_opportunity_forecasting_scores %}

This table contains Opportunity Forecasting predicted scores that will be automatically uploaded to Salesforce via Openprise.

{% enddocs %}

{% docs pump_smb_daily_case_automation %}

This table contains SMB event-based trigger data that will be automatically uploaded to Salesforce to create cases 

{% enddocs %}