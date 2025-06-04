{% docs data_detection_rule %}

A table to hold all the data quality detection rules. 

The data quality detection rules need to be manually added to the table.

{% enddocs %}

{% docs product_data_detection_run_detail %}

A table with all the run details of the data quality detection rules. 

This is an incremental model that contains the total number of processed records, passed records and failed records based on the data detection rules.

{% enddocs %}

{% docs product_data_detection_run_result %}

A table that holds the run results of data quality detection rules. 

This table includes a Flag to indicate if the Detection rule passed or failed based on the threshold value.

{% enddocs %}

{% docs data_detection_scorecard %}

A table that holds the name along with the purpose and release information of all the available Scorcards for different subject areas. 

{% enddocs %}

