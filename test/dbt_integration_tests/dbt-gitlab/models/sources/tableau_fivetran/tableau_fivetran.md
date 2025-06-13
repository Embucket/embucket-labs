{% docs tableau_fivetran_workbook %}
The tableau_fivetran_workbook represents Tableau workbooks synced via Fivetran. It contains information about individual Tableau workbooks, including metadata such as workbook name, owner, project, creation date, and last update date.
{% enddocs %}

{% docs tableau_fivetran_data_source %}
The tableau_fivetran_data_source contains information about data sources used in Tableau workbooks, synced via Fivetran. This includes details such as the data source name, type, connection information, and associated workbooks or projects.
{% enddocs %}

{% docs tableau_fivetran_project %}
The tableau_fivetran_project represents Tableau projects synced via Fivetran. It includes information about project structure, such as project name, description, parent project (if applicable), and associated permissions or user groups.
{% enddocs %}

{% docs tableau_fivetran_extract_refresh_task %}
The tableau_fivetran_extract_refresh_task contains data about extract refresh tasks in Tableau, synced via Fivetran. This includes information such as the associated workbook or data source, refresh schedule, last run time, and status of the refresh task.
{% enddocs %}