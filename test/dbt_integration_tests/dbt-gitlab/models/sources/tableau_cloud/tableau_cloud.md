{% docs tableau_cloud_events %}
TS Events functions as a primary audit data source. It contains data about various events happening on your site, including sign-ins, publishes, and accessed views.
Each row of data correlates with a single event that occurs on the Tableau Online site.
For detailed column descriptions see the linked datasource
[Datasource](https://10az.online.tableau.com/#/site/gitlab/datasources/51236814)
{% enddocs %}

{% docs tableau_cloud_groups %}
Groups identifies the group membership of users. Groups without members, and users not in a group, will be included as a row of data with null values represented as “NULL“.
There is one row of data for each unique combination of group and user pairing.
For detailed column descriptions see the linked datasource
[Datasource](https://10az.online.tableau.com/#/site/gitlab/datasources/51250281)
{% enddocs %}

{% docs tableau_cloud_permissions %}
Permissions contains the effective permissions for all users and content on the site. 
Each row of data corresponds to a combination of user, content item, and permission capability. 
For detailed column descriptions see the linked datasource
[Datasource](https://10az.online.tableau.com/#/site/gitlab/datasources/51246803)
{% enddocs %}

{% docs tableau_cloud_site_content %}
Site Content provides essential governance information about the projects, data sources, workbooks, and views on a site. The data provided about a content item may be unique to its item type. Item types with unique fields are in folders with titles that correspond to their Item Type.
Each row of data corresponds to a unique content item. Item LUID is the unique identifier for content items on a Tableau Online pod.
For detailed column descriptions see the linked datasource
[Datasource](https://10az.online.tableau.com/#/site/gitlab/datasources/51244312)
{% enddocs %}

{% docs tableau_cloud_users %}
TS Users contains data about your users such as remaining licenses, site roles, and content ownership metrics.
Each row of data corresponds to a unique User on your Tableau Online site.
For detailed column descriptions see the linked datasource
[Datasource](https://10az.online.tableau.com/#/site/gitlab/datasources/51240750)
{% enddocs %}
