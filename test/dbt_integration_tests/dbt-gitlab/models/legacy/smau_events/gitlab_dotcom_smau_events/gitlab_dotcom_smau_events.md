{% docs create_gitlab_dotcom_smau_events %}

This model provides a summary of relevant activation events for Create Stage coming from gitlab_dotcom database. A summary of all activation events is at the moment defined in this issue.

From gitlab_dotcom database, at the moment we track the following events:

* comment on a snippet
* comment on a merge request
* open a merge request 

{% enddocs %}


{% docs manage_gitlab_dotcom_smau_events %}

This model provides a summary of relevant actions for the Manage Stage coming from gitlab_dotcom database.

We currently include track the following events:

* Project Creation
* User Creation

{% enddocs %}

