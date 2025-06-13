{% docs handbook_merge_requests %}

This data is gathered by hitting the GitLab API for [project 7764](https://gitlab.com/gitlab-com/www-gitlab-com/), which includes the handbook.  

All merge request data is pulled for this project, and becomes part of the source for handbook_merge_requests.

The data is filtered to only include those merge requests that have a file that is part of the handbook.

The `handbook_file_classification_mapping.csv` is loaded and used to classify each handbook MR into one or more departments or divisions for easy analysis of how often the handbook is being changed for each department of interest.  

This data is also joined to the gitlab db data to get current merge request statuses.

{% enddocs %}

{% docs handbook_values_page %}

This data is gathered by using `git log` for the [handbook project](https://gitlab.com/gitlab-com/www-gitlab-com/). 

For data prior to June 2020, [this URL](https://gitlab.com/gitlab-com/www-gitlab-com/-/commits/826b9dc4a2687445b689fdaf4a902fcb73c36a5e/source/handbook/values/index.html.md) was used as the source to manually generate and upload a CSV file.

[This page](https://gitlab.com/gitlab-com/www-gitlab-com/-/commits/master/sites/handbook/source/handbook/values/index.html.md) is the source for data after June 2020.

{% enddocs %}
