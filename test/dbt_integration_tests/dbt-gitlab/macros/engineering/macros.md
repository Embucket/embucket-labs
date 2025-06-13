{% docs is_project_part_of_product %}
This macro pulls all the engineering projects that are part of the product from
the seeded csv and adds a boolean in the model that can be used to filter on it.
This macro applies to data from GitLab.com database.
{% enddocs %}

{% docs is_project_part_of_product_ops %}
This macro pulls all the engineering projects that are part of the product from
the seeded csv and adds a boolean in the model that can be used to filter on it.
This macro applies to data from ops.gitlab.net (GitLab Ops) database.
{% enddocs %}

{% docs is_project_included_in_engineering_metrics %}
This macro pulls all the engineering projects to be included from the seeded csv and adds a boolean in the model that can be used to filter on it.
{% enddocs %}
