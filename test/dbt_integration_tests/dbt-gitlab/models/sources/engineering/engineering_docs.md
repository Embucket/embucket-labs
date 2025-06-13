{% docs engineering_advisory_data %}

The data is derived from files contained in [gemnasium-db repository](https://gitlab.com/gitlab-org/security-products/gemnasium-db). The CI job leverages Git metadata and filesystem information directly and computes the following information:

* full file path from advisory file
* git merge date for advisory file
* publication date of the advisory
* package type to which the advisory relates

The script that performs these steps is available [here](https://gitlab.com/gitlab-org/security-products/gemnasium-db/blob/master/stats/scripts/prepare_data.rb).

The data is available via [this link](https://gitlab.com/gitlab-org/security-products/gemnasium-db/-/jobs/artifacts/master/raw/data/data.tar.gz?job=pages).

{% enddocs %}

{% docs engineering_blocking_time %}

This data is pulled from graphite for a collection of pages.  The form of the metric name is `sitespeed_io.desktop.gitlab.pageSummary.gitlab_com.{page}.chrome.cable.browsertime.statistics.cpu.longTasks.totalBlockingTime.{aggregation_type}` for each page we are tracking.

{% enddocs %}

{% docs engineering_layout_shift %}

This data is pulled from graphite for a collection of pages.  The form of the metric name is `sitespeed_io.desktop.gitlab.pageSummary.gitlab_com.{page}.chrome.cable.browsertime.statistics.pageinfo.cumulativeLayoutShift.*` for each page we are tracking.

{% enddocs %}

{% docs engineering_nvd_data %}

The data is derived from the National Vulnerability Database and details the coverage. 

The script that performs these steps is available [here](https://gitlab.com/gitlab-org/security-products/gemnasium-db/blob/master/stats/scripts/prepare_data.rb).

The data is available via [this link](https://gitlab.com/gitlab-org/security-products/gemnasium-db/-/jobs/artifacts/master/raw/data/nvd.tar.gz?job=pages).

There are two columns in CSV file representing respectively NVD feed publication year and NVD feed size (i.e, number of contained CVEs).

{% enddocs %}

{% docs engineering_product_merge_requests %}

This data is first gathered by getting the CSV located in the data project [here](https://gitlab.com/gitlab-data/analytics/raw/master/transform/snowflake-dbt/data/projects_part_of_product.csv).  The CSV data is then parsed to get each project id that is part of the product.  The GitLab API is then hit to get all information about the merge requests that are part of each project.

The data eventually made available through dbt currently only involves numbers of lines and files changed by the merge request.

{% enddocs %}

{% docs engineering_development_team_members %}

This data is publicly available data about the GitLab team-members in the engineering division.  This data is gathered, like other engineering extracts, by querying a public cloud function that returns the entire JSON array.  One extract of data results in a single row in the RAW table.  The row is an array with all of the team member data.

{% enddocs %}