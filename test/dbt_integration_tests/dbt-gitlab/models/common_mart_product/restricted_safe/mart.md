{% docs mart_user_request %}

Mart table that contains all user requests to the Gitlab product by the customers.

It unions `bdg_issue_user_request` and `bdg_epic_user_request` to have the product request that are contained both in the epics and issues in the `gitlab-org` group.
After that, it adds useful data around these issues and epics as well the crm_account and crm_opportunity links that are useful to prioritize those user requests.

More documentation around the use cases this model solves for can be found in the [Customer Issue Prioritization Framework Handbook Page](https://about.gitlab.com/handbook/product/product-processes/customer-issues-prioritization-framework/)

{% enddocs %}
