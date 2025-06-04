{% docs customers_db_customers_source %}

This model is the data from tap-postgres for the customers table from customers.gitlab.com. The schema of the database is defined in [this ruby code](https://gitlab.com/gitlab-org/customers-gitlab-com/blob/master/db/schema.rb).

{% enddocs %}

{% docs customers_db_leads_source %}

This model is the data from tap-postgres for the leads table from customers.gitlab.com. The schema of the database is defined in [this ruby code](https://gitlab.com/gitlab-org/customers-gitlab-com/blob/master/db/schema.rb).

{% enddocs %}

{% docs customers_db_license_seat_links_source %}

Self-managed EE instances will send seat link information to the customers portal on a daily basis. This model limits down to the last-updated record for each installation for each day. This information includes a count of active users and a maximum count of users historically in order to assist the true up process.

{% enddocs %}

{% docs customers_db_orders_source %}

This model is the data from tap-postgres for the orders table from customers.gitlab.com. The schema of the database is defined in [this ruby code](https://gitlab.com/gitlab-org/customers-gitlab-com/blob/master/db/schema.rb).

{% enddocs %}

{% docs customers_db_reconciliations_source %}

This model is the data from tap-postgres for the reconciliations table from customers.gitlab.com. The schema of the database is defined in [this ruby code](https://gitlab.com/gitlab-org/customers-gitlab-com/blob/master/db/schema.rb).

{% enddocs %}

{% docs customers_db_trial_histories_source %}

This model is the data from tap-postgres for the trial_histories table from customers.gitlab.com. The schema of the database is defined in [this ruby code](https://gitlab.com/gitlab-org/customers-gitlab-com/blob/master/db/schema.rb).

{% enddocs %}

{% docs customers_db_license_versions_source %}

This table contains data from licenses versions in customers portal.

{% enddocs %}

{% docs customers_db_billing_account_memberships_source %}

This model is the data from tap-postgres for the customer billing account memberships table from customers.gitlab.com. It is used to represent customer memberships to Zuora Accounts. This model is essentially a join table between customers and billing_accounts and hence has a foreign_key `billing_account_id`. The schema of the database is defined in [this ruby code](https://gitlab.com/gitlab-org/customers-gitlab-com/blob/master/db/schema.rb).

{% enddocs %}

{% docs customers_db_billing_accounts_source %}

This model is the data from tap-postgres for the customer billing accounts table from customers.gitlab.com. It is used to represent a Zuora Customer Account. The schema of the database is defined in [this ruby code](https://gitlab.com/gitlab-org/customers-gitlab-com/blob/master/db/schema.rb).

{% enddocs %}

{% docs customers_db_cloud_activations_source %}

This model is the data from tap-postgres for the cloud activations table from customers.gitlab.com. It stores information about all the activation codes that were generated for Cloud licenses. Customers use this code after the installation of their GitLab instance. The schema of the database is defined in [this ruby code](https://gitlab.com/gitlab-org/customers-gitlab-com/blob/master/db/schema.rb).

{% enddocs %}

{% docs customers_db_provisions_source %}

This model is the data from tap-postgres for the provisions table from customers.gitlab.com. It is used to track the provision of SM and SaaS subscriptions through its entire life cycle. The schema of the database is defined in [this ruby code](https://gitlab.com/gitlab-org/customers-gitlab-com/blob/master/db/schema.rb).

{% enddocs %}

{% docs customers_db_billing_account_contacts_source %}

This model is the data from tap-postgres for the customer billing account contacts table from customers.gitlab.com. It is used to cache Zuora Contacts (namely Bill to and Sold to) related to a given Billing Account. The schema of the database is defined in [this ruby code](https://gitlab.com/gitlab-org/customers-gitlab-com/blob/master/db/schema.rb).

{% enddocs %}

