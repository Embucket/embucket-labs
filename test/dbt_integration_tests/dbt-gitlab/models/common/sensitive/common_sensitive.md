{% docs dim_marketing_contact %}

A dimension table for marketing contacts, combining contacts from GitLab.com, Salesforce, CustomerDB, and Zuora Sources.

{% enddocs %}


{% docs bdg_marketing_contact_role %}

A bridge table that lists all roles for the marketing contacts.

{% enddocs %}

{% docs bdg_marketing_contact_order %}

A bridge table that lists all the orders, subsciptions, and namespaces per role per contact.

{% enddocs %}

{% docs mart_marketing_contact %}

This table aggregates data from namespaces, orders, and subscriptions to the level of a marketing contact. Therefore a marketing contact can fall into multiple plan types. An individual could be a Free Individual namespace owner, a member of an Ultimate Group Namespace, and an Owner of a Premium Group Namespace. Each column aggregates data to answers a specific question at the contact level.

All the usage metrics, fields prefixed by `usage_` come from the latest month of data from the Self-Managed Usage Ping data. This data is related to marketing contact by the user that makes the order in the customers portal or the billing account related to the subscription_id  of the self-managed instance. In case there are multiple self-managed instances per user, then these are summed.

{% enddocs %}
