{% docs event_pk %}

The unique identifier of an event. This is a generated primary key and will not join back to the source models

{% enddocs %}

{% docs event_id %}

The natural key of an event. This will join back to the source models.

{% enddocs %}

{% docs dim_latest_product_tier_id %}

The unique identifier of the ultimate parent namespace's latest product tier, easily joined to `dim_product_tier`

{% enddocs %}

{% docs dim_latest_subscription_id %}

The unique identifier of the ultimate parent namespace's latest subscription, easily joined to `dim_subscription`

{% enddocs %}

{% docs dim_event_date_id %}

The ID of the event date, easily joined to `dim_date`

{% enddocs %}

{% docs dim_crm_account_id %}

The unique identifier of a crm account, easily joined to `dim_crm_account`

{% enddocs %}

{% docs dim_billing_account_id %}

The identifier of the Zuora account associated with the subscription, easily joined to `dim_billing_account`

{% enddocs %}

{% docs dim_ultimate_parent_namespace_id_event_model %}

The unique identifier of the ultimate parent namespace in which the event was generated, easily joined to `dim_namespace`. The recommended JOIN is `dim_ultimate_parent_namespace_id = dim_namespace.dim_namespace_id`, which will be a one-to-one relationship. JOINing on `dim_ultimate_parent_namespace_id = dim_namespace.ultimate_parent_namespace_id` will return `dim_namespace` records for both the ultimate parent _and_ all sub-groups underneath it. This field will be NULL if the event is not tied to a namespace (ex. users_created)

{% enddocs %}

{% docs dim_project_id_event_model %}

The unique identifier of the project in which the event was generated, easily joined to `dim_project`. This will be NULL if the event is not tied to a project (ex. epic_creation, etc)

{% enddocs %}

{% docs dim_ultimate_parent_namespace_id %}

The unique identifier (and natural key) of the namespace's ultimate parent, easily joined to `dim_namespace`. The recommended JOIN is `dim_ultimate_parent_namespace_id = dim_namespace.dim_namespace_id`, which will be a one-to-one relationship. JOINing on `dim_ultimate_parent_namespace_id = dim_namespace.ultimate_parent_namespace_id` will return `dim_namespace` records for both the ultimate parent _and_ all sub-groups underneath it.

{% enddocs %}

{% docs dim_project_id %}

The unique identifier (and natural key) of the project, easily joined to `dim_project`

{% enddocs %}

{% docs dim_user_id %}

The unique identifier of the user who generated the event, easily joined to `dim_user`. This will be NULL if the event is not tied to a specific user (ex. terraform_reports, etc)

{% enddocs %}

{% docs event_created_at %}

Timestamp of the event

{% enddocs %}

{% docs event_date %}

The date of the event

{% enddocs %}

{% docs parent_id %}

The unique identifier of the project (dim_project_id) associated with the event. If no project is associated, the ultimate parent namespace associated with the event. This will be NULL if neither a project or namespace is associated with the event

{% enddocs %}

{% docs parent_type %}

Denotes whether the event was associate with a project or namespace ('project' or 'group'). This will be NULL if neither a project or namespace is associated with the event

{% enddocs %}

{% docs is_smau %}

Boolean flag set to True if the event (gitlab.com db data) or metric (Service Ping data) is chosen for the stage's SMAU metric

{% enddocs %}

{% docs is_gmau %}

Boolean flag set to True if the event (gitlab.com db data) or metric (Service Ping data) is chosen for the group's GMAU metric

{% enddocs %}

{% docs is_umau %}

Boolean flag set to True if the event (gitlab.com db data) or metric (Service Ping data) is chosen for the UMAU metric

{% enddocs %}

{% docs event_name %}

The name tied to the event

{% enddocs %}

{% docs stage_name %}

The name of the [product stage](https://gitlab.com/gitlab-com/www-gitlab-com/blob/master/data/stages.yml) (ex. secure, plan, create, etc)

{% enddocs %}

{% docs section_name %}

The name of the [product section](https://gitlab.com/gitlab-com/www-gitlab-com/-/blob/master/data/sections.yml) (ex. dev, ops, etc)

{% enddocs %}

{% docs group_name %}

The name of the [product group](https://gitlab.com/gitlab-com/www-gitlab-com/blob/master/data/stages.yml) (ex. code_review, project_management, etc)

{% enddocs %}

{% docs instrumentation_class_ping_metric %}

The name of the [instrumentation class](https://docs.gitlab.com/ee/development/service_ping/metrics_instrumentation.html#nomenclature) (ex. DatabaseMetric, RedisMetric, RedisHLLMetric, etc)

{% enddocs %}

{% docs plan_id_at_event_date %}

The ID of the ultimate parent namespace's plan on the day the event was created (ex. 34, 100, 101, etc). If multiple plans are available on a given day, this reflects the plan on the last event of the day for the namespace. Defaults to '34' (free) if a value is not available

{% enddocs %}

{% docs plan_name_at_event_date %}

The name of the ultimate parent namespace's plan type on the day when the event was created (ex. free, premium, ultimate). If multiple plans are available on a given day, this reflects the plan on the last event of the day for the namespace. Defaults to 'free' if a value is not available

{% enddocs %}

{% docs plan_was_paid_at_event_date %}

Boolean flag which is set to True if the ultimate parent namespace's plan was paid on the day when the event was created. If multiple plans are available on a given day, this reflects the plan on the last event of the day for the namespace. Defaults to False if a value is not available

{% enddocs %}

{% docs plan_id_at_event_timestamp %}

The ID of the ultimate parent namespace's plan at the timestamp the event was created (ex. 34, 100, 101, etc). Defaults to '34' (free) if a value is not available

{% enddocs %}

{% docs plan_name_at_event_timestamp %}

The name of the ultimate parent namespace's plan type at the timestamp when the event was created (ex. free, premium, ultimate). Defaults to 'free' if a value is not available

{% enddocs %}

{% docs plan_was_paid_at_event_timestamp %}

Boolean flag which is set to True if the ultimate parent namespace's plan was paid at the timestamp when the event was created. Defaults to False if a value is not available

{% enddocs %}

{% docs plan_id_at_event_month %}

The ID of the ultimate parent namespace's plan on the month the event was created (ex. 34, 100, 101, etc). If multiple plans are available during the month, this reflects the plan on the last event of the month for the namespace. Defaults to '34' (free) if a value is not available

{% enddocs %}

{% docs plan_name_at_event_month %}

The name of the ultimate parent namespace's plan on the month the event was created (ex. free, premium, ultimate, etc). If multiple plans are available during the month, this reflects the plan on the last event of the month for the namespace. Defaults to 'free' if a value is not available

{% enddocs %}

{% docs plan_was_paid_at_event_month %}

Boolean flag which is set to True if the ultimate parent namespace's plan was paid on the month when the event was created. If multiple plans are available during the month, this reflects the plan on the last event of the month for the namespace. Defaults to False if a value is not available

{% enddocs %}

{% docs days_since_user_creation_at_event_date %}

The count of days between user creation and the event. This will be NULL if a user is not associated with the event

{% enddocs %}

{% docs days_since_namespace_creation_at_event_date %}

The count of days between ultimate parent namespace creation and the event. This will be NULL if a namespace is not associated with the event

{% enddocs %}

{% docs days_since_project_creation_at_event_date %}

The count of days between project creation and the event. This will be NULL if a project is not associated with the event

{% enddocs %}

{% docs data_source %}

The source application where the data was extracted from (ex. GITLAB_DOTCOM, VERSION_DB)

{% enddocs %}

{% docs namespace_is_internal %}

Boolean flag set to True if the ultimate parent namespace in which the event was generated is identified as an internal GitLab namespace

{% enddocs %}

{% docs namespace_creator_is_blocked %}

Boolean flag set to True if the ultimate parent namespace creator is in a 'blocked' or 'banned' state

{% enddocs %}

{% docs gitlab_plan_is_paid %}

Indicates whether or not the namespace is subscribed to a paid plan. This can be inherited from the namespace's ultimate parent. `NULL` if the namespace has been deleted.

{% enddocs %}

{% docs namespace_created_at %}

The timestamp of the ultimate parent namespace creation

{% enddocs %}

{% docs namespace_created_date %}

The date of the ultimate parent namespace creation

{% enddocs %}

{% docs user_created_at %}

The timestamp of the user creation

{% enddocs %}

{% docs is_blocked_user %}

Boolean flag set to True if the user who generated the events is in a 'blocked' or 'banned' state

{% enddocs %}

{% docs project_is_learn_gitlab %}

Boolean flag set to True if the project in which the event was generated was a Learn GitLab project, one automatically created during user onboarding

{% enddocs %}

{% docs project_is_imported %}

Boolean flag set to True if the project in which the event was generated was imported

{% enddocs %}

{% docs event_calendar_month %}

The first day of the calendar month of the event (ex. 2022-05-01, etc)

{% enddocs %}

{% docs event_calendar_quarter %}

The calendar quarter of the event (ex. 2022-Q2, etc)

{% enddocs %}

{% docs event_calendar_year %}

The calendar year of the event (ex. 2022, etc)

{% enddocs %}

{% docs created_by %}

The GitLab handle of the original model creator

{% enddocs %}

{% docs updated_by %}

The GitLab handle of the most recent model editor

{% enddocs %}

{% docs model_created_date %}

Manually input ISO date of when model was original created

{% enddocs %}

{% docs model_updated_date %}

Manually input ISO date of when model was updated

{% enddocs %}

{% docs valid_from %}

Timestamp of when this metric path mapping is valid from

{% enddocs %}

{% docs valid_to %}

Timestamp of when this metric path mapping is valid until

{% enddocs %}

{% docs event_count %}

The count of events generated

{% enddocs %}

{% docs user_count %}

 The count of distinct users who generated an event

{% enddocs %}

{% docs ultimate_parent_namespace_count %}

 The count of distinct ultimate parent namespaces in which an event was generated

{% enddocs %}

{% docs event_date_count %}

 The count of distinct days in which an event was generated

{% enddocs %}

{% docs ultimate_parent_namespace_type %}

 The type of Ultimate Parent Namespace (user,group,project)

{% enddocs %}

{% docs dim_ping_date_id %}

The ID of the Service Ping creation date, easily joined to `dim_date`

{% enddocs %}

{% docs metrics_path  %}

The unique JSON key path of the identifier of the metric in the Service Ping payload. This appears as `key_path` in the metric definition YAML files

{% enddocs %}

{% docs metric_value %}

The value associated with the metric path. In most models, metrics with a value of `-1` (those that timed out during ping generation) are set to `0`. See model description for confirmation.

{% enddocs %}

{% docs monthly_metric_value %}

For 28 day metrics, this is the metric value that comes directly from the ping payload. For all-time metrics, this is a calculation using the monthly_all_time_metric_calc macro. The macro calculates an installation-level MoM difference in metric value, attempting to create a monthly version of an all-time counter.

{% enddocs %}

{% docs has_timed_out %}

Boolean flag which is set to True if the metric timed out during Service Ping generation. In the ping payload, timed out metrics have a value of `-1`, but in most models the value is set to `0` (see model description for confirmation).

{% enddocs %}

{% docs dim_ping_instance_id %}

The unique identifier of the ping. This appears as `id` in the ping payload.

{% enddocs %}

{% docs dim_instance_id %}

The identifier of the instance, easily joined to `dim_installation`. This id is stored in the database of the installation and appears as `uuid` in the ping payload. In Snowplow events, this field is coalesced from the GitLab Standard context and the Code Suggestions context.

{% enddocs %}

{% docs dim_license_id %}

The unique identifier of the license, easily joined to `dim_license`

{% enddocs %}

{% docs dim_installation_id %}

The unique identifier of the installation, easily joined to `dim_installation`. This id is the combination of `dim_host_id` and `dim_instance_id` and is considered the unique identifier of the installation for reporting and analysis

{% enddocs %}

{% docs latest_subscription_id %}

The latest child `dim_subscription_id` of the subscription linked to the license

{% enddocs %}

{% docs dim_parent_crm_account_id %}

The identifier of the ultimate parent account, easily joined to `dim_crm_account`

{% enddocs %}

{% docs dim_host_id %}

The identifier of the host, easily joined to `dim_installation` or `dim_host`. There is a 1:1 relationship between hostname and dim_host_id, so it will be shared across installations with the same hostname.

{% enddocs %}

{% docs host_name %}

The name (URL) of the host. This appears as `hostname` in the ping payload. In Snowplow events, this field is coalesced from the GitLab Standard context and the Code Suggestions context.

{% enddocs %}

{% docs ping_delivery_type %}

How the product is delivered to the installation (Self-Managed, SaaS). GitLab Dedicated installations are assigned a delivery type of `SaaS`. `ping_delivery_type` is determined using `dim_instance_id`/`uuid` and `is_saas_dedicated` and is defined as

``` sql
CASE
  WHEN dim_instance_id = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f' THEN 'SaaS' --dim_instance_id is synonymous with uuid
  WHEN is_saas_dedicated = TRUE THEN 'SaaS'
  ELSE 'Self-Managed'
END AS ping_delivery_type
```

{% enddocs %}

{% docs ping_edition %}

The main edition of GitLab on the installation (EE, CE), also referred to as distribution

{% enddocs %}

{% docs ping_product_tier %}

The product tier of the ping, inferred from the edition and the plan saved in the license (Free, Starter, Premium, Ultimate).

{% enddocs %}

{% docs ping_edition_product_tier %}

The concatenation of `ping_edition` and `ping_product_tier` (ex. `EE - Premium`, `EE - Ultimate`, `EE - Free`, etc).

{% enddocs %}

{% docs major_version %}

The major version of GitLab on the installation. For example, for 13.6.2, `major_version` is 13. See details [here](https://docs.gitlab.com/ee/policy/maintenance.html)

{% enddocs %}

{% docs minor_version %}

The minor version of GitLab on the installation. For example, for 13.6.2, `minor_version` is 6. See details [here](https://docs.gitlab.com/ee/policy/maintenance.html)

{% enddocs %}

{% docs major_minor_version %}

The concatenation of major and minor version, easily joined to `dim_gitlab_releases`. For example, for 13.6.2, `major_minor_version` is 13.6. See details [here](https://docs.gitlab.com/ee/policy/maintenance.html).

{% enddocs %}

{% docs major_minor_version_id %}

The id of the major minor version, defined as `major_version*100 + minor_version`. For example, for 13.6.2, the `major_minor_version_id` is 1306. This id is intended to facilitate easy filtering on versions. To be replaced with `major_minor_version_num`.

{% enddocs %}

{% docs major_minor_version_num %}

The numeric variation of `major_minor_version`, defined as `major_version*100 + minor_version`. For example, for 13.6.2, the `major_minor_version_num` is 1306. This id is intended to facilitate easy ordering on versions.

{% enddocs %}

{% docs app_release_major_minor_id %}

The natural key of dim_app_release_major_minor. This natural key is defined as the concatenation of the `application` and the major minor version. For example, for the GitLab version 13.6.2, the `app_release_major_minor_id` is `GiLab-13.06`.

{% enddocs %}

{% docs dim_app_release_major_minor_sk %}

Surrogate key of dim_app_release_major_minor. Currently identified by hashing the major_minor_version field combined with the application field.

{% enddocs %}

{% docs dim_latest_available_app_release_major_minor_sk %}

The latest avaiable dim_app_release_major_minor_sk at the moment the ping is sent.

{% enddocs %}

{% docs version_is_prerelease %}

Boolean flag which is set to True if the version is a pre-release Version of the GitLab App. See more details [here](https://docs.gitlab.com/ee/policy/maintenance.html). This is defined as `IFF(version ILIKE '%-pre', TRUE, FALSE)`.

{% enddocs %}

{% docs release_date %}

Release date of the GitLab version.

{% enddocs %}

{% docs version_number %}

The sequential number of the major_minor_version.

{% enddocs %}

{% docs next_version_release_date %}

Release date of the next GitLab version.

{% enddocs %}

{% docs days_after_version_release_date %}

The number of days between the date the ping was sent and the release date of the version associated with the ping. When `version_is_prerelease = TRUE`, then this field is less than 0.

There are some cases when `version_is_prerelease = FALSE` and the field is still lower than 0. These cases where manually set to zero in the data model.

{% enddocs %}

{% docs latest_version_available_at_ping_creation %}

The most recent version that is available at the time the ping is created.

{% enddocs %}

{% docs versions_behind_latest_at_ping_creation %}

The number of versions by which the ping-associated version lags behind. When `version_is_prerelease = TRUE`, then this field is less than 0.

There are some cases when `version_is_prerelease = FALSE` and the field is still lower than 0. These cases where manually set to zero in the data model.

{% enddocs %}

{% docs is_internal_installation %}

Boolean flag set to True if the installation is listed in the `internal_gitlab_installations.csv` seed file. This identification is implemented through a left join with the seed file. Currently, these are manually identified and added to the seed file.

{% enddocs %}

{% docs is_staging_installation %}

Boolean flag which is set to True if the installations meets our defined "staging" criteria (i.e., `staging` is in the host name). This is a directional identification and is not exhaustive of all staging installations. `is_staging` is defined as

``` sql
CASE
  WHEN hostname ILIKE 'staging.%' THEN TRUE
  WHEN hostname IN (
    'staging.gitlab.com',
    'dr.gitlab.com'
    )                             THEN TRUE
  ELSE FALSE
END AS is_staging
```

{% enddocs %}

{% docs is_trial_ping_model %}

Boolean flag which is set to True if the installation has a valid trial license at Service Ping creation. This is defined as `IFF(ping_created_at < license_trial_ends_on, TRUE, FALSE)`.

{% enddocs %}

{% docs license_trial_ping_model %}

Boolean flag which is set to True if the installation has a valid trial license at Service Ping creation. This field can be NULL if no license information is provided. There are cases where `license_trial = TRUE` even when an installation is outside of a trial period, so be cautious using this field.

{% enddocs %}

{% docs umau_value %}

The unique monthly active user (UMAU) value for the installation (i.e., the value of the metric flagged as UMAU)

{% enddocs %}

{% docs duo_pro_purchased_seats %}

The number of Duo Pro seats purchased for the installation

{% enddocs %}

{% docs duo_pro_assigned_seats %}

The number of Duo Pro seats assigned for the installation

{% enddocs %}

{% docs duo_enterprise_purchased_seats %}

The number of Duo Enterprise seats purchased for the installation

{% enddocs %}

{% docs duo_enterprise_assigned_seats %}

The number of Duo Enterprise seats assigned for the installation

{% enddocs %}

{% docs is_paid_gmau %}

Boolean flag set to True if the metric (Service Ping data) is chosen for the group's paid GMAU metric

{% enddocs %}

{% docs time_frame %}

The [time frame](https://docs.gitlab.com/ee/development/service_ping/metrics_dictionary.html#metric-time_frame) associated with the metric, as defined in the metric definition YAML file. May be set to `7d`, `28d`, `all`, `none`

{% enddocs %}

{% docs instance_user_count %}

The number of active users existing in the installation. In this case "active" is referring to a user's state (ex. not blocked) as opposed to an indication of user activity with the product

{% enddocs %}

{% docs subscription_name %}

If a subscription is linked to the license, name of the subscription, easily joined to `dim_subscription`, etc

{% enddocs %}

{% docs original_subscription_name_slugify %}

If a subscription is linked to the license, slugified name of the subscription, easily joined to `dim_subscription`, etc

{% enddocs %}

{% docs subscription_start_month %}

The first day of the calendar month when the subscription linked to the license started

{% enddocs %}

{% docs subscription_end_month %}

The first day of the calendar month when the subscription linked to the license is supposed to end according to last agreed terms

{% enddocs %}

{% docs product_category_array %}

An array containing all of the product tier names associated associated with the subscription

{% enddocs %}

{% docs product_rate_plan_name_array %}

An array containing all of the product rate plan names associated with the subscription

{% enddocs %}

{% docs is_paid_subscription %}

Boolean flag set to True if the subscription has a MRR > 0 on the month of ping creation

{% enddocs %}

{% docs is_program_subscription %}

Boolean flag set to True if the subscription is under an EDU or OSS Program. This is defined as `IFF(product_rate_plan_name ILIKE ANY ('%edu%', '%oss%'), TRUE, FALSE)`

{% enddocs %}

{% docs reporting_month %}


{% enddocs %}

{% docs license_id %}

{% enddocs %}

{% docs license_company_name %}

{% enddocs %}

{% docs license_expire_date %}

{% enddocs %}

{% docs dim_subscription_id %}

Unique identifier of a version of a subscription

{% enddocs %}

{% docs dim_subscription_id_original %}

Unique identifier of a subscription, does not change when amendments are made to the subscription. This ID will have multiple dim_subscription_id values associated with it for each version of the original subscription

{% enddocs %}

{% docs subscription_version %}

The version number of the subscription

{% enddocs %}

{% docs dim_namespace_id %}

The namespace ID of the instance (GitLab.com only)

{% enddocs %}

{% docs product_rate_plan_charge_name %}

The name of the product rate plan charge

{% enddocs %}

{% docs charge_type %}

Type of the charge

{% enddocs %}

{% docs parent_crm_account_upa_country %}

{% enddocs %}

{% docs crm_account_name %}

The name of the crm account coming from SFDC

{% enddocs %}

{% docs parent_crm_account_name %}

The name of the ultimate parent account coming from SFDC

{% enddocs %}

{% docs parent_crm_account_sales_segment %}

The sales segment of the ultimate parent account from SFDC. Sales Segments are explained [here](https://about.gitlab.com/handbook/sales/field-operations/gtm-resources/#segmentation)

{% enddocs %}

{% docs parent_crm_account_industry %}

The industry of the ultimate parent account from SFDC

{% enddocs %}

{% docs parent_crm_account_owner_team %}

The owner team of the ultimate parent account from SFDC

{% enddocs %}

{% docs parent_crm_account_territory %}

The sales territory of the ultimate parent account from SFDC

{% enddocs %}

{% docs technical_account_manager %}

The name of the technical account manager of the CRM account

{% enddocs %}

{% docs ping_created_at %}

The timestamp when the ping was created

{% enddocs %}

{% docs latest_ping_created_at %}

The timestamp when the most recent ping was created for a specific source.

{% enddocs %}

{% docs first_ping_created_at %}

The timestamp when the very first ping was created for a specific source.

{% enddocs %}

{% docs ping_created_date_month %}

The first day of the calendar month when the ping was created (YYYY-MM-01)

{% enddocs %}

{% docs is_last_ping_of_month %}

Boolean flag set to True if this is the installation's (defined by `dim_installation_id`) last ping of the calendar month (defined by `ping_created_at`)

{% enddocs %}

{% docs collected_data_categories %}

Comma-separated list of collected data categories corresponding to the installation's settings (ex: `standard,subscription,operational,optional`)

{% enddocs %}

{% docs ping_created_date_week %}

The first day of the calendar week when the ping was created (YYYY-MM-DOW)

{% enddocs %}

{% docs is_last_ping_of_week %}

Boolean flag set to True if this is the installation's (defined by `dim_installation_id`) last ping of the calendar week (defined by `ping_created_at`). This field leverages `first_day_of_week` from `common.dim_date`, which defines a week as starting on Monday and ending on Sunday.

{% enddocs %}

{% docs dim_product_tier_id_ping_model %}

The unique identifier of a product tier, easily joined to `dim_product_tier`. This will reflect the tier of the installation at time of ping creation.

{% enddocs %}

{% docs dim_product_tier_id %}

The unique identifier of a product tier, easily joined to `dim_product_tier`.

{% enddocs %}

{% docs dim_subscription_id_ping_model %}

The unique identifier of a subscription, easily joined to `dim_subscription`. This is defined as the subscription_id associated with the license, with `license_subscription_id` from the ping payload as a fallback value.

{% enddocs %}

{% docs dim_location_country_id_ping_model %}

The unique identifier of a country, easily joined to `dim_location_country`. The location is associated with the IP address of the ping.

{% enddocs %}

{% docs license_md5 %}

The md5 hash of the license file.

{% enddocs %}

{% docs license_billable_users %}

The count of active users who can be billed for. Guest users and bots are not included. This value comes from the ping payload.

{% enddocs %}

{% docs historical_max_users %}

The peak active (defined as non-blocked) user count ever reported over the lifetime of the subscription.

{% enddocs %}

{% docs license_user_count %}

Count of licensed users purchased with the customer's subscription.

{% enddocs %}

{% docs active_user_count %}

Count of active users currently on the customer's subcription. A count of 0 indicates that Seat Link has been turned off by the customer.

{% enddocs %}

{% docs max_historical_user_count %}

Maximum active user count ever reported over the lifetime of the customer's subcription.

{% enddocs %}

{% docs dim_subscription_license_id %}

The unique identifier of a license subscription. This appears as `license_subscription_id` in the ping payload.

{% enddocs %}

{% docs is_license_mapped_to_subscription %}

Data quality boolean flag set to True if the license table has a value in both license_id and subscription_id

{% enddocs %}

{% docs is_license_subscription_id_valid %}

Data quality boolean flag set to True if the subscription_id in the license table is valid (does it exist in the subscription table?)

{% enddocs %}

{% docs is_service_ping_license_in_customerDot %}

Data quality boolean flag set to True if the license from Service Ping exist in CustomerDot.

{% enddocs %}

{% docs ping_created_date %}

The date when the ping was created (YYYY-MM-DD)

{% enddocs %}

{% docs ping_created_date_28_days_earlier %}

The date 28 days prior to when the ping was created

{% enddocs %}

{% docs ping_created_date_year %}

The year when the ping was created (YYYY-01-01)

{% enddocs %}

{% docs ip_address_hash_ping_model %}

The hashed source_ip associated with the ping

{% enddocs %}

{% docs recorded_at_ping_model %}

The time when the Service Ping computation was started

{% enddocs %}

{% docs updated_at_ping_model %}

The time when the ping data was last updated from the Versions db

{% enddocs %}

{% docs source_license_id %}

The unique identifier of the source license. This appears as `license_id` in the ping payload.

{% enddocs %}

{% docs license_starts_at %}

The date the license starts

{% enddocs %}

{% docs license_expires_at %}

The date the license expires

{% enddocs %}

{% docs license_add_ons %}

The add-ons associated with the license. In [the handbook](https://about.gitlab.com/handbook/support/license-and-renewals/#common-terminology), the term "add-on" is defined as

> An optional extra that can be purchased to increase the limits of what is available in GitLab. Common examples of this are a Seat add-on where additional seats are purchased during the subscription term, or an additional Storage or Units of Compute purchase (on SaaS only).

{% enddocs %}

{% docs version_ping_model %}

The full version of GitLab associated with the installation (ex. 13.8.8-ee, 15.4.2, etc). See details [here](https://docs.gitlab.com/ee/policy/maintenance.html)

{% enddocs %}

{% docs cleaned_version %}

The full version of GitLab associated with the installation, formatted as `(Major).(Minor).(Patch)` (ex. 13.8.8, 15.4.2, 14.7.0). See details [here](https://docs.gitlab.com/ee/policy/maintenance.html)

{% enddocs %}

{% docs mattermost_enabled %}

Boolean flag set to True if Mattermost is enabled

{% enddocs %}

{% docs installation_type %}

The type of installation associated with the instance (i.e. gitlab-development-kit, gitlab-helm-chart, gitlab-omnibus-helm-chart)

{% enddocs %}

{% docs license_plan %}

The license plan associated with the installation (ex, premium, ultimate). This value comes directly from the ping payload

{% enddocs %}

{% docs uuid_ping_model %}

The identifier of the instance. This value is synonymous with `dim_instance_id` in other models.

{% enddocs %}

{% docs host_id %}

The identifier of the host. There is a 1:1 relationship between hostname and host_id, so it will be shared across installations with the same hostname. This value is synonymous with `dim_host_id` in other models

{% enddocs %}

{% docs id_ping_model %}

The unique identifier for a Service Ping. This value is synonymous with `dim_ping_instance_id` in other models.

{% enddocs %}

{% docs original_edition %}

The unmodified `edition` value as it appears in the ping payload (ex. CE, EE, EES, EEP, EEU, EE Free)

{% enddocs %}

{% docs cleaned_edition %}

A modified version of the edition of GitLab on the installation, with 3 possible values: CE, EE, and EE Free. Here is the SQL that generates this value

`IFF(license_expires_at >= ping_created_at OR license_expires_at IS NULL, main_edition, 'EE Free')`

{% enddocs %}

{% docs database_adapter %}

The database adapter associated with the installation. This only returns a value of `postgresql` in supported versions of GitLab.

{% enddocs %}

{% docs database_version %}

The version of the PostgreSQL database associated with the installation (ex. 9.5.3, 12.10, etc)

{% enddocs %}

{% docs git_version %}

The version of Git the installations is running (ex. 2.29.0, 2.35.1., 2.14.3, etc)

{% enddocs %}

{% docs gitlab_pages_enabled %}

Boolean flag set to True if GitLab Pages is enabled

{% enddocs %}

{% docs gitlab_pages_version %}

The version number of GitLab Pages (ex. 1.25.0, 1.51.0)

{% enddocs %}

{% docs container_registry_enabled %}

Boolean flag set to True if container registry is enabled

{% enddocs %}

{% docs elasticsearch_enabled %}

Boolean flag set to True if Elasticsearch is enabled

{% enddocs %}

{% docs geo_enabled %}

Boolean flag set to True if Geo is enabled

{% enddocs %}

{% docs gitlab_shared_runners_enabled %}

Boolean flag set to True if shared runners is enabled

{% enddocs %}

{% docs gravatar_enabled %}


Boolean flag set to True if Gravatar is enabled
{% enddocs %}

{% docs ldap_enabled %}

Boolean flag set to True if LDAP is enabled

{% enddocs %}

{% docs omniauth_enabled %}

Boolean flag set to True if OmniAuth is enabled

{% enddocs %}

{% docs reply_by_email_enabled %}

Boolean flag set to True if incoming email is set up

{% enddocs %}

{% docs signup_enabled %}

Boolean flag set to True if public signup (aka "Open Registration") is enabled. More details about this feature [here](https://gitlab.com/groups/gitlab-org/-/epics/4214)

{% enddocs %}

{% docs prometheus_metrics_enabled %}

Boolean flag set to True if the Prometheus Metrics endpoint is enabled

{% enddocs %}

{% docs usage_activity_by_stage %}

JSON object containing the `usage_activity_by_stage` metrics

{% enddocs %}

{% docs usage_activity_by_stage_monthly %}

JSON object containing the `usage_activity_by_stage_monthly` metrics

{% enddocs %}

{% docs gitaly_clusters %}

Count of total GitLab Managed clusters, both enabled and disabled

{% enddocs %}

{% docs gitaly_version %}

Version of Gitaly running on the installation (ex. 14.2.1, 15.5.1, etc)

{% enddocs %}

{% docs gitaly_servers %}

Count of total Gitaly servers

{% enddocs %}

{% docs gitaly_filesystems %}

Filesystem data for Gitaly installations

{% enddocs %}

{% docs gitpod_enabled %}

Text flag set to True if Gitpod is enabled. This is not a boolean field, so values are `t` and `f` instead of `TRUE` and `FALSE`

{% enddocs %}

{% docs object_store %}

JSON object containing the `object_store` metrics

{% enddocs %}

{% docs is_dependency_proxy_enabled %}

Boolean flag set to True if the dependency proxy is enabled

{% enddocs %}

{% docs recording_ce_finished_at %}

The time when CE features were computed

{% enddocs %}

{% docs recording_ee_finished_at %}

The time with EE-specific features were computed

{% enddocs %}

{% docs is_ingress_modsecurity_enabled %}

Boolean flag set to True if ModSecurity is enabled within Ingress

{% enddocs %}

{% docs topology %}

JSON object containing the `topology` metrics

{% enddocs %}

{% docs is_grafana_link_enabled %}

Boolean flag set to True if Grafana is enabled

{% enddocs %}

{% docs analytics_unique_visits %}

JSON object containing the `analytics_unique_visits` metrics

{% enddocs %}

{% docs raw_usage_data_id %}

The unique identifier of the raw usage data in the Versions db

{% enddocs %}

{% docs container_registry_vendor %}

The vendor of the container registry (ex. gitlab)

{% enddocs %}

{% docs container_registry_version %}

The version of the container registry in use (ex. 2.11.0-gitlab, 3.60.1-gitlab, etc)

{% enddocs %}

{% docs is_saas_dedicated %}

Boolean flag set to True if the ping is from a Dedicated implementation

{% enddocs %}

{% docs ping_deployment_type %}

Indicates whether the ping comes from a GitLab.com, SaaS Dedicated or Self-Managed instance.

{% enddocs %}

{% docs raw_usage_data_payload %}

Either the original payload value or a reconstructed value. See https://gitlab.com/gitlab-data/analytics/-/merge_requests/4064/diffs#bc1d7221ae33626053b22854f3ecbbfff3ffe633 for rationale.

{% enddocs %}

{% docs license_sha256 %}

The SHA-256 hash of the license file.

{% enddocs %}

{% docs stats_used %}

JSON object containing the `stats` metrics

{% enddocs %}

{% docs license_trial_ends_on %}

The date the trial license ends

{% enddocs %}

{% docs license_subscription_id %}

The unique identifier of a license subscription. This value is synonymous with `dim_subscription_license_id` in other models

{% enddocs %}

{% docs ping_metric_id %}

The unique ID of the dim_ping_metric model consisting of metrics_path, created using the dbt surrogate_key macro

{% enddocs %}

{% docs ping_metric_hist_id %}

The unique composite ID of the dim_ping_metric_daily_snapshot model consisting of metrics_path and date_id

{% enddocs %}

{% docs sql_friendly_path %}

Reformatted metrics_path that reflects how the metrics_path would be filtered from the raw payload. The metrics are prepended with `raw_usage_data_payload`, and `.` are replaced with `[]`. (Ex. `usage_activity_by_stage_monthly.plan.assignee_lists` has the value `raw_usage_data_payload['usage_activity_by_stage_monthly']['plan']['assignee_lists']`)

{% enddocs %}

{% docs description_ping_metric %}

A description of the metric

{% enddocs %}

{% docs skip_validation_ping_metric %}

This will always be NULL

{% enddocs %}

{% docs tier_ping_metric %}

The tier(s) where the tracked feature is available, formatted as an array containing one or a combination of free, premium or ultimate (ex. `[   "free",   "premium",   "ultimate" ]`)

{% enddocs %}

{% docs value_type_ping_metric %}

One of `string`, `number`, `boolean`, `object`

{% enddocs %}

{% docs milestone_ping_metric %}

The milestone when the metric was introduced and when it was available to Self-Managed installations with the official GitLab release

{% enddocs %}

{% docs metrics_status_ping_metric %}

[Status](https://docs.gitlab.com/ee/development/service_ping/metrics_dictionary.html#metric-statuses) of the metric, may be set to `active`, `removed`, or `broken`.

{% enddocs %}

{% docs data_source_ping_metric %}

The source of the metric. May be set to a value like `database`, `redis`, `redis_hll`, `prometheus`, `system`.

{% enddocs %}

{% docs data_category_ping_metric %}

The category of the metric. May be set to a value like `operational`, `optional`, `subscription`, `standard`.

{% enddocs %}

{% docs performance_indicator_type_ping_metric %}

The performance indicator type of the metric. May be set to a value like `gmau`, `smau`, `paid_gmau`, `umau` or `customer_health_score`.

{% enddocs %}

{% docs snapshot_date_ping_metric %}

The date the data was extracted from metrics YAML files

{% enddocs %}

{% docs uploaded_at_ping_metric %}

The time the data was extracted from metrics YAML files

{% enddocs %}

{% docs data_by_row_ping_metric %}

JSON object with all fields from YAML file

{% enddocs %}


{% docs dim_crm_task_sk %}

The unique surrogate key of a [task activity](https://help.salesforce.com/s/articleView?id=sf.tasks.htm&type=5) related to Salesforce account, opportunity, lead, or contact.

{% enddocs %}

{% docs dim_mapped_opportunity_id %}

A secondary key of a solutions-architect related task to be used similarly to `dim_crm_opportunity_id`. It relates an opportunity to a task if there is no opportunity directly related to the task, but if an [opportunity and task belonging to the same account coincide](https://gitlab.com/gitlab-com/sales-team/field-operations/analytics/-/issues/471#note_1519436401) in the same time-frame, they are mapped accordingly.

{% enddocs %}

{% docs task_mapped_to %}

Indicates if the task is mapped directly to an opportunity, if it was mapped to an opportuunity via an account (see `dim_mapped_opportunity_id`), or if it could not be mapped to either type of object.

{% enddocs %}

{% docs snapshot_id %}

The ID of the date the snapshot was valid, easily joined to `dim_date` (YYYYMMDD). This column is often used as the spined date for [date spining](https://discourse.getdbt.com/t/finding-active-days-for-a-subscription-user-account-date-spining/265).

{% enddocs %}

{% docs snapshot_date %}

The date the snapshot record was valid (YYYY-MM-DD)

{% enddocs %}

{% docs dbt_scd_id %}

A unique key generated for each snapshotted record. This is used internally by dbt and is not intended for analysis.

{% enddocs %}

{% docs dbt_created_at_snapshot_model %}

The created_at timestamp of the source record when this snapshot row was inserted. This is used internally by dbt and is not intended for analysis.

{% enddocs %}

{% docs dbt_updated_at_snapshot_model %}

The updated_at timestamp of the source record when this snapshot row was inserted or last updated. This is used internally by dbt and is not intended for analysis.

{% enddocs %}

{% docs dbt_valid_from %}

The timestamp when this snapshot row was first inserted. This column can be used to order the different "versions" of a record.

{% enddocs %}

{% docs dbt_valid_to %}

The timestamp when this row became invalidated. The most recent snapshot record will have `dbt_valid_to` set to null. When a new snapshot record is created, `dbt_valid_to` is updated and will match the new record's `dbt_valid_from` timestamp.

{% enddocs %}

{% docs order_type_name %}

An attribute of an opportunity to designate what type or order it is. This is stamped on the close date of the opportunity. In its latest version, Order Type has logic incorporated to filter out additional CI Minutes and credits as First Order, per the [Salesforce Documentation](https://gitlab.my.salesforce.com/00N4M00000Ib7ly?setupid=OpportunityFields).

{% enddocs %}

{% docs order_type %}

An attribute of an opportunity to designate what type or order it is. This is stamped on the close date of the opportunity. In its latest version, Order Type has logic incorporated to filter out additional CI Minutes and credits as First Order, per the [Salesforce Documentation](https://gitlab.my.salesforce.com/00N4M00000Ib7ly?setupid=OpportunityFields).

{% enddocs %}

{% docs dim_sales_funnel_kpi_sk %}

Surrogate key associated with the sales funnel KPIs.

{% enddocs %}

{% docs sales_funnel_kpi_name %}

The name of the associated sales funnel KPI.

{% enddocs %}

{% docs order_type_current %}

The current Order Type of an opportunity, potentially after it has been stamped on its close date. Per the [documentation in Salesforce](https://gitlab.my.salesforce.com/00N4M00000Ib8Ok?setupid=OpportunityFields), this field is used to track movement of values post deal close and is for analysis purposes only.

{% enddocs %}

{% docs country_name_ping_model %}

The name of the country associated with the IP address of the ping (ex. Australia, France, etc)

{% enddocs %}

{% docs iso_2_country_code_ping_model %}

The two-letter ISO country code associated with the IP address of the ping (ex. AU, FR, etc)

{% enddocs %}

{% docs dim_behavior_browser_sk %}

Surrogate key consisting of browser_name, browser_major_version, browser_minor_version, and browser_language, easily JOINed to dim_behavior_browser. This ID in generated in [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all) using `br_family`, `br_name`, `br_version`, and `br_lang`.

{% enddocs %}

{% docs browser_name %}

The name of the browser family (ex. 'Chrome', 'Firefox', 'Safari', etc). This appears as `br_family` in the raw Snowplow data

{% enddocs %}

{% docs browser_major_version %}

The name and major version of the browser (ex. 'Chrome 10', 'Firefox 10', etc). This appears as `br_name` in the raw Snowplow data

{% enddocs %}

{% docs browser_minor_version %}

The version of the browser (ex. '109.0.0.0', '15.3', etc). This appears as `br_version` in the raw Snowplow data

{% enddocs %}

{% docs browser_engine %}

The browser rendering engine (ex. 'WEBKIT', 'GECKO', etc). This appears as `br_renderengine` in the raw Snowplow data

{% enddocs %}

{% docs browser_language %}

Language the browser is set to (ex. 'en-GB', 'fr-FR', etc). This appears as `br_lang` in the raw Snowplow data

{% enddocs %}

{% docs dim_behavior_event_sk %}

Surrogate key consisting of event, event_name, platform, gsc_environment, event_category, event_action, event_label, event_property, easily JOINed to dim_behavior_event. This ID in generated in [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all) using `event`, `event_name`, `platform`, `gsc_environment`, `se_category`, `se_action`, `se_label` and `se_property`.

{% enddocs %}

{% docs event_behavior_model %}

The type of event i.e if an event is a strutured event OR an unstructured event OR a page view OR a page ping. Snowplow [documentation](https://docs.snowplow.io/docs/understanding-tracking-design/out-of-the-box-vs-custom-events-and-entities/) on types of events.

{% enddocs %}

{% docs event_name_behavior_model %}

Event type of an unstructured event (i.e. change form, click link, submit form etc). Note: WHEN `event=struct` THEN this value will always be `event` and WHEN `event=page_ping` THEN it will be `page_ping` and WHEN `event=page_view` it will be `page_view`

{% enddocs %}

{% docs platform_behavior_model %}

The platform is used to distinguish server side events and web events. When `platform = srv` then the event was a server side event, tracked using Ruby. When `platform = web` then the event was a web event, tracked using Javascript tracker.

{% enddocs %}

{% docs environment_behavior_model %}

Gitlab.com environment (production, staging etc) of the event.

{% enddocs %}

{% docs event_category %}

The event category i.e. The page or backend section of the application. Example: `projects:merge_requests:creations:new`, `InvitesController`, `projects:issues:designs` etc. See [GitLab Event schema for more details](https://docs.gitlab.com/ee/development/snowplow/index.html#event-schema).

Note:
- It is only populated for strutured events (`event=struct`) and **can not be NULL**
- The value of this field is not standardized and depends on implementing engineer

{% enddocs %}

{% docs event_action %}

The action the user takes, or aspect that’s being instrumented. Example: `invite_email_sent`, `join_clicked` etc. See [GitLab Event schema for more details](https://docs.gitlab.com/ee/development/snowplow/index.html#event-schema).

Note:
- It is only populated for strutured events (`event=struct`) and **can not be NULL**
- The value of this field is not standardized and depends on implementing engineer

{% enddocs %}

{% docs event_label %}

An optional string which identifies the specific object being actioned. Example: `invite_email`, `content_editor` etc. See [GitLab Event schema for more details](https://docs.gitlab.com/ee/development/snowplow/index.html#event-schema).

Note:
- It is only populated for strutured events (`event=struct`)
- The value of this field is not standardized and depends on implementing engineer

{% enddocs %}

{% enddocs %}

{% docs clean_event_label %}

An optional string which identifies the specific object being actioned. Example: `invite_email`, `content_editor` etc. See [GitLab Event schema for more details](https://docs.gitlab.com/ee/development/snowplow/index.html#event-schema).

Note:
- It is only populated for strutured events (`event=struct`)
- The value of this field is not standardized and depends on implementing engineer
- It includes [REGEX logic](https://docs.snowflake.com/en/sql-reference/functions/regexp_like) to standardize the namespace identifiers in this field.
Example: If `REGEXP_LIKE(event_label, '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')` THEN `identifier_containing_numbers` ELSE `event_label`.

{% enddocs %}

{% docs event_property %}

An optional string describing the object or the action performed on it. Example: There are four different possible merge request actions: “create”, “merge”, “comment”, and “close”. Each of these would be a possible property value. See [GitLab Event schema for more details](https://docs.gitlab.com/ee/development/snowplow/index.html#event-schema).

Note:
- It is only populated for strutured events (`event=struct`)
- The value of this field is not standardized and depends on implementing engineer

{% enddocs %}

{% docs event_value %}

An optional numeric data to quantify or further describe the user action. Example: `1` could mean success and `0` could mean failure of an event . See [GitLab Event schema for more details](https://docs.gitlab.com/ee/development/snowplow/index.html#event-schema).

Note:
- It is only populated for strutured events (`event=struct`)
- The value of this field is not standardized and depends on implementing engineer

{% enddocs %}

{% docs max_timestamp_behavior_model %}

The timestamp of the last event for that combination of columns/primary key. This is (defined as `MAX(behavior_at)`). The logic behind using max_timestamp is to avoid daily incremental refresh for all dimensions.

{% enddocs %}

{% docs behavior_structured_event_pk %}

This is the Primary key. This ID is generated from [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all) using `event_id`.

{% enddocs %}

{% docs dim_namespace_id_behavior_model %}

The unique identifier of the namespace in which the event was generated, easily joined to `dim_namespace`. This field will be NULL if the event is not tied to a namespace (ex. viewing the To Dos page) and/or if the event occurred before `2021-09-02` (when collection of this data started). This is passed in the GitLab standard context and appears as `gsc_namespace_id` in the raw Snowplow data.

{% enddocs %}

{% docs dim_project_id_behavior_model %}

The unique identifier of the project in which the event was generated, easily joined to `dim_project`. This field will be NULL if the event is not tied to a project (ex. viewing an epic) and/or if the event occurred before `2021-09-02` (when collection of this data started).. This is passed in the GitLab standard context and appears as `gsc_project_id` in the raw Snowplow data.

{% enddocs %}

{% docs dvce_created_tstamp %}

Timestamp for the event recorded on the client device.

{% enddocs %}

{% docs behavior_at %}

Timestamp for when the event actually happened. This appears as `derived_tstamp` in the raw Snowplow data.

{% enddocs %}

{% docs behavior_date %}

The date when the event happened (YYYY-MM-DD). This is based on the `derived_tstamp` field in the raw Snowplow data.

{% enddocs %}

{% docs tracker_version %}

Information about the event tracker version.

{% enddocs %}

{% docs session_index %}

It is the number of the current user session. For example, an event occurring during a user's first session would have session_index set to 1.

{% enddocs %}

{% docs app_id %}

The environment of the event - Production, Staging OR Development. To only include GitLab.com Production events, set filter to `app_id IN ('gitlab','gitlab_customers')`

{% enddocs %}

{% docs session_id %}

Unique identifier for each user session. Note: session_id is NULL for back-end events (`tracker_version LIKE '%rb%'`)

{% enddocs %}

{% docs user_snowplow_domain_id %}

Unique User ID set by Snowplow when the user visits GitLab.com for the first time (using 1st party cookie). This value will remain the same until a user clears their cookies. Note: if a user visits GitLab.com on a different browser, they will have a different unique ID.

{% enddocs %}

{% docs contexts %}

JSON object for custom contexts implemented during tracking implementation. [More information on Snowplow contexts](https://docs.snowplow.io/docs/understanding-your-pipeline/canonical-event/#contexts). [More information on GitLab standard context](https://docs.gitlab.com/ee/development/snowplow/schemas.html#gitlab_standard)

{% enddocs %}

{% docs page_url %}

The page URL example: 'http://www.example.com'.

{% enddocs %}

{% docs page_url_host_path %}

The page URL path of the event **with** the host (gitlab.com) information. Example: `gitlab.com/namespace9495566/project21362945/-/merge_requests/1575`.

{% enddocs %}

{% docs page_url_path %}

The page URL path of the event **without** the host (gitlab.com) information. Example: `/namespace9495566/project21362945/-/merge_requests/1575`

{% enddocs %}

{% docs page_url_scheme %}

Scheme i.e. protocol. Example: 'https'.

{% enddocs %}

{% docs page_url_host %}

Host/Domain information

{% enddocs %}

{% docs dim_behavior_operating_system_sk %}

Surrogate key consisting of os_name and os_timezone, easily JOINed to dim_behavior_operating_system. This ID in generated in [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all) using `os_name` and `os_timezone`.

{% enddocs %}

{% docs os %}

The operating system family (ex. 'Linux', 'iOS', 'Windows'). This appears as `os_family` in the raw Snowplow data

{% enddocs %}

{% docs os_name %}

The name of the operating system (ex. 'Linux', 'Mac OS X', 'Windows 10').

{% enddocs %}

{% docs os_manufacturer %}

The manufacturer of the operating system (ex. 'Apple Inc.', 'Microsoft Corporation')

{% enddocs %}

{% docs os_timezone %}

The timezone of the operating system (ex. 'Europe/London', 'Australia/Sydney')

{% enddocs %}

{% docs device_type %}

The type of device (ex. 'Mobile', 'Computer'). This appears as `dvce_type` in the raw Snowplow data

{% enddocs %}

{% docs is_device_mobile %}

Boolean flag set to True if the event is triggered on a mobile device. This appears as `dvce_ismobile` in the raw Snowplow data

{% enddocs %}

{% docs dim_behavior_website_page_sk %}

Surrogate key consisting of page_url_host_path, app_id and page_url_scheme, easily JOINed to dim_behavior_website_page. This ID in generated in [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all) using `page_url`, `app_id` and `page_url_scheme`

{% enddocs %}

{% docs page_url_query %}

This field is populated with the Querystring passed in the URL. Example: Branch name (`branch_name=masked_branch_name`) or Commit ID (`commit_id=ad604ea134d73261e5ab5c2c92df35d84f04b3c7`)

{% enddocs %}

{% docs clean_url_path %}

This field includes [REGEX logic](https://dbt.gitlabdata.com/#!/macro/macro.gitlab_snowflake.clean_url) to standardize the page_url_path field. Example: If `page_url_path` = `/groups/abc/-/group_members` THEN `clean_url_path` = `groups/abc/group_members`.

{% enddocs %}

{% docs page_group %}

This field is defined as the first part of clean_url_path field i.e if `clean_url_path` = `groups/abc/group_members` THEN `page_group` = `groups`

{% enddocs %}

{% docs page_type %}

This field is defined as the second part of clean_url_path field i.e if `clean_url_path` = `groups/abc/group_members` THEN `page_type` = `abc`

{% enddocs %}

{% docs page_sub_type %}

This field is defined as the third part of clean_url_path field i.e if `clean_url_path` = `groups/abc/group_members` THEN `page_sub_type` = `group_members`

{% enddocs %}

{% docs behavior_url_namespace_id %}

The unique identifier of the namespace passed in the URL. This field is only populated if namespace_id is passed in the page URL.

{% enddocs %}

{% docs behavior_url_project_id %}

The unique identifier of the project passed in the URL. This field is only populated if project_id is passed in the page URL.

{% enddocs %}

{% docs url_path_category %}

This field groups different `clean_url_path` values into distinct categories. Please refer to Code section on this page for logic behind this categorization.

{% enddocs %}

{% docs is_url_interacting_with_security %}

If the page URL is part of any page within the Group/Project Security feature on GitLab.com (Example: Vulnerability Report) THEN `is_url_interacting_with_security = 1` ELSE `is_url_interacting_with_security = 0`

{% enddocs %}

{% docs min_timestamp_behavior_model %}

The timestamp of the first event for that combination of columns/primary key. This field is defined as MIN (behavior_at).

{% enddocs %}

{% docs fct_behavior_website_page_view_sk %}

Primary key consisting of event_id and page_view_end_at. This ID in generated using event_id and page_view_end_at from [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all).

{% enddocs %}

{% docs dim_behavior_referrer_page_sk %}

Surrogate key consisting of referer_url, app_id and referer_url_scheme, easily JOINed to dim_behavior_website_page ON `dim_behavior_website_page_sk`. This ID in generated using `referer_url`, `app_id`, `referer_url_scheme` from [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all).

{% enddocs %}

{% docs page_view_start_at %}

Timestamp of when a web page was first rendered for that `fct_behavior_website_page_view_sk`.

{% enddocs %}

{% docs page_view_end_at %}

Timestamp of when a web page was last rendered for that `fct_behavior_website_page_view_sk`.

{% enddocs %}

{% docs referrer_url %}

The referer URL. Example: 'www.google.de/'

{% enddocs %}

{% docs referer_url_path %}

URL path of the referrer page.

{% enddocs %}

{% docs engaged_seconds %}

Total time (in seconds) user was on the page. This is calculated as difference between page_view_start_at and page_view_end_at.

{% enddocs %}

{% docs page_load_time_in_ms %}

Total time (in milliseconds) for the page to render.

{% enddocs %}

{% docs page_view_index %}

It is the number of page views per user.

{% enddocs %}

{% docs page_view_in_session_index %}

It is the number of the page views per user per session.

{% enddocs %}

{% docs event_id_behavior_model %}

Unique identifier for each event.

{% enddocs %}

{% docs fct_behavior_unstructured_sk %}

Primary key consisting of event_id and behavior_at

{% enddocs %}

{% docs link_click_target_url %}

The target URL on a `link_click` event. This appears as `lc_targeturl` in the raw Snowplow data. This field will only be populated for `link_click` events

{% enddocs %}

{% docs submit_form_id %}

The form ID on a `submit_form` event. This appears as `sf_formid` in the raw Snowplow data. This field will only be populated for `submit_form` events

{% enddocs %}

{% docs change_form_id %}

The form ID on a `change_form` event. This appears as `cf_formid` in the raw Snowplow data. This field will only be populated for `change_form` events

{% enddocs %}

{% docs change_form_type %}

The form type on a `change_form` event. This appears as `cf_type` in the raw Snowplow data. This field will only be populated for `change_form` events

{% enddocs %}

{% docs change_form_element_id %}

The form element ID on a `change_form` event. This appears as `cf_elementid` in the raw Snowplow data. This field will only be populated for `change_form` events

{% enddocs %}

{% docs focus_form_element_id %}

The form element ID on a `focus_form` event. This appears as `ff_elementid` in the raw Snowplow data. This field will only be populated for `focus_form` events

{% enddocs %}

{% docs focus_form_node_name %}

The form node name on a `focus_form` event. This appears as `ff_nodename` in the raw Snowplow data. This field will only be populated for `focus_form` events

{% enddocs %}


{% docs namespace_type %}

The type of namespace: Group, User, or Project.

{% enddocs %}

{% docs visibility_level %}

The visibility setting for the namespace or project: public, private, or internal. More information about namespace and project visibility [here](https://docs.gitlab.com/ee/user/public_access.html)

{% enddocs %}

{% docs page_url_fragment %}

Fragment aka anchor. Ex. For gitlab.com/projects/new#blank_project, the page_url_fragment is `blank_project`

{% enddocs %}

{% docs gs_first_value_date %}

Date when the account reached 10% of license utiliztion. The goal is to reach this within 30 days.

{% enddocs %}

{% docs gs_last_csm_activity_date %}

Last time the CSM had contact with the customer.

{% enddocs %}

{% docs eoa_sentiment %}

Red - customer was unhappy with the announcement and there's potential risk of churn
Yellow - customer exhibited some dissatisfaction with the announcement but likely won't churn
Green - customer responded favourably to the announcement and is a strong candidate to uptier

{% enddocs %}

{% docs gs_health_user_engagement %}

[Customer health score for engaging in meetings, cadence calls, or EBRs](https://about.gitlab.com/handbook/customer-success/customer-health-scoring/#customer-engagement).

{% enddocs %}

{% docs gs_health_cd %}

Customer [health score for CD use case adoption](https://about.gitlab.com/handbook/customer-success/product-usage-data/maturity-scoring/#cd-adoption-scoring).

{% enddocs %}

{% docs gs_health_devsecops %}

Customer [health score for DevSecOps use case adoption](https://about.gitlab.com/handbook/customer-success/product-usage-data/maturity-scoring/#devsecops-adoption-scoring).

{% enddocs %}

{% docs gs_health_ci %}

Customer [health score for CI use case adoption](https://about.gitlab.com/handbook/customer-success/product-usage-data/maturity-scoring/#ci-adoption-scoring).

{% enddocs %}

{% docs gs_health_scm %}

[Customer health score for source code management (SCM) use case adoption](https://about.gitlab.com/handbook/marketing/brand-and-product-marketing/product-and-solution-marketing/usecase-gtm/version-control-collaboration/#adoption-guide).

{% enddocs %}

{% docs dim_oldest_subscription_in_cohort_id %}

Zuora subscriptions can have lineages of linked subscriptions. This field provides the dimension key for the oldest subscription in a lineage. This key can be used to group a subscription lineage together for analysis.

{% enddocs %}

{% docs product_tier_name  %}

A GitLab offering that provides a set of features at a particular price point such as Free, Premium, or Ultimate. This field also includes the delivery type such as SaaS or Self-Managed.

{% enddocs %}

{% docs previous_month_product_tier_name  %}

This is the previous month's product tier. A GitLab offering that provides a set of features at a particular price point such as Free, Premium, or Ultimate. This field also includes the delivery type such as SaaS or Self-Managed.

{% enddocs %}

{% docs product_delivery_type %}

This is the delivery type of GitLab to include either SaaS or Self-Managed.

{% enddocs %}

{% docs product_deployment_type %}

This is the deployment type of GitLab to include either GitLab.com, Dedicated or Self-Managed.

{% enddocs %}

{% docs previous_month_product_delivery_type %}

This is the previous month delivery type. Includes either SaaS or Self-Managed.

{% enddocs %}

{% docs product_ranking %}

This is a field used for analysis and ranks the product tiers. Ultimate is 3, Premium is 2, and Bronze/Starter is 1.

{% enddocs %}

{% docs previous_month_product_ranking %}

This is the previous month product ranking. Ultimate is 3, Premium is 2, and Bronze/Starter is 1.

{% enddocs %}

{% docs type_of_arr_change %}

Types of Delta ARR:

new - ARR for the customer’s first paying month/quarter
expansion - ARR increased from previous month/quarter
contraction - ARR decreased from previous month/quarter
churn - ARR decreased all the way to zero for a customer who was paying in the previous month/quarter
no impact - ARR remained the same from previous month/quarter

{% enddocs %}

{% docs beg_arr %}

The ARR at the beginning of an arr_month.

{% enddocs %}

{% docs beg_quantity %}

The number of licensed users at the beginning of an arr_month.

{% enddocs %}

{% docs seat_change_arr %}

A change in ARR due to the quantity of seats purchased.

{% enddocs %}

{% docs seat_change_quantity %}

A change in the quantity of seats purchased.

{% enddocs %}

{% docs price_change_arr %}

The price changes represents discounts provided to the customer.

{% enddocs %}

{% docs tier_change_arr %}

Change in ARR due to an upward or downward change in the product purchased (i.e., from Premium up to Ultimate).

{% enddocs %}

{% docs end_arr %}

The ARR at the end of the arr_month.

{% enddocs %}

{% docs end_quantity %}

The number of licensed users at the end of the arr_month.

{% enddocs %}

{% docs annual_price_per_seat_change %}

Change in the price per seat paid in the arr_month.

{% enddocs %}

{% docs namespace_is_ultimate_parent %}

Boolean flag which is set to True if the namespace is the ultimate parent.

{% enddocs %}

{% docs subscription_action_type %}

The action to be carried out on the subscription. For example, 'Amend Subscription' or 'Renew Subscription'

{% enddocs %}

{% docs dim_app_release_sk %}

Unique identifier of an application (app) release. Identifies the combination of major, minor and patch version of a release from an specific application.

{% enddocs %}

{% docs app_release_id %}

The unique identifier of the release of an application.

{% enddocs %}

{% docs vsa_readout %}

The sentiment from the Value Stream Assessment readout meeting.

{% enddocs %}

{% docs vsa_start_date_net_arr %}

The stamped Opportunity Net ARR when Value Stream Assessment start date is populated and is after the opportunity created date. Otherwise, will stamp $0.

{% enddocs %}

{% docs vsa_start_date %}

Date of the Value Stream Assessment kickoff call with the prospect/customer.

{% enddocs %}

{% docs vsa_url %}

The URL to the Value Stream Assesment readout presentation.

{% enddocs %}

{% docs vsa_status %}

The status of the Value Stream Assessment.

{% enddocs %}

{% docs vsa_end_date %}

The date the Value Stream Assessment readout is presented to the prospect/customer.

{% enddocs %}

{% docs installation_creation_date %}

Based off of the [`installation_creation_date`](https://gitlab.com/gitlab-org/gitlab/-/blob/master/config/metrics/license/20230228110448_installation_creation_date.yml)
Service Ping metric. For installations where the root user (id = 1) is not deleted, it returns the root user creation date. For installations where the root user
is deleted, it returns the earliest available user creation date.

{% enddocs %}

{% docs last_at_risk_update_comments %}

The most recent Account-level At-Risk Update timeline activity in Gainsight, synced to Salesforce.

{% enddocs %}

{% docs won_arr_basis_for_clari %}

This will be used for forecasting, and depending on the Renewal Forecast field would calculate the Won ARR Basis portion for a given opp. For example, reduce ARR Basis being booked if Net ARR is negative in contractions.

{% enddocs %}

{% docs arr_basis_for_clari %}

This is the ARR Basis of the opportunity, except for contract resets where we are taking it from the subscription. In some cases where we have more than 1 credit opp, we would require a manual override of this value in order to capture the ARR Basis for all the subscriptions being consolidated.

{% enddocs %}

{% docs forecasted_churn_for_clari %}

This field forecasts churn before the opportunity is closed as lost or won if it is contraction.

{% enddocs %}

{% docs override_arr_basis_clari %}

Field that allows for manual overrides in the event when the reset is a consolidation of more than 1 subscription.  This field allows us to manually override the ARR Basis (for Clari) value since we would need to take into account all of the subscriptions consolidated.

{% enddocs %}

{% docs downgrade_details %}

Field where rep can add in more details as to why the customer has downgraded.

{% enddocs %}

{% docs dim_crm_opportunity_id_current_open_renewal %}

The current open renewal opportunity mapped to a subscription.

{% enddocs %}

{% docs dim_crm_opportunity_id_closed_lost_renewal %}

The closed lost renewal opportunity, where applicable, mapped mapped to a subscription.

{% enddocs %}

{% docs dim_oldest_crm_account_in_cohort_id %}

Zuora subscriptions can have lineages of linked subscriptions. This field provides the Account ID from SFDC identifing the customer for the oldest subscription in a lineage.

{% enddocs %}

{% docs oldest_subscription_start_date %}

Zuora subscriptions can have lineages of linked subscriptions. This field provides the start date for the oldest subscription in a lineage. This date can be used to know when customers first started paying and can be utilized for different use cases such as reporting on `Time to X Value`.

{% enddocs %}

{% docs oldest_subscription_cohort_month %}

Zuora subscriptions can have lineages of linked subscriptions. This field provides the start date month for the oldest subscription in a lineage. This cohort month can be used to know when customers first started paying and can be utilized for different use cases such as reporting on `Time to X Value`.

{% enddocs %}

{% docs dim_plan_sk %}

The surrogate key for joining to the `dim_plan` table

{% enddocs %}

{% docs dim_plan_id %}

The id of the plan as given by GitLab.com

{% enddocs %}

{% docs plan_id_modified %}

Modified plan id to conform legacy gold and silver plan ids to ultimate and premium plan ids.

{% enddocs %}

{% docs plan_name %}

The name of the plan as given by GitLab.com

{% enddocs %}

{% docs plan_name_modified %}

Modified plan name to conform legacy gold and silver plan names to ultimate and premium plan names.

{% enddocs %}

{% docs plan_title %}

The title of the plan as given by GitLab.com

{% enddocs %}

{% docs is_plan_paid %}

A flag to indicate if the plan is a paid plan or not.

{% enddocs %}

{% docs namespace_has_code_suggestions_enabled %}

Boolean flag set to True if the namespace has code suggestions enabled. This appears as `code_suggestions` in the gitlab.com db `namespace_settings` table.

{% enddocs %}

{% docs intended_product_tier %}

The intended product tier looking to be purchased for this opportunity.

{% enddocs %}

{% docs dim_parent_crm_opportunity_id %}

The Salesforce opportunity ID for the parent opportunity of this opportunity.

{% enddocs %}


{% docs dim_namespace_order_trial_sk %}

The surrogate key of `prep_namespace_order_trial` model. Currently identified by hashing the namespace_id field that is being sourced from customers portal at gitlab.com.

{% enddocs %}

{% docs trial_type %}

Represents the numerical identifier for different trial offerings in the GitLab product suite as per [this mapping](https://gitlab.com/gitlab-data/analytics/-/issues/21199#trial-type-to-product-mapping). This field is used to categorize trials based on the product or combination of products being tested. It includes trials for various GitLab tiers (e.g., Premium, Ultimate) and add-ons (e.g., Duo Pro, Duo Enterprise), as well as combinations of these offerings.

{% enddocs %}

{% docs trial_type_name %}

Provides a human-readable name corresponding to the trial_type. This field translates the numerical trial_type into descriptive labels that clearly identify the nature of each trial offering. It includes names for standalone product trials as well as combination trials that include multiple GitLab tiers or add-ons.

{% enddocs %}

{% docs dim_trial_latest_sk %}

The surrogate key of `dim_trial_latest` model. Currently identified by hashing the `order_snapshot_id` field that is being sourced from Snapshotted Orders model.

{% enddocs %}


{% docs dim_package_sk %}

The surrogate key of `dim_package` model. Currently identified by hashing the `package_id` field.

{% enddocs %}

{% docs package_id %}

The natural key of `dim_package` model.

{% enddocs %}

{% docs package_version %}

Version of the package in the [package registry](https://docs.gitlab.com/ee/user/packages/package_registry/).

{% enddocs %}

{% docs package_type %}

Type of package in the [package registry](https://docs.gitlab.com/ee/user/packages/package_registry/).

{% enddocs %}

{% docs package_created_at %}

Date the package was created.

{% enddocs %}

{% docs package_updated_at %}

Latest date the package was updated.

{% enddocs %}

{% docs dim_integration_sk %}

The surrogate key of `dim_integration` model. Currently identified by hashing the `integration_id` field.

{% enddocs %}

{% docs integration_id %}

The natural key of `dim_integration` model.

{% enddocs %}

{% docs integration_is_active %}

Boolean representing if an integration is active or not.

{% enddocs %}

{% docs integration_created_at %}

The date a integration was created.

{% enddocs %}

{% docs integration_updated_at %}

The latest date a service was updated.

{% enddocs %}

{% docs integration_type %}

Text field describing the type of integration (ex. Integrations::Discord, Integrations::Slack, Integrations::Prometheus, Integrations::Bamboo).

{% enddocs %}

{% docs integration_category %}

Text field describing the broader category of the integration (ex.CI, monitoring, deployment, chat).

{% enddocs %}

{% docs dim_requirement_sk %}

The surrogate key of `dim_requirement` model. Currently identified by hashing the `requirement_id` field.

{% enddocs %}

{% docs requirement_id %}

The natural key of `dim_requirement` model.

{% enddocs %}

{% docs requirement_internal_id %}

An identifier for requirements in the `dim_requirement` model that is project-specific.

{% enddocs %}

{% docs requirement_state %}

The state of the requirement (Opened/Archived) as defined by this [code](https://gitlab.com/gitlab-org/gitlab/-/blob/886e4652e57ef41b4ecdfeb9c42183467b625f72/ee/app/models/requirement.rb).

{% enddocs %}

{% docs created_date_id %}

The ID of the created date, easily joined to `dim_date`

{% enddocs %}

{% docs requirement_created_at %}

The date the requirement was created.

{% enddocs %}

{% docs requirement_updated_at %}

The date the requirement was last updated.

{% enddocs %}

{% docs dim_snippet_sk %}

The surrogate key of `dim_snippet` model. Currently identified by hashing the `snippet_id` field.

{% enddocs %}

{% docs snippet_id %}

The natural key of `dim_snippet` model.

{% enddocs %}

{% docs snippet_type %}

Identifies a snippet as a `Personal Snippet` or `Project Snippet`.

{% enddocs %}

{% docs snippet_created_at %}

The date a snippet was created.

{% enddocs %}

{% docs snippet_updated_at %}

The latest date a snippet was updated.

{% enddocs %}

{% docs is_trial_converted %}

Boolean flag to indicate if the Trial order has been converted to Paid Order or not

{% enddocs %}

{% docs dim_plan_id_at_creation %}

The GitLab plan id at the time of object creation.

{% enddocs %}

{% docs is_internal_epic %}

Boolean flag set to True if the epic's namespace is identified as an internal GitLab namespace.

{% enddocs %}

{% docs cycle_time_in_days %}

Number of days since created date of opportunity till its closure. For renewal opportunities created date = ARR created date

{% enddocs %}

{% docs is_health_score_metric %}

 Boolean flag set to True if the metric is used in Gainsight for customer health scoring.

{% enddocs %}

{% docs is_lighthouse_metric %}

 Boolean flag set to True if the metric is a [Lightouse Metric](https://internal.gitlab.com/handbook/enterprise-data/data-catalog/product-adoption-lighthouse-metrics/).

{% enddocs %}

{% docs is_leading_indicator_metric %}

 Boolean flag set to True if the metric is a [Leading Indicator](https://internal.gitlab.com/handbook/enterprise-data/data-catalog/product-adoption-lighthouse-metrics/).

{% enddocs %}

{% docs report_target_date %}

Target_Date + 1. This is used in Sisense when comparing QTD targets vs actuals for the current date.

{% enddocs %}

{% docs wtd_allocated_target %}

Week To Date allocated target.

{% enddocs %}

{% docs mtd_allocated_target %}

Month To Date allocated target.

{% enddocs %}

{% docs qtd_allocated_target %}

Quarter To Date allocated target.

{% enddocs %}

{% docs ytd_allocated_target %}

Year To Date allocated target.

{% enddocs %}

{% docs dim_cloud_activation_sk %}

The surrogate key of `prep_cloud_activation` model. Currently identified by hashing the `cloud_activation_id` field that is being sourced from customers portal at gitlab.com.

{% enddocs %}

{% docs dim_cloud_activation_id %}

The unique identifier that identifies a cloud activation.

{% enddocs %}

{% docs dim_crm_current_account_set_hierarchy_sk %}

Sales hierarchy surrogate key that accounts for yearly changes in the sales hierarchy. Views all hierarchies through the lens of the current year hierarchy, and it reflects how the sales hierarchy is applied in practice. It choose between the opportunity owner stamped hierarchy or the account owner live user hierarchy:

1. If the fiscal year of the live close_date of the opportunity is less than the current fiscal year, use the account owner live user hierarchy
2. If the fiscal year of the live close_date of the opportunity is greater than or equal to the current fiscal year, use the opportunity owner stamped hierarchy

{% enddocs %}

{% docs dim_crm_current_account_set_sales_segment_id %}

Sales segment key that accounts for yearly changes in the sales hierarchy.

Follows the same rules as the field `dim_crm_current_account_set_hierarchy_sk`

{% enddocs %}

{% docs dim_crm_current_account_set_geo_id %}

Geo key that accounts for yearly changes in the sales hierarchy.

Follows the same rules as the field `dim_crm_current_account_set_hierarchy_sk`

{% enddocs %}

{% docs dim_crm_current_account_set_region_id %}

Region key that accounts for yearly changes in the sales hierarchy.

Follows the same rules as the field `dim_crm_current_account_set_hierarchy_sk`

{% enddocs %}

{% docs dim_crm_current_account_set_area_id %}

Area key that accounts for yearly changes in the sales hierarchy.

Follows the same rules as the field `dim_crm_current_account_set_hierarchy_sk`

{% enddocs %}

{% docs dim_crm_current_account_set_business_unit_id %}

Business Unit key that accounts for yearly changes in the sales hierarchy.

Follows the same rules as the field `dim_crm_current_account_set_hierarchy_sk`

{% enddocs %}

{% docs label_title %}

String of labels associated with a GitLab object (issue, merge request, epic, etc.).

{% enddocs %}

{% docs namespace_name_unmasked %}

Top-level namespace name that is unmasked for any user who has been assigned the appropriate access.

{% enddocs %}

{% docs dim_milestone_sk %}

The surrogate key for milestones.

{% enddocs %}

{% docs milestone_id %}

The natural key for milestones.

{% enddocs %}

{% docs dim_milestone_id %}

The legacy unique identifier for milestones.

{% enddocs %}

{% docs milestone_created_at %}

The date a milestone was created.

{% enddocs %}

{% docs milestone_updated_at %}

The most date a milestone was most recently updated.

{% enddocs %}

{% docs milestone_created_date_id %}

The id associated with the milestone's created date that can be joined to `dim_date`.

{% enddocs %}

{% docs milestone_title %}

The title of a milestone. Only available for internal milestones.

{% enddocs %}

{% docs milestone_description %}

A text field description of the milestone.

{% enddocs %}

{% docs milestone_start_date %}

The date the milestone is set to begin.

{% enddocs %}

{% docs milestone_due_date %}

The date the milestone is set to end.

{% enddocs %}

{% docs milestone_status %}

A status indicating if the milestone is `Active` or `Closed`.

{% enddocs %}

{% docs next_ping_uploaded_at %}

The date of the next ping for the uuid and the host id. Will be the current ping upload_at if it is the current ping.

{% enddocs %}

{% docs gitlab_standard_context %}

Standard fields added to Snowplow events by GitLab as defined in the [schema](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/gitlab_standard/jsonschema)

{% enddocs %}

{% docs gitlab_standard_context_schema %}

Schema version of the Snowplow event for events with the [GitLab Standard Context](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/gitlab_standard/jsonschema).

{% enddocs %}

{% docs has_gitlab_standard_context %}

Flag indicating the Snowplow event includes fields from the [GitLab Standard Context](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/gitlab_standard/jsonschema).

{% enddocs %}

{% docs gsc_environment %}

Name of the source environment, such as `production` or `staging.

{% enddocs %}

{% docs gsc_extra %}

Any additional data associated with the event, in the form of key-value pairs.

{% enddocs %}

{% docs gsc_namespace_id %}

ID of the namespace associated with the Snowplow event.

{% enddocs %}

{% docs gsc_plan %}

Name of the plan, such as  `free`, `bronze`, `silver`, `premium`, `gold` or `ultimate` sent on the Snowplow event.

{% enddocs %}

{% docs gsc_google_analytics_id %}

Google Analytics ID from the marketing site

{% enddocs %}

{% docs gsc_google_analytics_client_id %}

Google Analytics Client ID calculated from the Google Analytics ID from the marketing site

{% enddocs %}

{% docs gsc_project_id %}

ID of the project associated with the Snowplow event

{% enddocs %}

{% docs gsc_pseudonymized_user_id %}

User database record ID attribute. This value undergoes a pseudonymization process at the collector level. Note: This field is only populated after a user successfully registers on GitLab.com i.e. they verify their e-mail and log-in for the first time. This value will be NULL in the following situations:

- The event occurred before `2021-09-29` (when the collection of this data started)
- A user is not logged in
- The event occurs on a page outside of the SaaS product (ex. about.gitlab.com, docs.gitlab.com)
- It is an unstructured event
- The event is not associated with a user (some backend events)

{% enddocs %}

{% docs gsc_source %}

Name of the source application/ event tracker, such as gitlab-rails or gitlab-javascript. This field can be used to distinguish front-end events V/S back-end events. When `gsc_source = 'gitlab-rails'` THEN back-end event i.e. event was tracked using Ruby. When `gsc_source = 'gitlab-javascript'` THEN front-end event i.e. event was tracked using Javascript.

{% enddocs %}

{% docs gsc_is_gitlab_team_member %}

Boolean flag that aims to distinguish between GitLab team members and non-team members. This is defined using the [gitlab-com namespace members list](https://gitlab.com/groups/gitlab-com/-/group_members), meaning that non-team members like contractors and core team members could trigger `gsc_is_gitlab_team_member = TRUE`. When `gsc_is_gitlab_team_member = TRUE`, the event was triggered by a member of the gitlab-com namespace. When `gsc_is_gitlab_team_member = FALSE`, the event was not triggered by a member of the gitlab-com namespace.

{% enddocs %}

{% docs gsc_feature_enabled_by_namespace_ids %}

List of namespaces that allow the user to use the tracked feature. This list does not have to be 1:1 with the event and does not necessarily correspond to where the event took place.

{% enddocs %}

{% docs gsc_instance_version %}

Version of the GitLab instance where the event comes from.

{% enddocs %}

{% docs gsc_correlation_id %}

Unique request id for each request in Snowplow.

{% enddocs %}

{% docs interface %}

Interface where the Snowplow request originates from, e.g. Duo Chat

{% enddocs %}

{% docs client_type %}

Client type which sends the request (e.g. 'ide', 'browser') inferred from client_name

{% enddocs %}

{% docs client_name %}

Client name which sends the request (e.g. 'chrome', 'firefox', 'vscode', 'jetbrains') inferred from user agent

{% enddocs %}

{% docs client_version %}

Client version which sends the request (e.g. '1.4.5') inferred from user agent

{% enddocs %}

{% docs feature_category %}

Category where the specific feature belongs to (e.g. duo chat related features) recorded in the Snowplow standard context

{% enddocs %}

{% docs web_page_context %}

Web page fields added to Snowplow as defined in the [schema](https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0)

{% enddocs %}

{% docs web_page_context_schema %}

Schema version of the Snowplow event for events with the [Web Page Context](https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0).

{% enddocs %}

{% docs has_web_page_context %}

Flag indicating the Snowplow event includes fields from the [Web Page Context](https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0).

{% enddocs %}

{% docs web_page_id %}

Web page id provided by Snowplow event.

{% enddocs %}

{% docs gitlab_experiment_context %}

Experiment fields added to Snowplow as defined by GitLab in the [schema](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/gitlab_experiment/jsonschema).

{% enddocs %}

{% docs gitlab_experiment_context_schema %}

Schema version of the Snowplow event for events with the [GitLab Experiment Context](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/gitlab_experiment/jsonschema).

{% enddocs %}

{% docs has_gitlab_experiment_context %}

Flag indicating the Snowplow event includes fields from the [GitLab Experiment Context](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/gitlab_experiment/jsonschema).

{% enddocs %}

{% docs experiment_name %}

The name of the experiment as per implementation. More details on [Experimentation Design](https://about.gitlab.com/handbook/product/product-analysis/experimentation/#event-requirements)

{% enddocs %}

{% docs experiment_variant %}

Experiment group (control/candidate) to which the event belongs to. More details on [Experimentation Design](https://about.gitlab.com/handbook/product/product-analysis/experimentation/#event-requirements)

{% enddocs %}

{% docs experiment_context_key %}

The value passed in `key` section of the `experiment` context json. [More information on Snowplow contexts](https://docs.snowplow.io/docs/understanding-your-pipeline/canonical-event/#contexts). More details on [Experimentation Design](https://about.gitlab.com/handbook/product/product-analysis/experimentation/#event-requirements)

{% enddocs %}

{% docs experiment_migration_keys %}

This column may contain a list of migration keys.

{% enddocs %}

{% docs code_suggestions_context %}

Code Suggestion fields added to Snowplow as defined by GitLab in the [schema](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/code_suggestions_context/jsonschema).

{% enddocs %}

{% docs code_suggestions_context_schema %}

Schema version of the Snowplow event for events with the [Code Suggestions Context](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/code_suggestions_context/jsonschema).

{% enddocs %}

{% docs has_code_suggestions_context %}

Flag indicating the Snowplow event includes fields from the [Code Suggestions Context](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/code_suggestions_context/jsonschema).

{% enddocs %}

{% docs code_suggestions_model_engine %}

Model engine used for the completions.

{% enddocs %}

{% docs code_suggestions_model_name %}

Model name used for the completions.

{% enddocs %}

{% docs code_suggestions_prefix_length %}

Length of the prefix in characters.

{% enddocs %}

{% docs code_suggestions_suffix_length %}

Length of the suffix in characters.

{% enddocs %}

{% docs code_suggestions_language %}

Programming language of the completions request.

{% enddocs %}

{% docs code_suggestions_user_agent %}

User-agent string of the request (holds information about the origin of the request).

{% enddocs %}

{% docs code_suggestions_api_status_code %}

HTTP status code of GitLab API.

{% enddocs %}

{% docs code_suggestions_duo_namespace_ids %}

List of the namespace IDs that the user has a Duo Pro Add-Onn provisioned from (available GitLab.com only).

{% enddocs %}

{% docs code_suggestions_saas_namespace_ids %}

List of the namespace IDs that the user has a Code Suggestions subscription in SaaS for.

{% enddocs %}

{% docs code_suggestions_namespace_ids %}

Coalesced list of namespace ids from `duo_namespace_ids` and `saas_namespace_ids` to create a complete list of namespaces that provide access to Code Suggestions/Duo Pro.

{% enddocs %}

{% docs code_suggestions_is_streaming %}

Code suggestions can be returned to the user in a single response or as a stream (in chunks). This boolean field distinguishes between the streamed and non-streamed responses.

{% enddocs %}

{% docs code_suggestions_is_invoked %}

Flag to indicate whether the request for a suggestion was triggered automatically while the user was typing or invoked by the user hovering over the suggestion to get more options.

{% enddocs %}

{% docs code_suggestions_options_count %}

The total number of options provided for the current suggestion. A user can select from one of these options.

{% enddocs %}

{% docs code_suggestions_accepted_option %}

When the suggestion is accepted, this field indicates the option number which was chosen by the user out of the possible options provided. This is a 1-based index.

{% enddocs %}

{% docs code_suggestions_has_advanced_context %}

Flag indicating whether the suggestion was requested with additional context. NULL means that the feature flags for the advanced context feature are not enabled

{% enddocs %}

{% docs code_suggestions_is_direct_connection %}

Flag indicating whether the suggestion request was sent to monolith or directly to AI gateway.

{% enddocs %}

{% docs code_suggestions_suggestion_source %}

Source where the suggestion is retrieved from. This can be either the cache or network.

{% enddocs %}

{% docs code_suggestions_delivery_type %}

This is the delivery type of GitLab to include either SaaS or Self-Managed.

To derive this attribute, we join the instance information (`instance_id`, `host_name`) sent in the Code Suggestions context to find the delivery type from the installation that sent the event. We coalesce this delivery type with the `realm` (delivery type) sent in the context. This adjusts for the `Dedicated` instances that are labeled as Self-Managed in the context, but are considered to be SaaS delivery types.

{% enddocs %}

{% docs code_suggestions_total_context_size_bytes %}

Total byte size of all context items in request

{% enddocs %}

{% docs code_suggestions_content_above_cursor_size_bytes %}

Total byte size of text above cursor

{% enddocs %}

{% docs code_suggestions_content_below_cursor_size_bytes %}

Total byte size of text below cursor

{% enddocs %}

{% docs code_suggestions_context_items %}

Set of final context items sent to AI Gateway

{% enddocs %}

{% docs code_suggestions_resolution_strategy %}

Strategy used to resolve the context item

{% enddocs %}

{% docs code_suggestions_context_items_count %}

The count of context items sent to AI Gateway

{% enddocs %}

{% docs code_suggestions_input_tokens %}

Total tokens used in request to model provider

{% enddocs %}

{% docs code_suggestions_output_tokens %}

Total output tokens received from model provider

{% enddocs %}

{% docs code_suggestions_context_tokens_sent %}

Total tokens sent as context to AI Gateway

{% enddocs %}

{% docs code_suggestions_context_tokens_used %}

Total context tokens used in request to model provider

{% enddocs %}

{% docs code_suggestions_debounce_interval%}

Number of milliseconds we wait between last keystroke and API request for Code Suggestions

{% enddocs %}

{% docs code_suggestions_region%}

GCP location where the code suggestions request was processed

{% enddocs %}

{% docs ide_extension_version_context %}

IDE extension version fields added to Snowplow as defined by GitLab in the [schema](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/ide_extension_version/jsonschema).

{% enddocs %}

{% docs ide_extension_version_context_schema %}

Schema version of the Snowplow event for events with the [IDE Extension Version Context](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/ide_extension_version/jsonschema).

{% enddocs %}

{% docs has_ide_extension_version_context %}

Flag indicating the Snowplow event includes fields from the [IDE Extension Version Context](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/ide_extension_version/jsonschema.

{% enddocs %}

{% docs extension_name %}

Name of the IDE extension, e.g. GitLab Workflow.

{% enddocs %}

{% docs extension_version %}

Version number of the IDE extension, e.g. 3.81.1.

{% enddocs %}

{% docs ide_name %}

Name of the IDE, e.g. RubyMibe.

{% enddocs %}

{% docs ide_vendor %}

Name of the IDEs vendor, e.g. Microsoft.

{% enddocs %}

{% docs ide_version %}

Version number of the IDE, e.g. 1.81.1.

{% enddocs %}

{% docs language_server_version %}

Version number of the Language Server, e.g. 3.9.0.

{% enddocs %}

{% docs gitlab_service_ping_context %}

Service Ping fields added to Snowplow as defined by GitLab in the [schema](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/gitlab_service_ping/jsonschema).

{% enddocs %}

{% docs gitlab_service_ping_context_schema %}

Schema version of the Snowplow event for events with the [GitLab Service Ping Context](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/gitlab_service_ping/jsonschema).

{% enddocs %}

{% docs has_gitlab_service_ping_context %}

Flag indicating the Snowplow event includes fields from the [GitLab Service Ping Context](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/gitlab_service_ping/jsonschema).

{% enddocs %}

{% docs redis_event_name %}

Name of the redis event (for Redis HLL/unique metrics) or the redis key (for Redis/total metrics).

{% enddocs %}

{% docs service_ping_key_path %}

`key_path` attribute from metrics YAML definition. This is a legacy field and is combined with `data_source` to get full coverage on metrics.

{% enddocs %}

{% docs service_ping_data_source %}

`data_source` attribute from metrics YAML definition.

{% enddocs %}

{% docs performance_timing_context %}

Performance Timing fields added to Snowplow as defined in the [schema](https://github.com/snowplow/iglu-central/blob/master/schemas/org.w3/PerformanceTiming/jsonschema/1-0-0).

Definitions for these attributes can be found [here](https://www.w3.org/TR/navigation-timing/#sec-navigation-timing-interface).

{% enddocs %}

{% docs performance_timing_context_schema %}

Schema version of the Snowplow event for events with the [Performance Timing Context](https://github.com/snowplow/iglu-central/blob/master/schemas/org.w3/PerformanceTiming/jsonschema/1-0-0).

Definitions for these attributes can be found [here](https://www.w3.org/TR/navigation-timing/#sec-navigation-timing-interface).

{% enddocs %}

{% docs has_performance_timing_context %}

Flag indicating the Snowplow event includes fields from the [Performance Timing Context](https://github.com/snowplow/iglu-central/blob/master/schemas/org.w3/PerformanceTiming/jsonschema/1-0-0).

{% enddocs %}

{% docs connect_end %}

This attribute must return the time immediately after the user agent finishes establishing the connection to the server to retrieve the current document. If a persistent connection is used or the current document is retrieved from relevant application caches or local resources, this attribute must return the value of domainLookupEnd

If the transport connection fails and the user agent reopens a connection, connectStart and connectEnd should return the corresponding values of the new connection.

`connect_end` must include the time interval to establish the transport connection as well as other time interval such as SSL handshake and SOCKS authentication.

{% enddocs %}

{% docs connect_start %}

This attribute must return the time immediately before the user agent start establishing the connection to the server to retrieve the document. If a persistent connection is used or the current document is retrieved from relevant application caches or local resources, this attribute must return value of domainLookupEnd.

{% enddocs %}

{% docs dom_complete %}

This attribute must return the time immediately before the user agent sets the current document readiness to "complete".

If the current document readiness changes to the same state multiple times, `dom_loading`, `dom_interactive`, `dom_content_loaded_event_start`, `dom_content_loaded_event_end` and `dom_complete` must return the time of the first occurrence of the corresponding document readiness change.

{% enddocs %}

{% docs dom_content_loaded_event_end %}

This attribute must return the time immediately after the document's `dom_content_loaded` event completes.

{% enddocs %}

{% docs dom_content_loaded_event_start %}

This attribute must return the time immediately before the user agent fires the `dom_content_loaded` event at the Document.

{% enddocs %}

{% docs dom_interactive %}

This attribute must return the time immediately before the user agent sets the current document readiness to "interactive".

{% enddocs %}

{% docs dom_loading %}

This attribute must return the time immediately before the user agent sets the current document readiness to "loading".

{% enddocs %}

{% docs domain_lookup_end %}

This attribute must return the time immediately after the user agent finishes the domain name lookup for the current document. If a persistent connection is used or the current document is retrieved from relevant application caches or local resources, this attribute must return the same value as `fetch_start`.

{% enddocs %}

{% docs domain_lookup_start %}

This attribute must return the time immediately before the user agent starts the domain name lookup for the current document. If a persistent connection is used or the current document is retrieved from relevant application caches or local resources, this attribute must return the same value as `fetch_start`.

{% enddocs %}

{% docs fetch_start %}

If the new resource is to be fetched using HTTP GET or equivalent, `fetch_start` must return the time immediately before the user agent starts checking any relevant application caches. Otherwise, it must return the time when the user agent starts fetching the resource.

{% enddocs %}

{% docs load_event_end %}

This attribute must return the time when the load event of the current document is completed. It must return zero when the load event is not fired or is not completed.

{% enddocs %}

{% docs load_event_start %}

This attribute must return the time immediately before the load event of the current document is fired. It must return zero when the load event is not fired yet.

{% enddocs %}

{% docs navigation_start %}

This attribute must return the time immediately after the user agent finishes prompting to unload the previous document. If there is no previous document, this attribute must return the same value as `fetch_start`.

{% enddocs %}

{% docs redirect_end %}

If there are HTTP redirects or equivalent when navigating and all redirects and equivalents are from the same origin, this attribute must return the time immediately after receiving the last byte of the response of the last redirect. Otherwise, this attribute must return zero.

{% enddocs %}

{% docs redirect_start %}

If there are HTTP redirects or equivalent when navigating and if all the redirects or equivalent are from the same origin, this attribute must return the starting time of the fetch that initiates the redirect. Otherwise, this attribute must return zero.

{% enddocs %}

{% docs request_start %}

This attribute must return the time immediately before the user agent starts requesting the current document from the server, or from relevant application caches or from local resources.

If the transport connection fails after a request is sent and the user agent reopens a connection and resend the request, `request_start` should return the corresponding values of the new request.

This interface does not include an attribute to represent the completion of sending the request, e.g., `request_end`.

Completion of sending the request from the user agent does not always indicate the corresponding completion time in the network transport, which brings most of the benefit of having such an attribute.
Some user agents have high cost to determine the actual completion time of sending the request due to the HTTP layer encapsulation.

{% enddocs %}

{% docs response_end %}

This attribute must return the time immediately after the user agent receives the last byte of the current document or immediately before the transport connection is closed, whichever comes first. The document here can be received either from the server, relevant application caches or from local resources.

{% enddocs %}

{% docs response_start %}

This attribute must return the time immediately after the user agent receives the first byte of the response from the server, or from relevant application caches or from local resources.

{% enddocs %}

{% docs secure_connection_start %}

If the scheme of the current page is HTTPS, this attribute must return the time immediately before the user agent starts the handshake process to secure the current connection. If this attribute is available but HTTPS is not used, this attribute must return zero.

{% enddocs %}

{% docs unload_event_end %}

If the previous document and the current document have the same same origin, this attribute must return the time immediately after the user agent finishes the unload event of the previous document. If there is no previous document or the previous document has a different origin than the current document or the unload is not yet completed, this attribute must return zero.

If there are HTTP redirects or equivalent when navigating and not all the redirects or equivalent are from the same origin, both `unload_event_start` and `unload_event_end` must return the zero.

{% enddocs %}

{% docs unload_event_start %}

If the previous document and the current document have the same origin, this attribute must return the time immediately before the user agent starts the unload event of the previous document. If there is no previous document or the previous document has a different origin than the current document, this attribute must return zero.

{% enddocs %}

{% docs code_suggestions_ultimate_parent_namespace_ids %}

All ultimate parent namespace ids mapped to the list of namespace ids that granted the user access to Code Suggestions for this event.

{% enddocs %}

{% docs code_suggestions_subscription_names %}

All subscriptions mapped to the list of namespace ids that granted the user access to Code Suggestions for this event.

{% enddocs %}

{% docs code_suggestions_dim_crm_account_ids %}

All CRM account ids mapped to the list of namespace ids that granted the user access to Code Suggestions for this event.

{% enddocs %}

{% docs code_suggestions_dim_parent_crm_account_ids %}

All ultimate parent CRM account ids mapped to the list of namespace ids that granted the user access to Code Suggestions for this event.

{% enddocs %}

{% docs code_suggestions_crm_account_names %}

All CRM account names mapped to the list of namespace ids that granted the user access to Code Suggestions for this event.

{% enddocs %}

{% docs code_suggestions_parent_crm_account_names %}

All ultimate parent CRM account names mapped to the list of namespace ids that granted the user access to Code Suggestions for this event.

{% enddocs %}

{% docs code_suggestions_host_names %}

All host names mapped to the instance id associated with the Code Suggestion event.

{% enddocs %}

{% docs code_suggestions_dim_installation_ids %}

All installation ids mapped to the instance id associated with the Code Suggestion event.

{% enddocs %}

{% docs code_suggestions_dim_crm_account_id %}

If only one `dim_crm_account_id` can be mapped to the Code Suggestion event through the namespace or instance id, then it is listed here.

{% enddocs %}

{% docs code_suggestions_dim_parent_crm_account_id %}

If only one `dim_parent_crm_account_id` can be mapped to the Code Suggestion event through the namespace or instance id, then it is listed here.

{% enddocs %}

{% docs code_suggestions_subscription_name %}

If only one `subscription_name` can be mapped to the Code Suggestion event through the namespace or instance id, then it is listed here.

{% enddocs %}

{% docs code_suggestions_crm_account_name %}

If only one `crm_account_name` can be mapped to the Code Suggestion event through the namespace or instance id, then it is listed here.

{% enddocs %}

{% docs code_suggestions_parent_crm_account_name %}

If only one ultimate parent `crm_account_name` can be mapped to the Code Suggestion event through the namespace or instance id, then it is listed here.

{% enddocs %}

{% docs code_suggestions_dim_installation_id %}

If only one `dim_installation_id` can be mapped to the Code Suggestion event through the instance id, then it is listed here.

{% enddocs %}

{% docs code_suggestions_installation_host_name %}

If only one `host_name` can be mapped to the Code Suggestion event through the instance id, then it is listed here.

{% enddocs %}

{% docs code_suggestions_ultimate_parent_namespace_id %}

If only one ultimate parent namespace id can be mapped to the Code Suggestion event through the namespace or instance id, then it is listed here.

{% enddocs %}

{% docs code_suggestions_namespace_is_internal %}

Flag indicates if any namespace associated with the Code Suggestion is internal. Since a Code Suggestion can permissioned at the user-level, it has a 1:many relationship with namespaces, so the event can be associated with multiple namespace IDs. We look at all possible associations to verify if the event is internal.

{% enddocs %}

{% docs code_suggestions_installation_is_internal %}

Flag indicates if any installation associated with the Code Suggestion is internal to GitLab. Since a Code Suggestion event can be associated with multiple installations (stored in the `dim_installation_ids` array), we evaluate all installations to determine if any are internal.

This flag is derived from the `is_internal` column in the `dim_installation` table and is set to TRUE if at least one installation ID in the array matches an entry in the `internal_gitlab_installations` seed.

{% enddocs %}

{% docs code_suggestions_suggestion_id %}

The unique identifier for the code suggestion. Appears as `event_label` on the Snowplow events

{% enddocs %}

{% docs code_suggestions_requested_at %}

Timestamp of the `suggestion_requested` event. This event is sent when the IDE extension requests a suggestion from the backend.

{% enddocs %}

{% docs code_suggestions_loaded_at %}

Timestamp of the `suggestion_loaded` event. This event is sent when the suggestion request returns without error.

{% enddocs %}

{% docs code_suggestions_shown_at %}

Timestamp of the `suggestion_shown` event. This event is sent when the suggestion is shown to the user.

{% enddocs %}

{% docs code_suggestions_accepted_at %}

Timestamp of the `suggestion_accepted` event. This event is sent when the suggestion was shown and then accepted by the user.

{% enddocs %}

{% docs code_suggestions_rejected_at %}

Timestamp of the `suggestion_rejected` event. This event is sent when the suggestion was shown and then rejected by the user.

{% enddocs %}

{% docs code_suggestions_cancelled_at %}

Timestamp of the `suggestion_cancelled` event. This event is sent when the suggestion request was canceled and not shown. For example, the user starts typing again before the suggestion is shown.

{% enddocs %}

{% docs code_suggestions_not_provided_at %}

Timestamp of the `suggestion_not_provided` event. This event is sent when no suggestion was provided that could be shown to the user. This can happen if the suggestion does not meet a rule designed to limit poor suggestions from being shown. For example, the suggestion is all whitespace characters.

{% enddocs %}

{% docs code_suggestions_error_at %}

Timestamp of the `suggestion_error` event. This event is sent when the suggestion request leads to an error.

{% enddocs %}

{% docs code_suggestions_load_time_in_ms %}

The difference (in ms) between the `suggestion_requested` and `suggestion_loaded` events

{% enddocs %}

{% docs code_suggestions_display_time_in_ms %}

The difference (in ms) between the `suggestion_shown` and `suggestion_accepted` or `suggestion_rejected` events

{% enddocs %}

{% docs code_suggestions_suggestion_outcome %}

The outcome of the suggestion (ex. `suggestion_accepted`, `suggestion_rejected`, `suggestion_cancelled`, `suggestion_error`, etc)

{% enddocs %}

{% docs code_suggestions_was_requested %}

Boolean flag set to `TRUE` if the suggestion had a `suggestion_requested` event. This event is sent when the IDE extension requests a suggestion from the backend. This field will always be TRUE.

{% enddocs %}

{% docs code_suggestions_was_loaded %}

Boolean flag set to `TRUE` if the suggestion had a `suggestion_loaded` event. This event is sent when the suggestion request returns without error.

{% enddocs %}

{% docs code_suggestions_was_shown %}

Boolean flag set to `TRUE` if the suggestion had a `suggestion_shown` event. This event is sent when the suggestion is shown to the user.

{% enddocs %}

{% docs code_suggestions_was_accepted %}

Boolean flag set to `TRUE` if the suggestion had a `suggestion_accepted` event. This event is sent when the suggestion was shown and then accepted by the user.

{% enddocs %}

{% docs code_suggestions_was_rejected %}

Boolean flag set to `TRUE` if the suggestion had a `suggestion_rejected` event. This event is sent when the suggestion was shown and then rejected by the user.

{% enddocs %}

{% docs code_suggestions_was_cancelled %}

Boolean flag set to `TRUE` if the suggestion had a `suggestion_cancelled` event. This event is sent when the suggestion request was canceled and not shown. For example, the user starts typing again before the suggestion is shown.

{% enddocs %}

{% docs code_suggestions_was_not_provided %}

Boolean flag set to `TRUE` if the suggestion had a `suggestion_not_provided` event. This event is sent when no suggestion was provided that could be shown to the user. This can happen if the suggestion does not meet a rule designed to limit poor suggestions from being shown. For example, the suggestion is all whitespace characters.

{% enddocs %}

{% docs code_suggestions_was_error %}

Boolean flag set to `TRUE` if the suggestion had a `suggestion_error` event. This event is sent when the suggestion request leads to an error.

{% enddocs %}

{% docs code_suggestions_was_stream_started %}

Boolean flag set to `TRUE` if the suggestion had a `suggestion_stream_started` event. This event is sent when the first chunk of the suggestion stream was returned from the network.

{% enddocs %}

{% docs code_suggestions_was_stream_completed %}

Boolean flag set to `TRUE` if the suggestion had a `suggestion_stream_completed` event. This event is sent when the last chunk of the streamed suggestion was returned from the network.

{% enddocs %}

{% docs dimensions_checked_at %}

A timestamp used for checking when the last time the dimensions for the record were updated.

{% enddocs %}

{% docs has_merge_trains_enabled %}

Flag to indicate a project has turned on the [Merge Trains](https://docs.gitlab.com/ee/ci/pipelines/merge_trains.html) feature

{% enddocs %}

{% docs cost_factor %}

For Shared Runners, whenever a user runs jobs on a specific machine type the cost to GitLab needs to be multiplied by a cost factor as the larger the machine the more expensive it is to run.

{% enddocs %}

{% docs ping_instance_metric_monthly_pk %}

Primary key for the model, consisting of dim_installation_id, metrics_path, and ping_created_date_month

{% enddocs %}

{% docs is_deleted %}

The flag indicating if the record has been deleted

{% enddocs %}

{% docs is_deleted_updated_at %}

The timestamp of when the `is_deleted` flag has been updated

{% enddocs %}

{% docs gitlab_global_user_id %}

Pseudonymised combination of instance id and user id coalesced from the `global_user_id` sent in the GitLab Standard Context and the `gitlab_global_user_id` sent in Code Suggestions events.

{% enddocs %}

{% docs user_type_id %}

Numeric id indicating the type of user in GitLab.com data.

{% enddocs %}

{% docs user_type %}

User type name based on the [mapping](https://gitlab.com/gitlab-org/gitlab/-/blob/master/app/models/concerns/has_user_type.rb) to translate the ids to human-readable types.

{% enddocs %}

{% docs is_bot %}

Calculated field based on the [mapping](https://gitlab.com/gitlab-org/gitlab/-/blob/master/app/models/concerns/has_user_type.rb) of bot user types.

{% enddocs %}

{% docs cloud_connector_backend_services %}

Name of the backend who is serving the feature.

{% enddocs %}

{% docs cloud_connector_feature_category %}

Name of the cloud connector feature such as code_suggestions, duo_chat etc.

{% enddocs %}

{% docs cloud_connector_unit_primitive_name %}

Name of the unit primitive bundled with the add-on. Unit primitive is a smallest logical feature that a permission/access scope can govern.

{% enddocs %}

{% docs cloud_connector_unit_primitive_description %}

Description of the unit primitive bundled with the add-on. Unit primitive is a smallest logical feature that a permission/access scope can govern.

{% enddocs %}

{% docs cloud_connector_cut_off_date %}

The earliest date when the feature requires add-on seat purchase.

{% enddocs %}

{% docs cloud_connector_min_gitlab_version %}

Minimum required GitLab version to use the feature.

{% enddocs %}

{% docs cloud_connector_min_gitlab_version_for_free_access %}

Minimum required GitLab version to use the feature during free_access period (beta). If it's not set, the feature is available for all GitLab versions.

{% enddocs %}
{% docs cloud_connector_engineering_group %}

Engineering group who maintains the unit primitive.

{% enddocs %}

{% docs cloud_connector_available_to_license_types %}

Name of the license(s) under which the group of unit primitives are bundled and sold together.

{% enddocs %}

{% docs cloud_connector_available_to_add_ons %}

Name of the add-on(s) under which the group of unit primitives are bundled and sold together. For Ex - code_suggestions and duo_chat are 2 UP sold together under `DUO_PRO` add-on.

{% enddocs %}

{% docs cloud_connector_documentation_url %}

URL linking to unit primitive documentation.

{% enddocs %}

{% docs cloud_connector_introduced_by_url %}

URL linking to MR where unit primitive was introduced.

{% enddocs %}

{% docs cloud_connector_unit_primitive_issue_url %}

URL linking to issue where unit primitive was introduced.

{% enddocs %}

{% docs cloud_connector_deprecated_by_url %}

URL linking to MR where unit primitive was deprecated

{% enddocs %}

{% docs cloud_connector_deprecation_message %}

 Describes unit primitive deprecation

{% enddocs %}

{% docs subscription_created_datetime %}

Datetime the subscription was created.

{% enddocs %}

{% docs subscription_updated_datetime %}

Datetime the subscription was updated.

{% enddocs %}

{% docs subscription_created_datetime_adjusted %}

Some subscriptions are created after the effective date of the subscription. In order to properly attribute subscriptions across the entire effective date, we adjust the created date to reflect the minimum between the first created date (version 1 of the subscription) and the subscription_start_date for that subscription version.

{% enddocs %}

{% docs next_subscription_created_datetime %}

The created date of the next subscription in the lineage (version+1). If there is no next version of the subscription, this field will be filled in with the GREATER of the subscription_end_date and the CURRENT_DATE.

{% enddocs %}

{% docs rate_plan_charge_number %}

A unique number that identifies the charge. This number is returned as a string.

{% enddocs %}

{% docs rate_plan_charge_version %}

The version of the rate plan charge. Each time a charge is amended, Zuora creates a new version of the rate plan charge.

{% enddocs %}

{% docs rate_plan_charge_segment %}

The identifying number of the subscription rate plan segment. Segments are numbered sequentially, starting with 1.

Zuora returns segmented rate plan charges, which are referred to as rate plan charge segments or charge segments. Every time an amendment (or an order action) changes a charge (except the Remove Product amendment), Zuora creates a new segmented RatePlanCharge object.

{% enddocs %}

{% docs rate_plan_name %}

A rate plan is part of a subscription or an amendment to a subscription, and it comes from a product rate plan. This field displays the name of the rate plan.

{% enddocs %}

{% docs rate_plan_charge_name %}

The name of the rate plan charge.

{% enddocs %}

{% docs rate_plan_charge_description %}

A description of the charge.

{% enddocs %}

{% docs rate_plan_charge_amendement_type %}

The type of amendment associated with the charge.

{% enddocs %}

{% docs dim_charge_id %}

Unique identifier of the rate plan charge.

{% enddocs %}

{% docs dim_product_detail_id %}

Unique identifier of the product rate plan charge.

A product rate plan charge represents a charge model or a set of fees associated with a product rate plan, which is the part of a product that your customers subscribe to. Each product rate plan can have multiple product rate plan charges.

{% enddocs %}

{% docs dim_amendment_id_charge %}

Unique identifier of the amendment associated with a charge.

{% enddocs %}

{% docs effective_start_date_id %}

The date id when the segmented charge starts or started that can be joined to dim_date.

{% enddocs %}

{% docs effective_end_date_id %}

The date id when the segmented charge ends or ended that can be joined to dim_date.

{% enddocs %}

{% docs is_last_segment %}

Indicates if the segment of the rate plan charge is the most recent segment.

{% enddocs %}

{% docs discount_level %}

Specifies if the discount applies to just the product rate plan, the entire subscription, or to any activity in the account.

{% enddocs %}

{% docs unit_of_measure %}

Specifies the units to measure usage.

{% enddocs %}

{% docs is_paid_in_full %}

When the Charged Through Date is equal to the Effective End Month, flag the charge as paid in full.

{% enddocs %}

{% docs months_of_future_billings %}

If the Charged Through Date is null, then take the Current Term from the subscription, otherwise calcualte the months of future billings as the number of months between the Charged Through Date and the Effective End Month of the charge.

{% enddocs %}

{% docs is_included_in_arr_calc %}

Charges can be marked cancelled by upating the effective dates. We do not want to include these charges in a final ARR calculation, so we mark them for exclusion.

When the Effective End Month > Effective Start Month OR Effective End Month is null, then include the records in the ARR calculation.

{% enddocs %}

{% docs charged_through_date %}

The date through which a customer has been billed for the charge.

{% enddocs %}

{% docs charge_created_date %}

Date the rate plan charge was created.

{% enddocs %}

{% docs charge_created_datetime %}

Datetime the rate plan charge was created.

{% enddocs %}

{% docs charge_updated_date %}

Date the rate plan charge was last updated.

{% enddocs %}

{% docs charge_term %}

The number of months between the effective start date and the effective end date of a charge.

{% enddocs %}

{% docs billing_period %}

Allows billing period to be overridden on rate plan charge.

{% enddocs %}

{% docs mrr %}

Monthly recurring revenue (MRR) is the amount of recurring charges in a given month. The MRR calculation doesn't include one-time charges nor usage charges. This field returns MRR of a particular charge segment.

{% enddocs %}

{% docs previous_mrr_calc %}

If the previous MRR is NULL, then replace it with 0.

{% enddocs %}

{% docs previous_mrr %}

Previous MRR of the rate plan charge.

{% enddocs %}

{% docs delta_mrr_calc %}

Difference between MRR and the previous MRR of a charge

{% enddocs %}

{% docs delta_mrr %}

When a subscription is active, the subscription ended before today and this charge is the last segment of the charges, the delta_mrr is
equal to `-previous_mrr`.

When the subscription is cancelled and it is the last segment of the charge, then the delta_mrr is equal to `-previous_mrr`.

Otherwise, delta_mrr is equal to the difference between MRR and the previous MRR of a charge.

{% enddocs %}

{% docs delta_mrc %}

A delta monthly recurring charge is the change in monthly recurring revenue caused by an amendment or a new subscription. This is the value at the charge segment level.

{% enddocs %}

{% docs arr %}

Monthly recurring revenue * 12

{% enddocs %}

{% docs previous_arr %}

Previous ARR associated with the

{% enddocs %}

{% docs delta_arc %}

delta_mrc * 12

{% enddocs %}

{% docs delta_arr %}

delta_mrr * 12

{% enddocs %}

{% docs list_price %}

{% enddocs %}

{% docs extended_list_price %}

The list price base for the product rate plan charge.

{% enddocs %}

{% docs quantity %}

The default quantity of units, such as the number of authors in a hosted wiki service. Valid for all charge models except for Flat Fee pricing.

{% enddocs %}

{% docs previous_quantity_calc %}

The previous quantity associated with the charge.

{% enddocs %}

{% docs previous_quantity %}

When previous quantity is NULL, replace the value with 0.

{% enddocs %}

{% docs delta_quantity_calc %}

quantity - previous_quantity

{% enddocs %}

{% docs delta_quantity %}

When a subscription is active, the subscription ended before today and this charge is the last segment of the charges, the delta_mrr is
equal to `-previous_quantity`.

When the subscription is cancelled and it is the last segment of the charge, then the delta_mrr is equal to `-previous_quantity`.

Otherwise, delta_quantity is equal to the difference between MRR and the previous quantity of a charge.

{% enddocs %}

{% docs tcv %}

The total contract value (TCV) is the value of a single rate plan charge at the segment level in a subscription over the lifetime of the subscription. This value does not represent all charges on the subscription. The TCV includes recurring charges and one-time charges, but it doesn't include usage charge.

{% enddocs %}

{% docs delta_tcv %}

The difference in TCV between two charges.

{% enddocs %}

{% docs estimated_total_future_billings %}

When a charge is not paid in full, then multiply the MRR by the months of future billings remaining.
{% enddocs %}

{% docs is_manual_charge %}

Flag to indicate if a charge was added through a manual process outside of the billing system.

{% enddocs %}

{% docs dim_order_id %}

Unique identifier of an order.

{% enddocs %}

{% docs subscription_status %}

The status of the subscription.

{% enddocs %}

{% docs subscription_start_date %}

The date when the subscription term starts. This date is the same as the start date of the original term, which isn't necessarily the start date of the current or new term.

{% enddocs %}

{% docs subscription_end_date %}

The date when the subscription term ends, where the subscription ends at midnight the day before.

{% enddocs %}

{% docs effective_start_date %}

The date when the segmented charge starts or started.

{% enddocs %}

{% docs effective_end_date %}

The date when the segmented charge ends or ended.

{% enddocs %}

{% docs effective_start_month %}

The month when the segmented charge starts or started.

{% enddocs %}

{% docs effective_end_month %}

The month when the segmented charge ends or ended.

{% enddocs %}

{% docs is_current_date %}

Flag to filter a given model to today's data.

{% enddocs %}

{% docs first_commit_at %}

Time of first commit on a merge request.

{% enddocs %}

{% docs is_licensed_user %}

Flag to identify SKUs that should whose quantities should be counted as licensed users. It works by excluding `Storage` product_tier, administration fees, and storage costs.

{% enddocs %}

{% docs is_licensed_user_base_product %}

Flag to identify SKUs that should whose quantities should be counted as licensed users for the base GitLab application. It works by excluding `Storage` product_tier, administration fees, storage costs, SKUs that are not in the "Base Products" `product_category`, and various rate plan charges that correspond to support SKUs.

{% enddocs %}

{% docs is_licensed_user_add_on %}

Flag to identify SKUs that should whose quantities should be counted as licensed users for add ons. It works by excluding `Storage` product_tier, administration fees, storage costs, and SKUs that are in the "Base Products" `product_category`.

{% enddocs %}

{% docs duo_pro_license_users_coalesced %}

Coalesced `Duo Pro Licensed Users` count from multiple sources in priority order:
1. Zuora (Single Source of Truth for both GitLab.com and Self-Managed)
2. Service Ping (Self-Managed fallback)
3. Seat Link (final Self-Managed fallback)

For Self-Managed, when Zuora data is unavailable, Service Ping is used as it's the source
for many other critical Self-Managed metrics. Seat Link data is used only if both Zuora
and Service Ping data are unavailable.

{% enddocs %}

{% docs duo_pro_billable_users_coalesced %}

Coalesced `Duo Pro Billable Users` count from multiple sources in priority order:

1. Service Ping (primary for Self-Managed)
2. Seat Link (Self-Managed fallback)

For Self-Managed, Service Ping is prioritized as it's the source for many critical Self-Managed metrics. Seat Link data is used only if Service Ping data is unavailable.
For GitLab.com, future implementation will rely on assignment tables which is not added yet.

{% enddocs %}

{% docs duo_enterprise_license_users_coalesced %}

Coalesced `Duo Enterprise Licensed Users` count from multiple sources in priority order:

1. Zuora (Single Source of Truth for both GitLab.com and Self-Managed)
2. Service Ping (Self-Managed fallback)
3. Seat Link (final Self-Managed fallback)

For Self-Managed, when Zuora data is unavailable, Service Ping is used as it's the source for many critical Self-Managed metrics. Seat Link data is used only if both Zuora and Service Ping data are unavailable.

{% enddocs %}

{% docs duo_enterprise_billable_users_coalesced %}

Coalesced `Duo Enterprise Billable Users` count from multiple sources in priority order:

1. Service Ping (primary for Self-Managed)
2. Seat Link (Self-Managed fallback)

For Self-Managed, Service Ping is prioritized as it's the source for many critical Self-Managed metrics. Seat Link data is used only if Service Ping data is unavailable.
For GitLab.com, future implementation will rely on assignment tables which is not added yet.

{% enddocs %}

{% docs is_null_user %}

Indicates if the user is NULL (does not exist).

{% enddocs %}

{% docs latest_event_date_timestamp %}

The latest event_date_timestamp for that grain. Used in downstream fct_event_namespace_monthly model to calculate the plan.

{% enddocs %}

{% docs pipeline_name_internal_only %}

Name of the pipeline. Populated only for internal pipelines.

{% enddocs %}

{% docs finance_pl_category %}

Finance allocation for Profit & Loss reporting. This buckets internal costs into Free, Paid, or Internal.

{% enddocs %}

{% docs close_fiscal_quarter_name_for_pipegen_grouping %}

This field can be used for grouping Pipeline Generation metrics in SFDC Opportunity Snapshot models.

Logic:
1. If arr_created_date is in the current fiscal quarter or in future fiscal quarters, the live close_fiscal_quarter_name is returned.
2. If arr_created_date is in past fiscal quarters, the snapshotted close_fiscal_quarter_name is returned.

{% enddocs %}

{% docs commit_count %}

Total commits associated with a project

{% enddocs %}

{% docs storage_size %}

Amount of storage used by a project

{% enddocs %}

{% docs repository_size %}

Total repository size of a project

{% enddocs %}

{% docs container_registry_size %}

Total container registry size of a project

{% enddocs %}

{% docs lfs_objects_size %}

Total Git Large Files size of a project

{% enddocs %}

{% docs build_artifacts_size %}

Total build artifacts size of a project

{% enddocs %}

{% docs packages_size %}

Total packages size of a project

{% enddocs %}

{% docs wiki_size %}

Total wiki size of a project

{% enddocs %}

{% docs shared_runners_seconds %}

Number of shared runner seconds used by a project

{% enddocs %}

{% docs build_artifacts_gb %}

Total build artifacts size of a project in gigabytes

{% enddocs %}

{% docs repo_size_gb %}

Total repository size of a project in gigabytes

{% enddocs %}

{% docs container_registry_gb %}

Total container registry size of a project in gigabytes

{% enddocs %}

{% docs scheduling_type %}

[Enum](https://gitlab.com/gitlab-org/gitlab/-/blob/3dc80fdbc2098f5b2e87a9601daa9eb5e2d2d685/app/models/commit_status.rb#L40) denoting whether a build is a DAG.

{% enddocs %}
