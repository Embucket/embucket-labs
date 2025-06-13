{% docs zuora_subscription %}

This table is the base table for Zuora subscriptions.

It removes all subscriptions that are marked as "Exclude from Analysis". It also creates a slug of the subscription name and the renewal subscription name (if it exists) using the user-defined function (UDF) "zuora_slugify". There is a test specifically to validate this.

This table also sets the zuora_renewal_subscription_name_slugify field as an array. This is important because we need to do deduplication downstream and can't have a fan out at this stage. Storing the data as an array enables us to still clean it with the slugify UDF while still having a single row.

#### Zuora Slugify UDF
This function is used to simplify Zuora subscriptions names. Subscription names, by default, are unique. However, they can have basically any character they want in them, including arbitrary amounts of whitespace. The original idea was to truly slugify the name but that wasn't unique enough across subscriptions. We did have an issue where links to renewal subscriptions were not being copied correctly. Creating a slug that simply replaces most characters with hyphens maintained uniqueness but made it easier to reason about how the data was incorrectly formatted. It in the end it may not be wholly necessary, but from a programming perspective it is a bit easier to reason about.

{% enddocs %}
