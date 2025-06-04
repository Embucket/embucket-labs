{% docs commonroom_activities_source %}

Activity shows the types of engagement happening across your community - joins, comments, replies, retweets, RSVPs - anything you can monitor within a given signal. Activity is essentially your community newsfeed. It gives you a complete look at who’s saying what and where, whether it’s with other Contacts or to your organization directly.

[More details](https://docs.commonroom.io/using-common-room/activity-page).

{% enddocs %}

{% docs commonroom_community_members_source %}

Community member source.

{% enddocs %}


{% docs commonroom_organizations_source %}

An organization reflects real organizations _(and the Contacts within them)_ in the real world—not just how they may appear in someone’s database. This means if you have multiple accounts and opportunities from Salesforce related to a single company, or if you have multiple domains related to a company for product usage, all of these will be merged into a single organization profile.

[More details](https://docs.commonroom.io/using-common-room/organizations-page).

{% enddocs %}

{% docs _uploaded_at %}

A moment when the record was uploaded into Snowflake. Stored in the `TIMESTAMP` format.

{% enddocs %}

{% docs commonroom_primary_key %}

The `primary_key` column combination of multiple columns to ensure uniqueness for the deduplication. 
No no use this column to join the data as it is internal column.


{% enddocs %}


{% docs commonroom_member_ids %}

`member_ids` is a comma separated string of the contact's tokens.

{% enddocs %}

{% docs _file_name %}

An original file name from where a record come from. The main purpose is for tracking the lineage and data history.

{% enddocs %}