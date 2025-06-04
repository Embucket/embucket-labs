{% docs sfdc_account_source %}

The account source table contains info about the individual accounts (organizations and persons) involved with your business. This could be a customer, a competitor, a partner, and so on. [Link to Documentation](https://www.stitchdata.com/docs/integrations/saas/salesforce/#account)

Note: A number of fields prefixed with JB_ and ATAM_ are pulled in as part of [Territory Success Planning](https://about.gitlab.com/handbook/sales/field-operations/sales-operations/go-to-market/#territory-success-planning-tsp). These are cast as fields prefixed with TSP_ in downstream models to distinguish from equivalent "Actual" fields reflecting the current Go-To-Market approach.

{% enddocs %}

{% docs sfdc_bizible_source %}

Bizible generated table pulled via Salesforce. Source of truth for Bizible data.

{% enddocs %}

{% docs sfdc_campaignmember_source %}

The campaign member source table represents the association between a campaign and either a lead or a contact. [Link to Documentation](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_campaignmember.htm)

{% enddocs %}

{% docs sfdc_campaign_source %}

This campaign source table represents and tracks a marketing campaign, such as a direct mail promotion, webinar, or trade show. [Link to Documentation](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_campaign.htm)

{% enddocs %}

{% docs sfdc_case_source %}

The case source table represents a case, which is a customer issue or problem. [Link to Documentation](https://developer.salesforce.com/docs/atlas.en-us.234.0.object_reference.meta/object_reference/sforce_api_objects_case.htm)

{% enddocs %}

{% docs sfdc_case_history_source %}

The case history table includes any updates to Case fields with field history tracking enabled. [Link to Documentation](https://developer.salesforce.com/docs/atlas.en-us.234.0.object_reference.meta/object_reference/sforce_api_objects_casehistory.htm)

{% enddocs %}

{% docs sfdc_contact_source %}

The contact source table contains info about your contacts, who are individuals associated with accounts in your Salesforce instance. [Link to Documentation](https://www.stitchdata.com/docs/integrations/saas/salesforce/#contact)

{% enddocs %}

{% docs sfdc_event_source %}

The event source table represents an event in the calendar. In the user interface, event and task records are collectively referred to as activities.
 [Link to Documentation](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_event.htm)

{% enddocs %}

{% docs sfdc_execbus_source %}

Custom source table: This table contains executive business review data.

{% enddocs %}

{% docs sfdc_hg_insights_technographics_source %}

This table contains snapshotted HG Insights Technographic data for accounts and leads.

{% enddocs %}

{% docs sfdc_lead_source %}

The lead source table contains info about your leads, who are prospects or potential Opportunities. [Link to Documentation](https://www.stitchdata.com/docs/integrations/saas/salesforce/#lead)

Note: A number of fields prefixed with JB_ and ATAM_ are pulled in as part of [Territory Success Planning](https://about.gitlab.com/handbook/sales/field-operations/sales-operations/go-to-market/#territory-success-planning-tsp). These are cast as fields prefixed with TSP_ in downstream models to distinguish from equivalent "Actual" fields reflecting the current Go-To-Market approach.

{% enddocs %}

{% docs sfdc_opportunity_product_source %}

Source model for SFDC opportunity products. [Link to Epic](https://gitlab.com/groups/gitlab-com/business-technology/enterprise-apps/-/epics/527)

{% enddocs %}

{% docs sfdc_oppstage_source %}

The opportunity stage source table represents the stage of an Opportunity in the sales pipeline, such as New Lead, Negotiating, Pending, Closed, and so on. [Link to Documentation](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_opportunitystage.htm)

{% enddocs %}

{% docs sfdc_opp_source %}

The opportunity source table contains info about your opportunities, which are sales or pending deals. [Link to Documentation](https://www.stitchdata.com/docs/integrations/saas/salesforce/#opportunity)

{% enddocs %}

{% docs sfdc_opportunity_split %}

OpportunitySplit credits one or more opportunity team members with a portion of the opportunity amount.
[See original issue](https://gitlab.com/gitlab-data/analytics/-/issues/6073)
[Opportunity Split Docs](https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_objects_opportunitysplit.htm)

{% enddocs %}

{% docs sfdc_opportunity_split_type %}

Added as references for the sfdc_opportunity_split data
[See original issue](https://gitlab.com/gitlab-data/analytics/-/issues/6073)
[Opportunity Split Type Docs](https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_objects_opportunitysplittype.htm)

{% enddocs %}

{% docs sfdc_opportunity_team_member %}

Represents a User on the opportunity team of an Opportunity.
[See original issue](https://gitlab.com/gitlab-data/analytics/-/issues/6073)
[Opportunity Team member Docs](https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_objects_opportunitysplit.htm)

{% enddocs %}
{% docs sfdc_contact_role_source %}

The opportunity contact role is a junction object between Opportunities and contacts that facilitates a many:many relationship. [Link to Salesforce Documentation](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_opportunitycontactrole.htm)

{% enddocs %}

{% docs sfdc_pov_source %}

This table contains data on the proof of value. Note the API name for this source remained proof_of_concept after renaming of the SFDC custom object.

{% enddocs %}

{% docs sfdc_professional_services_engagement_source %}

This table contains data on the professional services engagement. Note the API name for this source remained statement_of_work after renaming of the SFDC custom object.

{% enddocs %}

{% docs sfdc_quote_source %}

This table contains data on Zuora quotes generated for opportunities. This provides the SSOT mapping between opportunities and subscriptions for quotes where `status = 'Sent to Z-Billing'` and `is_primary_quote = TRUE`
.
{% enddocs %}

{% docs sfdc_recordtype_source %}

The record type source table represents a record type. [Link to Documentation](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_recordtype.htm)

{% enddocs %}

{% docs sfdc_task_source %}

The task source table represents a business activity such as making a phone call or other to-do items. In the user interface, Task and Event records are collectively referred to as activities. [Link to Documentation](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_task.htm)

{% enddocs %}

{% docs sfdc_userrole_source %}

The user role source table represents a user role in your organization. [Link to Documentation](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_role.htm)

{% enddocs %}

{% docs sfdc_user_source %}

The user source table contains info about the users in your organization. [Link to Documentation](https://www.stitchdata.com/docs/integrations/saas/salesforce/#user)

{% enddocs %}

{% docs sfdc_acct_arch_source %}

This is the source table for the archived Salesforce accounts.

{% enddocs %}

{% docs sfdc_opp_arch_source %}

This is the source table for the archived Salesforce opportunities.

{% enddocs %}

{% docs sfdc_users_arch_source %}

This is the source table for the archived Salesforce users.

{% enddocs %}

{% docs sfdc_oppfieldhistory_source %}

This is the source table for Opportunity Field History.

{% enddocs %}

{% docs sfdc_contacthistory_source %}

This is the source table for Contact Field History.

{% enddocs %}

{% docs sfdc_leadhistory_source %}

This is the source table for Lead Field History.

{% enddocs %}

{% docs sfdc_accounthistory_source %}

This is the source table for Account Field History.

{% enddocs %}

{% docs sfdc_opphistory_source %}

This is the source table for Opportunity History.

{% enddocs %}

{% docs sfdc_zqu_quote_rate_plan_source %}

Source model for SFDC custom object representing a quote rate plan from Zuora.

{% enddocs %}

{% docs sfdc_zqu_quote_rate_plan_charge_source %}

Source model for SFDC custom object representing a quote rate plan charge from Zuora.

{% enddocs %}

{% docs sfdc_customer_subscription_source %}

Custom source table: This table contains customer subscription data.

{% enddocs %}

{% docs sfdc_zoom_source %}

Source models for salesforce and zoom integrated objects.

{% enddocs %}

{% docs sfdc_permission_set_assignment_source %}

Source model for SFDC Permission Set Assignment object. Represents a group of permission sets and the permissions within them. Use permission set groups to organize permissions based on job functions or tasks. Then, you can package the groups as needed.

{% enddocs %}

{% docs sfdc_profile_source %}

Source model for SFDC Profile object. Represents a profile, which defines a set of permissions to perform different operations. Operations can include creating a custom profile or querying, adding, updating, or deleting information.

{% enddocs %}

{% docs sfdc_group_member_source %}

 Source model for SFDC Group Member object. Represents a User or Group that is a member of a public group.

{% enddocs %}

{% docs sfdc_group_source %}

Source model for SFDC Group object. Group are sets of users. They can contain individual users, other groups, the users in particular role of territory, or the users in a particular role of territory plus all the users below that role or territory in the hierarchy.

{% enddocs %}

{% docs sfdc_account_share_source %}

Source model for SFDC Account Share object. For a given account, the user or group that has access based on the account id.
  
{% enddocs %}

{% docs sfdc_account_team_member_source %}

Source model for SFDC Account Team Member object. Represents a User who is a member of an Account team.
  
{% enddocs %}

{% docs sfdc_opportunity_share_source %}

Source model for SFDC Opportunity Share object. For a given opportunity, the user or group that has access based on the opportunity id.

{% enddocs %}

{% docs sfdc_vartopia_drs_registration_source %}

Source model for SFDC custom object from Vartopia with all Deal Registraitons 

{% enddocs %}

{% docs sfdc_impartner_mdf_funds_claim_source  %}

Source model for SFDC MDF Funds Claims

{% enddocs %}

{% docs sfdc_impartner_mdf_funds_request_source  %}

Source model for SFDC MDF Funds Requests

{% enddocs %}

{% docs sfdc_user_territory_association_source  %}

Source model for SFDC user territory association

{% enddocs %}

{% docs sfdc_zqu_product_rate_plan_source  %}

Source model for SFDC custom object product rate plan source representing a product rate plan from Zuora

{% enddocs %}

{% docs sfdc_zqu_zproduct_source  %}

Source model for SFDC custom object product source representing a product from Zuora

{% enddocs %}