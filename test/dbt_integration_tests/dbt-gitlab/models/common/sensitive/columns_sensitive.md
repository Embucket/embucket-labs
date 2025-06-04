{% docs namespaces_array %}

Array of objects containing information related to each ultimate parent namespace that the marketing contact is directly related to. Each object inside the array is a different namespace.

For the trial fields in the objects, in case a namespace has had multiple trials only the latest one will be used to populate the fields. If a namespace never had a trials, these fields will not exist in the object.

Each object in the array contains the following fields (in case it does not, it means that the attribute is not applicable to that namespace):

- **Namespace_id**: Gitlab.com namespace_id of the namespace in question.
- **user_access_level**: Numerical value representing the access level the marketing user contact has to the namespace. Mapping of the numerical values can be seen [HERE](https://docs.gitlab.com/ee/api/access_requests.html).
- **user_access_level_name**: Name of the access level the marketing user contact has to the namespace.
- **gitlab_plan_title**: Plan of the namespace.
- **gitlab_plan_is_paid**: Flag indicating whether the namespace is paid or not.
- **is_setup_for_company**: Flag indicating whether the namespace was set up for company use or not.
- **current_member_count**: Number of members in the namespace
- **created_at**: Creation timestamp of the namespace.
- **creator_user_id**: The gitlab_dotcom_user_id of the creator of the namespace.
- **is_setup_for_company**: Flag indicating whether the namespace was set up for company use or not.
- **trial_start_date**: Trial start date of the namespace. 
- **trial_expired_date**: Date in which the trial expires/expired at. 
- **trial_type**: Indicates the type of trial offering, such as Premium/Ultimate subscription or GitLab Duo Pro add-on. The default value is 1 for both Premium and Ultimate trials, and 2 for GitLab DuoPro trials.
- **trial_type_name**: Specifies the name of the trial offering type. The value 1 corresponds to Premium/Ultimate trials, while 2 corresponds to GitLab Duo Pro trials.
- **is_active_trial**: Flag that indicates whether the namespace is in an active trial. 
- **glm_content**: GLM content of the trial. 
- **glm_source**: GLM source of the trial. 
- **is_namespace_pql**: Flag indicating whether the namespace is part of the PQL program (Product Qualified Lead). 
- **list_of_stages**: Array listing all the stages adopted by the namespace during Trial and/or Free process. 
- **nbr_integrations_installed**: Number integrations installed in the namespace. 
- **integrations_installed**: Array listing all the integrations installed in the namespace. 
- **ptp_source**: Source of the propensity to purchase model. Currently, that can be from either PtPT (Propensity to Purchase: Trial) or PtPF (Propensity to Purchase: Free) models. 
- **ptp_score_date**: Date in which the ptp_score_group was calculated. 
- **ptp_score_group**: Score group for the namespace given by the PtP machine learning model. 
- **ptp_insights**: List of insights on the PtP scores. 
- **ptp_previous_score_group**: Previous score group for the namespace given by the PtP machine learning model. 
- **user_limit_notification_at**: Datetime detailing when the user limit notification happened for the namespace. 
- **user_limit_enforcement_at**: Datetime detailing when the user limit enforcement will happen for the namespace. 
- **is_impacted_by_user_limit**: Flag indicating whether the namespace is impacted by the user limits project. 

{% enddocs %}