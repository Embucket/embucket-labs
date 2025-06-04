## Source

{% docs workday %}
The HRIS system for GitLab
{% enddocs %}

## Tables
{% docs workday_compensation %}

A custom report built in workday for the transition between BambooHR and Workday as the HRIS tool.

| Workday Notes                |                                                                                                                                                                                                                     |
|------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| BONUS_ID                     | Empty - looks like this is a snowflake created values                                                                                                                                                               |
| EMPLOYEE_ID                  | True Employee ID, not BHR ID. Sample Employee IDs above, not real Employees                                                                                                                                         |
| EFFECTIVE_DATE               | Effective Date of the Compensation Change                                                                                                                                                                           |
| COMPENSATION_TYPE            | Salary or Hourly                                                                                                                                                                                                    |
| COMPENSATION_CHANGE_REASON   | Reason for Change                                                                                                                                                                                                   |
| PAY_RATE                     | This will show how often a Team Member is paid.  Values are Monthly, Twice a month, Every other Week                                                                                                                |
| COMPENSATION_VALUE           | Annualized Salary for Team Member.  If they work less than 40 hrs/week - the prorated salary will show here.                                                                                                        |
| COMPENSATION_CURRENCY        | Currency the annualized salary is paid.                                                                                                                                                                             |
| COMPENSATION_VALUE_USD       | Annualized Salary for Team Member converted to USD based on then conversion rate in Workday as of the compensation change effective date.  If they work less than 40 hrs/week - the prorated salary will show here. |
| COMPENSATION_CURRENCY_USD    | USD                                                                                                                                                                                                                 |
| CONVERSION_RATE_LOCAL_TO_USD | Conversion Rate.  Unable to pull conversion table into compensation tables.  This is calculated by Base Pay - Proposed USD Conversion / Base Pay - Proposed                                                         |

|  Filters                                                    |
|-------------------------------------------------------------------------------|
| Worker Type is Employee  |
| Will show all Active Employees and all Terminated Employees from 1/1/21-Today |
| Will show all Compensation changes from 4/4/22-today                          |

| And/Or | ( | Field                                                 | Operator                  | Comparison Type                | Comparison Value                                   |
|--------|---|-------------------------------------------------------|---------------------------|--------------------------------|----------------------------------------------------|
| And    |   | Business Process Reason Text                          | not equal to              | Value specified in this filter | Conversion                                         |
| And    | ( | Pay Rate Type - Current                               | not in the selection list | Value from another field       | Pay Rate Type - Proposed                           |
| Or     |   | CF - LVD - Pay Schedule Alpha as of Effective Date -1 | not in the selection list | Value from another field       | CF - LVD - Pay Schedule Alpha as of Effective Date |
| Or     |   | Base Pay Current - Amount                             | not equal to              | Value from another field       | Base Pay Proposed - Amount                         |
| Or     |   | Base Pay Current - Currency                           | not in the selection list | Value from another field       | Base Pay Proposed - Currency                       |

{% enddocs %}

{% docs workday_custom_bonus %}

A custom report built in workday for the transition between BambooHR and Workday as the HRIS tool.

| Workday Notes        |                                                                             |
|----------------------|-----------------------------------------------------------------------------|
| BONUS_ID             | Empty - looks like this is a snowflake created values                       |
| EMPLOYEE_ID          | True Employee ID, not BHR ID. Sample Employee IDs above, not real Employees |
| BONUS_TYPE           | Type of Bonus being paid out                                                |
| BONUS DATE           | Date the bonus is being paid out                                            |
| BONUS_NOMINATOR_TYPE | This will not be available in Workday - will show as blank                  |

|    Filters                                                    |
|-------------------------------------------------------------------------------|
| Worker Type is Employee |
| Will show all Active Employees and all Terminated Employees from 1/1/21-Today |
| Will show all Bonuses paid from 4/4/22-today                                  |

{% enddocs %}

{% docs workday_on_target_earnings %}

A custom report built in workday for the transition between BambooHR and Workday as the HRIS tool.

| Workday Notes                |                                                                                                                        |
|------------------------------|------------------------------------------------------------------------------------------------------------------------|
| TARGET_EARNINGS_UPDATE_ID    | Empty - looks like this is a snowflake created values                                                                  |
| EMPLOYEE_ID                  | True Employee ID, not BHR ID. Sample Employee IDs above, not real Employees                                            |
| EFFECTIVE_DATE               | Effective Date of Bonus Plan Change                                                                                    |
| ANNUAL_AMOUNT_LOCAL          | Amount of compensation received annually by bonus plan.  If an employee works less than 40 hours, this prorated        |
| ANNUAL_AMOUNT_LOCAL_Currency | Currency of Local Bonus Amount                                                                                         |
| ANNUAL_AMOUNT_USD_VALUE      | Amount of compensation received annually by bonus plan in USD.  If an employee works less than 40 hours, this prorated |
| OTE_LOCAL                    | Total compensation for team member (Base Pay + Bonus).  Prorated if working under 40 hours                             |
| OTE_LOCAL_CURRENCY           | Local Currency for Total Compensation                                                                                  |
| OTE_USD                      | Total compensation for team member annualized in USD (Base Pay + Bonus).  Prorated if working under 40 hours           |
| OTE_TYPE                     | Bonus Plan Type in Workday                                                                                             |

|       Filters                        |
|------------------------------------------------------|
|  Worker Type is Employee |
| Will show all Compensation changes from 4/4/22-today |

| And/Or | ( | Field                                                            | Operator     | Comparison Type                | Comparison Value                                                    | )  |
|--------|---|------------------------------------------------------------------|--------------|--------------------------------|---------------------------------------------------------------------|----|
| And    |   | Business Process Reason Text                                     | not equal to | Value specified in this filter | Conversion                                                          |    |
| And    | ( | CF - LVD - FTE Adjusted Bonus Target Amount as if Effective Date | not equal to | Value from another field       | CF - LVD - FTE Adjusted Bonus Target Amount as if Effective Date -1 |    |
| Or     | ( | CF - LVD - Bonus Plans as of Effective Date                      | is not empty |                                |                                                                     |    |
| And    |   | CF - LVD - Bonus Plans as of Effective Date                      | is empty     |                                |                                                                     | )) |

{% enddocs %}

{% docs workday_directory %}

A custom report built in workday for the transition between BambooHR and Workday as the HRIS tool.

| Workday Notes |                                                                               |
|---------------|-------------------------------------------------------------------------------|
| employee_id   | True Employee ID, not BHR ID. Sample Employee IDs above, not real Employees   |
| full_name     | Legal Name                                                                    |
| job_title     | Business Title                                                                |
| supervisor    | Manager Legal Name                                                            |
| work_email    | Work Email                                                                    |

| Filters                                                                                   |
|----------------------------------------------------------------------------------------------|
|     Worker Type is Employee                                                       |
|                Will show all Active Employees and all Terminated Employees from 1/1/21-Today |

{% enddocs %}

{% docs workday_emergency_contacts %}

A custom report built in workday for the transition between BambooHR and Workday as the HRIS tool.

| Workday Notes        |                                                                                                             |
|----------------------|-------------------------------------------------------------------------------------------------------------|
| EMPLOYEE_ID          | True Employee ID, not BHR ID. Sample Employee IDs above, not real Employees                                 |
| EMERGENCY_CONTACT_ID | Empty - this looks like snowflake created value                                                             |
| FULL_NAME            | Type of Bonus being paid out                                                                                |
| HOME_PHONE           | Primary Landline number for Primary Emergency Contact.  Shows full value including country code & extension |
| MOBILE_PHONE         | Primary Mobile number for Primary Emergency Contact.  Shows full value including country code & extension   |
| WORK_PHONE           | Primary Work Phone for Primary Emergency Contact.  Shows full value including country code & extension      |

|     Filters                                                   |
|-------------------------------------------------------------------------------|
|  Worker Type is Employee  |
| Will show all Active Employees and all Terminated Employees from 1/1/21-Today |

{% enddocs %}

{% docs workday_employee_mapping %}

A custom report built in workday for the transition between BambooHR and Workday as the HRIS tool.

| Workday Notes                          |                                                                                                                                      |
|----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| Sample Data Above - not real employees |                                                                                                                                      |
| EMPLOYEE_ID                            | True Employee ID, not BHR ID. Sample Employee IDs above, not real Employees                                                          |
| JOB_ID                                 | Will be blank as this looks like to just be a unique ID in the Snowflake Report                                                      |
| FIRST_NAME                             | Legal First Name                                                                                                                     |
| LAST_NAME                              | Legal Last Name                                                                                                                      |
| HIRE_DATE                              | Most Recent Hire Date                                                                                                                |
| TERMINATION_DATE                       | Most Recent Termination Date                                                                                                         |
| GENDER_DROPDOWN                        | Male/Female - not sure the difference between this field & Gender                                                                    |
| EMPLOYEE_STATUS_DATE                   | If Active, Hire Date. If Termed, Term Date                                                                                           |
| EMPLOYMENT_HISTORY_STATUS              | Will only show Active & Terminated for now. (per this issue: https://gitlab.com/gitlab-data/analytics/-/issues/10930#note_875516037) |
| COUNTRY                                | Home Address Country                                                                                                                 |

|     Filters                                                   |
|-------------------------------------------------------------------------------|
|  Worker Type is Employee  |
| Will show all Active Employees and all Terminated Employees from 1/1/21-Today |

{% enddocs %}

{% docs workday_employment_status %}

A custom report built in workday for the transition between BambooHR and Workday as the HRIS tool.

| Workday Notes     |                                                                                                                                                                                                    |
|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| EMPLOYEE_ID       | True Employee ID, not BHR ID. Sample Employee IDs above, not real Employees                                                                                                                        |
| EFFECTIVE_DATE    | Effective date of the Business Process Associate with the change.  Currently only hire & termination business processes are pulled in.  Once absence is implemented, could consider adding leaves |
| STATUS_ID         | Will be blank as this looks like to just be a unique ID in the Snowflake Report                                                                                                                    |
| EMPLOYMENT_STATUS | Will only show Active & Terminated for now.   (per this issue: https://gitlab.com/gitlab-data/analytics/-/issues/10930#note_875516037)                                                             |

|     Filters                                                   |
|-------------------------------------------------------------------------------|
|  Worker Type is Employee  |
| Will show all Active Employees and all Terminated Employees from 1/1/21-Today |

{% enddocs %}

{% docs workday_job_info %}

A custom report built in workday for the transition between BambooHR and Workday as the HRIS tool.

| Workday Notes  |                                                                                                                                                                    |
|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| JOB_ID         | Will be blank as this looks like to just be a unique ID in the Snowflake Report                                                                                    |
| EMPLOYEE_ID    | True Employee ID, not BHR ID. Sample Employee IDs above, not real Employees                                                                                        |
| JOB_TITLE      | Business Title                                                                                                                                                     |
| EFFECTIVE_DATE | Effective date of the transaction entered.  Currently only pulls transactions in which this data will change (transactions listed in view of Workday filter below) |
| DEPARTMENT     | Workday Cost Center as of Effective Date                                                                                                                           |
| DIVISION       | Workday Cost Center Hierarchy as of Effective Date                                                                                                                 |
| ENTITY         | Workday Company as of Effective Date                                                                                                                               |
| REPORTS_TO     | Manager Legal Name as of Effective Date                                                                                                                            |

|      Filters                                                                                               |
|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|Will show all Active Employees and all Terminated Employees from 1/1/21-Today |
| Will Only show Job changes from 4/4/22 - Today.  Hire record will show worker's job data as of 4/4/22 (as historical data before 4/4/22 has not been loaded in this data source) |

| And/Or | ( | Field                 | Operator              | Comparison Type                | Comparison Value |
|--------|---|-----------------------|-----------------------|--------------------------------|------------------|
|        |   | Worker Type           | in the selection list | Value specified in this filter | Employee         |
| And    |   | Effective Date        | less than or equal to | Value from another field       | Today (System)   |
| And    |   | Worker Event - Parent | is empty              |                                |                  |
|And||Business Process Type|in the selection list|Value specified in this filter|Change Job, Change Organization Assignments for Worker, Change Organization Assignments for Workers by Organization, Demote Employee Inbound, Hire, Move Worker (By Organization), Move Worker (Supervisory), Promote Employee Inbound, Title Change, Transfer Employee Inbound|
| And | ( | CF - LVD - Business Title as of Effective Date -1   | not equal to              | Value from another field | CF - LVD - Business Title as of Effective Date    |
| Or  |   | CF - LVD - Cost Center as of Effective Date - 1 Day | not in the selection list | Value from another field | CF - LVD - Cost Center as of Effective Date       |
| Or  |   | CF - LVD - Company as of Effective Date - 1 Day     | not in the selection list | Value from another field | CF - LVD - Company as of Effective Date           |
| Or  |   | CF - LVD - Division as of Effective Date -1         | not in the selection list | Value from another field | CF - LVD - Division as of Effective Date          |
| Or  |   | CF - LVD - Manager 01 as of Effective Date          | not in the selection list | Value from another field | CF - LVD - Manager 01 as of Effective Date -1 Day |

{% enddocs %}

{% docs supervisory_organization %}

A custom report built in Workday for the creation of the Supervisory Organizations model. Supervisory organizations are the primary foundation of Workday, as they show who reports to whom

| Workday Notes         |                                                                                                                                                                    |
|-----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|TEAM_ID                | The unique organization identifier                                                                                                                                 |
|TEAM_HIERARCHY_LEVEL   | Team level within the hierarchy. Organizations can be organized into hierarchies and enable reporting at different levels in the hierarchy                         |
|TEAM_MEMBERS_COUNT     | Number of team members in the organization                                                                                                                         |
|TEAM_MANAGER_INHERITED |                                                                                                                                                                    |
|TEAM_INACTIVATED       | Boolean. Tracks inactive teams.                                                                                                                                    |
|TEAM_MANAGER_NAME      | Organization's manager name                                                                                                                                        |
|TEAM_NAME              | Organization's name                                                                                                                                                |
|TEAM_MANAGER_NAME_ID   | Organization's manager employee ID                                                                                                                                 |
|TEAM_SUPERIOR_TEAM_ID  | Organization's superior team ID                                                                                                                                    |
|TEAM_INACTIVATED_DATE  | Date the organization became inactive                                                                                                                              |

{% enddocs %}


{% docs performance_growth_potential %}

A custom report built in Workday for the creation of the dim_team_member model. The data is extracted using Fivetran and loaded into Snowflake, with a sync frequency of 12 hours. The grain of the table is one row per Workday ID, which is the unique ID for the business process. GitLab started using Workday for Talent Assessment in FY23, and all team members who were part of the talent assessment process in FY23 should have both performance and growth potential rating. Historical data from BambooHR might have null values. Workday only captures history from mid-April 2022 onwards. 

[This sheet](https://docs.google.com/spreadsheets/d/1-RvIjlca6b4T0XmKRdZToP2x5gLRQJ1RDj7H1vWYknc/edit) contains the links to the reports in Workday production (access restricted, Laura Janssen is the owner)

{% enddocs %}

{% docs assess_talent %}

A custom report built in Workday for the creation of the dim_team_member model. The data is extracted using Fivetran and loaded into Snowflake, with a sync frequency of 12 hours. The grain of the table is one row per Workday ID, which is the unique ID for the business process. This table contains information regarding the status of [key talent](https://about.gitlab.com/handbook/people-group/talent-assessment/#key-talent) at GitLab, including the effective date and the employee id. 

[This sheet](https://docs.google.com/spreadsheets/d/1-RvIjlca6b4T0XmKRdZToP2x5gLRQJ1RDj7H1vWYknc/edit) contains the links to the reports in Workday production (access restricted, Laura Janssen is the owner)

{% enddocs %}

{% docs all_workers %}

A custom report built in Workday for the creation of the dim_team_member model. The data is extracted using Fivetran and loaded into Snowflake, with a sync frequency of 12 hours. The grain of the table is one row per team member. This table contains sensitive data, including the most recent inputs for ethnicity, date of birth and gender per team member. The history of sensitive fields (such as gender and ethnicity) shouldn't be captured and only the most recent fields should be stored in the data warehouse. This table contains records for all active employees & any employee that terminated in 2021. Anyone who terminated before 2021 isn't captured in this table.

[This sheet](https://docs.google.com/spreadsheets/d/1-RvIjlca6b4T0XmKRdZToP2x5gLRQJ1RDj7H1vWYknc/edit) contains the links to the reports in Workday production (access restricted, Laura Janssen is the owner)

{% enddocs %}

{% docs gitlab_usernames %}

A custom report built in Workday for the creation of the dim_team_member model. The data is extracted using Fivetran and loaded into Snowflake, with a sync frequency of 12 hours. The grain of the table is one row per Workday ID, which is the unique ID for the business process. This table contains the gitlab_username, along with the effective_date and employee_id. 

[This sheet](https://docs.google.com/spreadsheets/d/1-RvIjlca6b4T0XmKRdZToP2x5gLRQJ1RDj7H1vWYknc/edit) contains the links to the reports in Workday production (access restricted, Laura Janssen is the owner)

{% enddocs %}

{% docs staffing_history_approved %}

A custom report built in Workday for the creation of the dim_team_member model. The data is extracted using Fivetran and loaded into Snowflake, with a sync frequency of 12 hours. The grain of the table is one row per Workday ID, which is the unique ID for the business process. This table contains data regarding the location of the team member (Country and region) as well their start and termination dates and the business process associated with the change in their worker profile. This table (and Workday) only captures history from mid-April 2022 onwards. 

[This sheet](https://docs.google.com/spreadsheets/d/1-RvIjlca6b4T0XmKRdZToP2x5gLRQJ1RDj7H1vWYknc/edit) contains the links to the reports in Workday production (access restricted, Laura Janssen is the owner).

{% enddocs %}

{% docs job_profiles %}

A custom report built in Workday. The data is extracted using Fivetran and loaded into Snowflake, with a sync frequency of 12 hours. The grain of the table is one row per job_code, which is the unique ID for the job profile. This table contains data regarding the job profile, job family, management level and job level.  This table doesn't capture history.

[This sheet](https://docs.google.com/spreadsheets/d/1-RvIjlca6b4T0XmKRdZToP2x5gLRQJ1RDj7H1vWYknc/edit) contains the links to the reports in Workday production (access restricted, Laura Janssen is the owner).

{% enddocs %}

{% docs cost_centers %}

A custom report built in Workday. The data is extracted using Fivetran and loaded into Snowflake, with a sync frequency of 12 hours. The grain of the table is one row per dept_workday_id. This table contains data regarding the department, as well as the cost center and division it belongs to.  This table doesn't capture history.

[This sheet](https://docs.google.com/spreadsheets/d/1-RvIjlca6b4T0XmKRdZToP2x5gLRQJ1RDj7H1vWYknc/edit) contains the links to the reports in Workday production (access restricted, Laura Janssen is the owner).

{% enddocs %}



## Columns

{% docs workday_employee_id %}
The unique identifier the the team member in the HRIS system.
{% enddocs %}

{% docs workday_bonus_date %}
The date the team member bonus was entered in to the system
{% enddocs %}
 
{% docs workday_bonus_type %}
The type of team member bonus that was awarded
{% enddocs %}
 
{% docs workday_uploaded_at %}
The date that the report was uploaded from Workday
{% enddocs %}
 
{% docs workday_effective_date %}
The date that the record or event was entered into Workday
{% enddocs %}
 
{% docs workday_compensation_type %}
Denotes if a team members is compensated `Hourly` or is `Salary`
{% enddocs %}
 
{% docs workday_compensation_change_reason %}
The event or action that lead to the change in compensation
{% enddocs %}
 
{% docs workday_pay_rate %}
The frequency a team member is paid
{% enddocs %}
 
{% docs workday_compensation_value %}
The value in local currency that the team member is paid
{% enddocs %}
 
{% docs workday_compensation_currency %}
The local currency the team member is paid in.
{% enddocs %}
 
{% docs workday_conversion_rate_local_to_usd %}
The conversion rate between the local currency and USD at the time of the compensation change.
{% enddocs %}
 
{% docs workday_compensation_currency_usd %}
The currency that is related to the `conversion_rate_local_to_usd` field
{% enddocs %}
 
{% docs workday_compensation_value_usd %}
The value, in USD, that the team member is paid.
{% enddocs %}
 
{% docs workday_initiated_at %}
The timestamp the action was initiated, to be used as a way to order the events
{% enddocs %}
 
{% docs workday_work_email %}
The work email of the team member.
{% enddocs %}
 
{% docs workday_full_name %}
The full name of the team member
{% enddocs %}
 
{% docs workday_supervisor %}
The person the team member reports to.
{% enddocs %}
 
{% docs workday_home_phone %}
The home phone number for the team member contact
{% enddocs %}
 
{% docs workday_mobile_phone %}
The mobile phone number for the team member contact
{% enddocs %}
 
{% docs workday_work_phone %}
The work phone number for the team member contact
{% enddocs %}
 
{% docs workday_employment_history_status %}
Shows the the Active or Terminated status for the team member
{% enddocs %}
 
{% docs workday_employee_status_date %}
Show the hire date or terminated date of the team member
{% enddocs %}
 
{% docs workday_cost_center %}
The reported top level cost center for the team member
{% enddocs %}
 
{% docs workday_last_name %}
The last name of the team member
{% enddocs %}
 
{% docs workday_first_name %}
The first name of the team member
{% enddocs %}
 
{% docs workday_region %}
The top level geographical region of the team member
{% enddocs %}
 
{% docs workday_hire_date %}
The date the team member started working at GitLab
{% enddocs %}
 
{% docs workday_country %}
The country of the team member
{% enddocs %}
 
{% docs workday_greenhouse_candidate_id %}
The latest id from the Greenhouse application for a team member
{% enddocs %}
 
{% docs workday_gender %}
The reported gender of the team member. Only includes `Male` and `Female` options.
{% enddocs %}
 
{% docs workday_job_role %}
Indicates the team member is an Individual Contributor, Manager, Director, or Leader
{% enddocs %}
 
{% docs workday_gender_dropdown %}
Other reported gender identifications of the team member.
{% enddocs %}
 
{% docs workday_date_of_birth %}
The team members birthday
{% enddocs %}
 
{% docs workday_job_grade %}
The grade of the job of the team member
{% enddocs %}
 
{% docs workday_pay_frequency %}
How frequently the team member is paid.
{% enddocs %}
 
{% docs workday_age %}
The age of the team member
{% enddocs %}
 
{% docs workday_jobtitle_speciality_single_select %}
The specialty of the team member
{% enddocs %}
 
{% docs workday_ethnicity %}
The reported ethnicity of the team member.
{% enddocs %}
 
{% docs workday_jobtitle_speciality_multi_select %}
A list of specialties of a team member
{% enddocs %}
 
{% docs workday_gitlab_username %}
The GitLab application username for the team member.
{% enddocs %}
 
{% docs workday_sales_geo_differential %}
To be added
{% enddocs %}
 
{% docs workday_locality %}
The generalized location of the team member, the lowest level of the team member geographical hierarchy
{% enddocs %}
 
{% docs workday_termination_date %}
The date of the team members last dat working for GitLab
{% enddocs %}
 
{% docs workday_nationality %}
The reported nationaly of the team member
{% enddocs %}
 
{% docs workday_employment_status_column %}
The reported status of the team member. Currently only includes `Active`, `On Leave`, and `Terminated`
{% enddocs %}
 
{% docs workday_termination_type %}
Additional details about the termination of the team member
{% enddocs %}
 
{% docs workday_business_process_event %}
To be added
{% enddocs %}
 
{% docs workday_department %}
The second level of the GitLab organization hierarchy
{% enddocs %}
 
{% docs workday_division %}
The top level of the GitLab organization hierarchy
{% enddocs %}
 
{% docs workday_entity %}
The legal entity that employs the team member.
{% enddocs %}
 
{% docs workday_job_title %}
The title of the position the team member holds.
{% enddocs %}
 
{% docs workday_reports_to %}
The person the the team member reports to in the organization hierarchy.
{% enddocs %}
 
{% docs workday_annual_amount_local %}
The annualized amount the team member is paid in their local currency
{% enddocs %}
 
{% docs workday_annual_amount_local_currency_code %}
The currency the team member is paid in.
{% enddocs %}
 
{% docs workday_annual_amount_usd_value %}
The annualized amount the team member is paid in USD.
{% enddocs %}
 
{% docs workday_ote_local %}
The on time earnings of the team member in their local currency.
{% enddocs %}
 
{% docs workday_ote_local_currency_code %}
The local currency of the team member.
{% enddocs %}
 
{% docs workday_ote_type %}
The type of on time earings for the team member.
{% enddocs %}
 
{% docs workday_ote_usd %}
The on time earnings of the team member in USD.
{% enddocs %}

{% docs workday_per_pay_period_amount %}
The amount compensated for each pay period.
{% enddocs %}

{% docs workday_workday_id %}
A unique identifier for the event from the workday application.
{% enddocs %}
 