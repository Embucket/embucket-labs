{% docs greenhouse_applications_source %}
This table is A snapshot of relevant application data
Each row represents Each row represents the latest status of a single application
Things to note:
"This table contains the current/latest data per application, so ""status"" and ""stage_name"" will represent the current status and current stage, and ""prospect"" will represent whether or not the application is a prospect application. If a prospect application is converted to an actual application, the status on the prospect application will change to ""converted"". This table does NOT contain any historical records.

In advance of a new prospecting feature that will be released, we've proactively added ""prospect_pool"", ""prospect_pool_stage"" and ""prospect_owner_id"" as dimensions into this table

This table contains several ID numbers that can be joined to other tables to get the names (e.g. ""source_id"" and ""referrer_id"")."
{% enddocs %}


{% docs greenhouse_applications_jobs_source %}
This table is Mapping of applications to jobs
Each row represents a mapping of one application to one job
Things to note:
A candidate can have multiple applications, but a single application is only on one job.
{% enddocs %}

{% docs greenhouse_interviewers_source %}
This table is A list of interviewers per scheduled interview
Each row represents an interviewer for a scheduled interview
Things to note:
This table is joined to the scheduled_interviews table on scheduled_interviews.id = interviewers.interview_id
{% enddocs %}

{% docs greenhouse_offers_source %}
This table is All offer details
Each row represents the details of a single version an offer for an application
Things to note:
Each version of an offer generates a new row of data. The older versions are all marked with "status" = deprecated.
{% enddocs %}

{% docs greenhouse_referrers_source %}
This table is A list of all referrers
Each row represents A single referrer
Things to note:
This table can be joined to any other table that has a referrer ID to get the name of a referrer
{% enddocs %}

{% docs greenhouse_sources_source %}
This table is A list of all sources
Each row represents A single source
Things to note:
This table can be joined to any other table that has a source ID to get the name of a source
{% enddocs %}

{% docs greenhouse_application_custom_fields_source %}
This table is Custom fields associated with an application
Each row represents a single value of a single custom field for a single application
Things to note:
"custom_field"" represents the name of the custom field, and ""display_value"" represents the value of the custom field.
{% enddocs %}

{% docs greenhouse_application_question_answers_source %}
This table is The questions and answers that the candidate answered on the job post when submitting an application
Each row represents a single question and answer
{% enddocs %}

{% docs greenhouse_application_stages_source %}
This table is Historical activity of all stages an application can be in
Each row represents a stage that an application can be in, and the timestamp that the application entered and exited the stage
Things to note:
This table contains a row for each stage that an application can be in (taken from the job that the application is on). Thus, there may be rows for stages that an application has yet to reach, or will not reach (if the application was rejected).
{% enddocs %}

{% docs greenhouse_approvals_source %}
This table is
Each row represents
Things to note:
{% enddocs %}

{% docs greenhouse_attributes_source %}
This table is A list of all attributes ever used on a scorecard
Each row represents A single attribute
Things to note:
This table can be joined to any other table that has an attribute ID to get the name of the attribute
{% enddocs %}

{% docs greenhouse_candidate_custom_fields_source %}
This table is Custom fields associated with a candidate
Each row represents a single value of a single custom field for a single candidate
Things to note:
"custom_field" represents the name of the custom field, and "display_value" represents the value of the custom field.
{% enddocs %}

{% docs greenhouse_candidate_survey_questions_source %}
This table is The list of candidate survey questions
Each row represents The text of the question asked on the candidate survey
Things to note:
The ID numbers here correspond to question #1, question #2 etc.
{% enddocs %}

{% docs greenhouse_candidate_surveys_source %}
This table is Candidate survey responses
Each row represents a single response to the candidate survey
Things to note:
For privacy, candidate survey responses are not associated to the candidate or to the job.
{% enddocs %}

{% docs greenhouse_candidates_source %}
This table is A snapshot of relevant candidate data
Each row represents a single candidate
Things to note:
"A candidate is a person. A candidate may have multiple applications on multiple jobs.

The ""recruiter"" and ""coordinator"" that is assigned to a candidate may or may not be the same recruiter and coordinator that is assigned to the hiring team of a job that the candidate has an application on."
{% enddocs %}

{% docs greenhouse_candidates_tags_source %}
This table is All tags associated with a candidate
Each row represents a single tag on a single candidate
Things to note:
This table only has "tag_id" and needs to be joined to the tags table to get the tag name.
{% enddocs %}

{% docs greenhouse_departments_source %}
This table is A list of all departments
Each row represents A single department
Things to note:
This table can be joined to any other table that has a department ID to get the name of a department
{% enddocs %}

{% docs greenhouse_educations_source %}
This table is All educations associated with a candidate
Each row represents a single structured education for a single candidate
Things to note:
This data is only generated when education is added in a structured manner in the candidate info section (candidate details tab in Greenhouse).
{% enddocs %}

{% docs greenhouse_eeoc_responses_source %}
This table is Responses to EEOC questions
Each row represents a single application's responses to the EEOC questions
Things to note:
Data is only generated in this table when an application responds to the EEOC questions, or a request is sent to an application to respond to the questions. Data is not generated for applications that did not respond to questions on the job post, and did not receive a request to resopnd to the questions afterwards.
{% enddocs %}

{% docs greenhouse_employments_source %}
This table is All employments associated with a candidate
Each row represents a single structured employment record for a single candidate
Things to note:
This data is only generated when employment is added in a structured manner in the candidate info section (candidate details tab in Greenhouse).
{% enddocs %}

{% docs greenhouse_hiring_team_source %}
This table is The hiring team for a job
Each row represents a single Greenhouse user in a hiring team role on a single job
Things to note:
Each row represents a single user on a single role, so if a hiring team has 2 or more members on the same role, those members will be represented in multiple rows.

This table only contains ""user_id"" and not user name, which can be identified by joining onto the users table.
{% enddocs %}

{% docs greenhouse_interviewer_tags_source %}
This table is A list of all interviewer tags associated with users
Each row represents A single interviewer tag for a single user
{% enddocs %}

{% docs greenhouse_interviews_source %}
This table is A list of all interviews
Each row represents A single interview
Things to note:
This table can be joined to any other table that has an interview ID to get the name of an interview
{% enddocs %}

{% docs greenhouse_job_custom_fields_source %}
This table is Custom fields associated with a job
Each row represents a single value of a single custom field for a single job
Things to note:
"custom_field" represents the name of the custom field, and "display_value" represents the value of the custom field.
{% enddocs %}

{% docs greenhouse_job_posts_source %}
This table is A list of job posts by job
Each row represents the details of a job post that are associated with a job
Things to note:
{% enddocs %}

{% docs greenhouse_job_snapshots_source %}
This table is A daily snapshot of job-level activity
Each row represents various job-level metrics for a single day
{% enddocs %}

{% docs greenhouse_jobs_source %}
This table is A snapshot of relevant job data
Each row represents the latest status of a single job
Things to note:
Each job only has one set of "opened_at" and "closed_at" values. If a job is opened, closed, and re-opened again, the first pair of open/close dates are lost. This table does NOT contain any historical records. A history of job open/close dates can be found in the Change Log in Greenhouse.
{% enddocs %}

{% docs greenhouse_jobs_attributes_source %}
This table is A list of attributes on scorecards by job
Each row represents a single tag on a single job
{% enddocs %}

{% docs greenhouse_jobs_departments_source %}
This table is A list of departments by job
Each row represents a single department on a single job
{% enddocs %}

{% docs greenhouse_jobs_interviews_source %}
This table is A list of interviews by job
Each row represents a single interview on a single stage for a single job
{% enddocs %}

{% docs greenhouse_jobs_offices_source %}
This table is A list of offices by job
Each row represents a single office on a single job
{% enddocs %}

{% docs greenhouse_jobs_stages_source %}
This table is A list of stages by job
Each row represents a single stage for a single job
{% enddocs %}

{% docs greenhouse_offer_custom_fields_source %}
This table is Custom fields associated with an offer
Each row represents a single value of a single custom field for a single offer
Things to note:
"custom_field" represents the name of the custom field, and "display_value" represents the value of the custom field.
{% enddocs %}

{% docs greenhouse_offices_source %}
This table is A list of all offices
Each row represents A single office
Things to note:
This table can be joined to any other table that has an office ID to get the name of an office
{% enddocs %}

{% docs greenhouse_openings_source %}
This table is The openings for a job
Each row represents a single opening for a single job
{% enddocs %}

{% docs greenhouse_organizations_source %}
This table is The name of your organization
Each row represents The name of your organization
{% enddocs %}

{% docs greenhouse_referral_question_custom_fields_source %}
This table is Custom referral questions associated with an application
Each row represents a single value of a single custom referral question for a single application
Things to note:
"custom_field" represents the name of the custom rejection question, and "display_value" represents the value of the custom question.
{% enddocs %}

{% docs greenhouse_rejection_question_custom_fields_source %}
This table is Custom fields associated with an application's rejection
Each row represents a single value of a single custom rejection question for a single application
Things to note:
"""custom_field"" represents the name of the custom rejection question, and ""display_value"" represents the value of the custom question.
{% enddocs %}

{% docs greenhouse_rejection_reasons_source %}
This table is A list of all rejection reasons
Each row represents A single rejection reason
Things to note:
This table can be joined to any other table that has a rejection reason ID to get the name of a rejection reason
{% enddocs %}

{% docs greenhouse_scheduled_interviews_source %}
This table is Data on scheduled interviews
Each row represents a scheduled interview
Things to note:
Data is only generated when an interview is scheduled with Greenhouse (as evidenced the presence of a Greenhouse calendar on the scheduled interview's calendar invite). If an interview occurs or a scorecard is submitted when the interview was not scheduled with Greenhouse, it will NOT appear in this table. If interviews are rescheduled, this table does not capture the timestamp at which it was rescheduled; the "scheduled_at" table only represents the very first the interivew was scheduled.
{% enddocs %}

{% docs greenhouse_scorecard_question_answers_source %}
This table is The questions and answers that exist on a scorecard
Each row represents a single question and answer
Things to note:
{% enddocs %}

{% docs greenhouse_scorecards_source %}
This table is Data on submitted scorecards
Each row represents a submitted scorecard
Things to note:
Data is only generated when a scorecard is submitted in Greenhouse. If a scorecard is not submitted, it will NOT appear in this table.If an interview has been scheduled, you can connect the scheduled_interviews table to the interviewers table, and then connect the scorecards table to the interviewers table.
{% enddocs %}

{% docs greenhouse_scorecards_attributes_source %}
This table is Attribute-level data on a submitted scorecard
Each row represents an attribute rating on a submitted scorecard
Things to note:
This table can be joined to the scorecards data to show attribute-level ratings, in addition to the overall recommendation.
{% enddocs %}

{% docs greenhouse_stage_snapshots_source %}
This table is A daily snapshot of stage-level activity
Each row represents the number of active applications in a stage at 4am EST on a given date, only for jobs that were open at that time
{% enddocs %}

{% docs greenhouse_stages_source %}
This table is A list of all stages
Each row represents A single stage
Things to note:
This table can be joined to any other table that has a stage ID to get the name of a stage
{% enddocs %}

{% docs greenhouse_tags_source %}
This table is A list of all tags
Each row represents A single tag
Things to note:
This table can be joined to any other table that has a tag ID to get the name of a tag
{% enddocs %}

{% docs greenhouse_user_actions_source %}
This table is A record of user actions in Greenhouse
Each record represents a single action from a user on a single job
Things to note:
Not all actions are recorded in this table. In general, it is better to look for a specific timestamped activity in a different table than to use this table.
{% enddocs %}

{% docs greenhouse_users_source %}
This table is A list of all users
Each row represents A single user
Things to note:
This table can be joined to any other table that has a user ID to get the name of a user
{% enddocs %}

{% docs greenhouse_survey_builder_questions_source %}
This table holds the list of candidate survey questions.
Each row represents A single question of the survey.
{% enddocs %}

{% docs greenhouse_survey_builder_answers_source %}
This table is Candidate survey answers table.
Each row represents a single response to the candidate survey
Things to note:
For privacy, candidate survey responses are not associated to the candidate or to the job.
{% enddocs %}

{% docs greenhouse_survey_builder_surveys_source %}
The table lists all surveys campaigns.
{% enddocs %}

{% docs greenhouse_delivered_candidate_surveys_source %}
This table lists the department and office details of every submission of a candidate survey. 
{% enddocs %}