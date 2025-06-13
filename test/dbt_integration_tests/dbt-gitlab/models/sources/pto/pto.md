## Source

{% docs gitlab_pto %}
Data from the PTO by Roots system
{% enddocs %}

## Tables
{% docs gitlab_pto_source %}
PTO information from PTO by Roots on the grain of a single missed day
source documentation https://www.tryroots.io/api-docs/pto#tag/OOO-Event-Model
{% enddocs %}

## Columns

### end_date
{% docs gitlab_pto_end_date %}
The end date (timezone naive, ISO-formatted) of the event
{% enddocs %}
### start_date
{% docs gitlab_pto_start_date %}
The start date (timezone naive, ISO-formatted) of the event
{% enddocs %}
### pto_status
{% docs gitlab_pto_pto_status %}
Enum: "AP" "RQ" "DN" "CN"
2-character enumeration denoting the status of the OOO Event.

"AP" = Approved

"RQ" = Requested

"DN" = Denied

"CN" = Cancelled

{% enddocs %}
### employee_day_length
{% docs gitlab_pto_employee_day_length %}
How long this User's "day" is in hours
{% enddocs %}
### hr_employee_id
{% docs gitlab_pto_hr_employee_id %}
If using an external HRIS, this is the User's ID in that HRIS
{% enddocs %}
### employee_uuid
{% docs gitlab_pto_employee_uuid %}
Unique identifier for the employee
{% enddocs %}
### pto_uuid
{% docs gitlab_pto_pto_uuid %}
Unique identifier for this pto event
{% enddocs %}
### pto_date
{% docs gitlab_pto_pto_date %}
The date (timezone naive, ISO-formatted) of this OOO Day
{% enddocs %}
### pto_ends_at
{% docs gitlab_pto_pto_ends_at %}
If this user has designated a start time, meaning they aren't taking the whole OOO day off, this is the timezone-aware ISO-formatted datetime when the OOO ends on this day.
{% enddocs %}
### is_holiday
{% docs gitlab_pto_is_holiday %}
If this OOO Day overlaps a holiday.
{% enddocs %}
### recorded_hours
{% docs gitlab_pto_recorded_hours %}
How many hours were recorded as OOO for this event.
{% enddocs %}
### pto_starts_at
{% docs gitlab_pto_pto_starts_at %}
If this user has designated a start time, meaning they aren't taking the whole OOO day off, this is the timezone-aware ISO-formatted datetime when the OOO starts on this day.
{% enddocs %}
### total_hours
{% docs gitlab_pto_total_hours %}
How long the user was OOO for. This is different from `recorded_hours` in that it is unaware of holidays and weekends.
{% enddocs %}
### pto_group_type
{% docs gitlab_pto_pto_group_type %}
Enum: "STL" "EXL" "OTH"
3-character enumeration denoting the OOO Group Type of this OOO Type.
"STL" = Short-term Leave
"EXL" = Extended Leave
"OTH" = Other
{% enddocs %}
### is_pto
{% docs gitlab_pto_is_pto %}
If an OOO Event of this type is considered PTO for reporting purposes.
{% enddocs %}
### pto_type_name
{% docs gitlab_pto_pto_type_name %}
The name of the OOO Type.
{% enddocs %}
### pto_type_uuid
{% docs gitlab_pto_pto_type_uuid %}
The UUID of this OOO Type.
{% enddocs %}
### pto_created_at
{% docs gitlab_pto_pto_created_at %}
The timestamp (UTC datetime, ISO-formatted) when this OOO Event was created.
{% enddocs %}

