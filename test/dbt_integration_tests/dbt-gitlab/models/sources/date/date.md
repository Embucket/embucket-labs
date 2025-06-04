## Columns

{% docs date_date_day %}
Calendar date, e.g. '2019-02-02'
{% enddocs %}

{% docs date_date_actual %}
Matches `date_day`, duplicated for ease of use
{% enddocs %}

{% docs date_day_name %}
Abbreviated name of the day of the week, e.g. 'Sat' for 2019-02-02
{% enddocs %}

{% docs date_month_actual %}
Number for the calendar month of the year, e.g. '2' for 2019-02-02
{% enddocs %}

{% docs date_year_actual %}
Calendar year, e.g. '2019' for 2019-02-02
{% enddocs %}

{% docs date_quarter_actual %}
Calendar quarter, e.g. '1' for 2019-02-02
{% enddocs %}

{% docs date_day_of_week %}
Number of the day of the week, with Monday = 1 and Sunday = 7
{% enddocs %}

{% docs date_first_day_of_week %}
Calendar date of the first Monday of that week, e.g. '2019-01-28' for 2019-02-02
{% enddocs %}

{% docs date_week_of_year %}
Calendar week of year, e.g. '5' for 2019-02-02

special_considerations:
  - Week numbers range from 1 to 53.
  - The first week of the year is the week containing January 1st.
  - Week numbering is consistent with Snowflake's WEEK function, using WEEK_OF_YEAR_POLICY = 0.
  - This approach ensures consistent week numbering across year boundaries.
  - The last few days of December might be part of week 1 of the next year if January 1st falls early in the week.
  - Similarly, the first few days of January might be part of the last week of the previous year if January 1st falls late in the week.

{% enddocs %}

{% docs date_day_of_month %}
Day Number of the month, e.g. '2' for 2019-02-02
{% enddocs %}

{% docs date_day_of_quarter %}
Day Number from the start of the calendar quarter, e.g. '33' for 2019-02-02
{% enddocs %}

{% docs date_day_of_year %}
Day Number from the start of the calendar year, e.g. '33' for 2019-02-02
{% enddocs %}

{% docs date_fiscal_year %}
Fiscal year for the date, e.g. '2020' for 2019-02-02
{% enddocs %}

{% docs date_fiscal_quarter %}
Fiscal quarter for the date, e.g. '1' for 2019-02-02
{% enddocs %}

{% docs date_day_of_fiscal_quarter %}
Day Number from the start of the fiscal quarter, e.g. '2' for 2019-02-02
{% enddocs %}

{% docs date_day_of_fiscal_year %}
Day Number from the start of the fiscal year, e.g. '2' for 2019-02-02
{% enddocs %}

{% docs date_month_name %}
The full month name for any calendar month, e.g. 'February' for 2019-02-02
{% enddocs %}

{% docs date_first_day_of_month %}
The first day of a calendar month, e.g. '2019-02-01' for 2019-02-02
{% enddocs %}

{% docs date_last_day_of_month %}
The last day of a calendar month, e.g. '2019-02-28' for 2019-02-02
{% enddocs %}

{% docs date_first_day_of_year %}
The first day of a calendar year, e.g. '2019-01-01' for 2019-02-02
{% enddocs %}

{% docs date_last_day_of_year %}
The last day of a calendar year, e.g. '2019-12-31' for 2019-02-02
{% enddocs %}

{% docs date_first_day_of_quarter %}
The first day of a calendar quarter, e.g. '2019-01-01' for 2019-02-02
{% enddocs %}

{% docs date_last_day_of_quarter %}
The last day of a calendar quarter, e.g. '2019-03-31' for 2019-02-02
{% enddocs %}

{% docs date_first_day_of_fiscal_quarter %}
The first day of the fiscal quarter, e.g. '2019-02-01' for 2019-02-02
{% enddocs %}

{% docs date_last_day_of_fiscal_quarter %}
The last day of the fiscal quarter, e.g. '2019-04-30' for 2019-02-02
{% enddocs %}

{% docs date_first_day_of_fiscal_year %}
The first day of the fiscal year, e.g. '2019-02-01' for 2019-02-02
{% enddocs %}

{% docs date_last_day_of_fiscal_year %}
The last day of the fiscal year, e.g. '2020-01-31' for 2019-02-02
{% enddocs %}

{% docs date_week_of_fiscal_year %}
The week number for the fiscal year, e.g. '1' for 2019-02-02
{% enddocs %}

{% docs date_month_of_fiscal_year %}
The month number for the fiscal year, e.g. '1' for 2019-02-02
{% enddocs %}

{% docs date_last_day_of_week %}
The Saturday of the week, e.g. '2019-02-02' for 2019-02-02
{% enddocs %}

{% docs date_quarter_name %}
The name of the calendar quarter, e.g. '2019-Q1' for 2019-02-02
{% enddocs %}

{% docs date_fiscal_quarter_name %}
The name of the fiscal quarter, e.g '2020-Q1' for 2019-02-02
{% enddocs %}

{% docs date_fiscal_quarter_name_fy %}
The name of the fiscal quarter, e.g 'FY20-Q1' for 2019-02-02
{% enddocs %}

{% docs date_fiscal_quarter_number_absolute %}
Monotonically increasing integer for each fiscal quarter. This allows for comparing the relative order of fiscal quarters.
{% enddocs %}

{% docs date_fiscal_month_name %}
The name of the fiscal month, e.g '2020-Feb' for 2019-02-02
{% enddocs %}

{% docs date_fiscal_month_name_fy %}
The name of the fiscal month, e.g 'FY20-Feb' for 2019-02-02
{% enddocs %}

{% docs date_holiday_desc %}
The name of the holiday, if applicable
{% enddocs %}

{% docs date_is_holiday %}
Whether or not it is a holiday
{% enddocs %}

{% docs date_last_month_of_fiscal_quarter %}
Date indicating last month of fiscal quarter e.g '2020-04-01' for 2020-02-02
{% enddocs %}

{% docs date_is_first_day_of_last_month_of_fiscal_quarter %}
Flag indicating date that is the first day of last month of fiscal quarter. E.g TRUE for '2020-04-01'
{% enddocs %}
  
{% docs date_last_month_of_fiscal_year %}
Date indicating last month of fiscal year e.g '2021-01-01' for 2020-02-02
{% enddocs %}

{% docs date_is_first_day_of_last_month_of_fiscal_year %}
Flag indicating date that is the first day of last month of fiscal year. E.g TRUE for '2021-01-01'
{% enddocs %}

{% docs date_snapshot_date_fpa %}
8th calendar day of a month used for FP&A snapshots
{% enddocs %}

{% docs date_snapshot_date_fpa_fifth %}
5th calendar day of a month used for FP&A snapshots
{% enddocs %}

{% docs date_snapshot_date_billings %}
45 calendar day after a month begins used for Billings snapshots
{% enddocs %}

{% docs date_days_in_month_count %}
Number of calendar days in the given month.
{% enddocs %}

{% docs current_day_of_month %}
The day of the month for the current date. Since it references the current date it is the same number for every record in the data model.

Example: For current_date = '2023-08-15', `current_day_of_month` -> 15
{% enddocs %}

{% docs current_day_of_fiscal_quarter %}
The day of the fiscal quarter for the current date. Since it references the current date it is the same number for every record in the data model.

Example: For current_date = '2023-08-15', `current_day_of_fiscal_quarter` -> 15. Because the fiscal quarter associated with this current_date starts on August.
{% enddocs %}

{% docs current_day_of_fiscal_year %}
The day of the fiscal year for the current date. Since it references the current date it is the same number for every record in the data model.

Example: For current_date = '2023-08-15', `current_day_of_fiscal_year` -> 196. Because we count the number of days from Feb 1st 2023 (start of fiscal quarter) to 15th August 2023 (current_date).
{% enddocs %}

{% docs is_fiscal_month_to_date %}
Boolean flag indicating if the day `day_of_month` is less than or equal to the current_date `current_day_of_month`. This calculation is not only done for the current month, it is done for ALL months.

Example: For current_date = '2023-08-15', and date_actual = 2023-07-14 `is_fiscal_month_to_date` -> TRUE. Because the day of date_actual = 14 which is less than or equal to the day of the current_date, even though the months are different.

{% enddocs %}

{% docs is_fiscal_quarter_to_date %}
Boolean flag indicating if the day of the `day_of_fiscal_quarter` is less than or equal to the current_date `current_day_of_fiscal_quarter`. This calculation is not only done for the current fiscal quarter, it is done for ALL fiscal quarters.

Example: For current_date = '2023-08-15', and date_actual = 2023-07-14 `is_fiscal_quarter_to_date` -> TRUE. Because the `day_of_fiscal_quarter` = 14 which is less than or equal to the day of the `current_day_of_fiscal_quarter`.

{% enddocs %}

{% docs is_fiscal_year_to_date %}
Boolean flag indicating if the day of the `day_of_fiscal_year` is less than or equal to the current_date `current_day_of_fiscal_year`. This calculation is not only done for the current fiscal year, it is done for ALL fiscal years.

Example: For current_date = '2023-02-15', and date_actual = 2022-02-14 `is_fiscal_year_to_date` -> TRUE. Because the `day_of_fiscal_year` = 14 which is less than or equal to the day of the `current_day_of_fiscal_year`.

{% enddocs %}

{% docs fiscal_days_ago %}
Calculates how many days have passed between the current_date and the date_actual. For dates in the future, negatives values are used. If the date_actual is equal to the current_date then zero is used.

Example: For current_date = '2023-08-15', then '2023-08-15' would be 0, '2023-08-16' would be -1, '2023-08-14' would be 1

{% enddocs %}

{% docs fiscal_weeks_ago %}
Calculates how many weeks have passed between the current_date and the date_actual.

This is calculated based on a Monday to Sunday week.

Example: For current_date = '2023-08-15', then '2023-08-14' would be 0 (since it is still in the same week), '2023-08-21' would be -1 (in the next week), '2023-08-07' would be 1 (Monday of the past week)

{% enddocs %}

{% docs fiscal_months_ago %}
Calculates how many fiscal months have passed between the current_date and the date_actual.

Example: For current_date = '2023-08-15', then '2023-08-01' would be 0 (same month), '2023-09-28' would be -1 (in the next month), '2023-07-20' would be 1 (past month)

{% enddocs %}

{% docs fiscal_quarters_ago %}
Calculates how many fiscal quarters have passed between the current_date and the date_actual.

Example: For current_date = '2023-08-15', then '2023-08-01' would be 0 (same fiscal quarter), '2023-11-28' would be -1 (in the next fiscal quarter), '2023-07-20' would be 1 (past fiscal quarter)

{% enddocs %}

{% docs fiscal_years_ago %}
Calculates how many fiscal years have passed between the current_date and the date_actual.

Example: For current_date = '2023-08-15', then '2023-02-01' would be 0 (same fiscal year), '2024-02-28' would be -1 (in the next fiscal year), '2022-02-20' would be 1 (past fiscal year)

{% enddocs %}

{% docs is_third_business_day_of_fiscal_quarter %}

Boolean flag indicating if the date is the third business day of its fiscal quarter. Business days exclude weekends (Saturday and Sunday) and holidays (based on is_holiday). Returns 1 for the third business day of each fiscal quarter and 0 for all other dates.

This flag enables automated refresh of end-of-quarter reporting on the third business day of the month, ensuring reports are updated as soon as the month-end close process is complete.

{% enddocs %}
