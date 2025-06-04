{% docs atr_change_flag %}

Field showing movement of the top subscriptions by arr each month. Available values - 'Became available to renew', 'Dropped as available to renew', 'New subscription', 'Subscription ends this month' or 'Top available to renew excluded deal'. These are calculated as shown below:

### Top available to renew excluded deal

This flags subscriptions where the `is_available_to_renew` flag is false. The top 10 by arr are shown each month.

### Subscription ends this month

Either the multi_year_booking_subscription_end_month or bookings_term_end_month is in the month being checked.

### Became available to renew

This model uses the daily snapshot of mart_available_to_renew so by using the LAG function to compare the `is_available_to_renew` flag to the previous day, we can see which went from false to true indicating that they became available to renew in that month. The top 10 by arr are shown.

### Dropped as available to renew

The inverse of the above. The `is_available_to_renew` flag went from true to false.

### New subscription

A new `subscription_name` has appeared this month, limited to top 10 by arr.

{% enddocs %}

{% docs rpt_subscription_renewal_linking %}

This report suggests previous term subscription for renewal use cases where the subscription is continued in a new subscription and not via amendment on the existing one. There are 3 use cases with sub-use cases covered by this model. The first use case is where the opportunity is a renewal and quote is new subscrption; the sub cases here are the renewal starting immediately after previous subscription ends and a late renewal. The second use case is where both the opportunity and the quote are built as new business/subscription but are in fact continuation of one another, e.g. legacy ramps. The third use case is the change of entity.

This is a functional report meaning that it is used for an upload of the renewal data where the renewal fields were not already populated. This report should not be used as a list of all linked subscription as it only contains not linked subscription or suggestion for new linking e.g. after debook and rebook.

renewal_close_month - the month the related opportunity was closed

renewal_dim_crm_opportunity_id - the opportunity for the renewal

renewal_subscription_name - subscription name created for the renewal opportunity

renewal_subscription_start_date - the start date of the renewal subscription

previous_term_dim_crm_account_id - CRM ID of the suggested previous term subscription

previous_term_subscription_name - the name of the suggested previous subscription

previous_term_subscription_id - previous subscription ID, last subscription version ID available in Snowflake

previous_term_subscription_end_date - the end date of the suggested previous term

zuora_renewal_subscription_name - the renewal subscription field on the suggested previous subscription

use_case - use case type

{% enddocs %}

{% docs rpt_accounting_period_balance_monthly %}

The report mirrors Zuora accounting period monthly balances.

The  following columns are included:

### Fiscal Year

### Fiscal Quarter

### Period

### Starting Accounts Receivable 
Ending Accounts Receivable from previous month

### Total Billings 
Total billed in a month including tax

### Payments
All payments received in the month regardless whether applied or not applied to invoices. The amount may vary from Zuora if payments were backdated as Zuora takes a snapshot of this information for the accounting period but if payments are made after the snapshot was taken the payments will not flow in. The variance will be in favor of GitLab

### Overpayments 
Payments that were not applied to invoices

### Refunds 
All refunds made from invoices and accounts

### Adjustments 
Invoice item adjustments made to invoices

### Ending Accounts Receivable = Starting Accounts Receivable + Total Billings - Payments minus Overpayments + Refunds - Adjustments

### Invoice Aging Buckets which are as follows:

Current - current due date open invoices balances

Further buckets: 1 to 30 days past due, 31 to 60 days past due, 61 to 90 days past due, 91 to 120 days past due, more than 120 days past due

### Total Invoice Aging Balance 
Total of all aging buckets

### Variance between Ending Accounts Receivable and Total Invoice Aging Balance

### Credit Balance (Customer Refunds) 
Credit balance adjustments running total for the month

### Payments or Refunds on Future Dated Invoices

### Final Check 
The variance between Ending Accounts Receivable and Total Invoice Aging Balance is taken less Credit Balance and Payments or Refunds on Future Dated Invoices - this should show a 0 variance however it is possible that due to backdated payment application this amount will not balance out to 0

{% enddocs %}

{% docs rpt_booking_billing_collections_monthly %}

Booking - total booking amount of booked opportunities in the month

Billing - total billing, billing exclusive the tax amount and tax amount of the invoicing in the month

Collections - total of payments applied to invoices in the month

{% enddocs %}

{% docs rpt_dso_cei_monthly %}

Days Sales Outstanding

Average AR = (Starting Accounts Receivable / Ending Accounts Receivable) / 2

DSO = (Average AR / Total Billing) * Number of Days in Period

Collection Effectiveness Index

CEI = (Total Beginning AR + Total Billing - Total AR at End of Period) / (Total Beginning AR + Total Billing - Total Current AR) * 100

{% enddocs %}

{% docs rpt_potential_bad_debt %}

The model is designed to provide a comprehensive view of potential bad debt, including both current and future (pending/preview/not billed) invoices. The invoice information is linked to the corresponding opportunity information. A 1:1 relation between invoice and opportunity is not always possible hence the column 'possible_opportunity_id' was created to flag possible other opportunities.

{% enddocs %}
