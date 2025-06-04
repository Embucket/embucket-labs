{% docs mart_processed_refunds_monthly %}

View sum of refund amounts processed monthly; period indicates accounting period in the billing system; period indicates accounting period in the billing system.

{% enddocs %}

{% docs mart_processed_iia_monthly %}

View sums of invoice item adjustment amounts processed monthly for both credit and charges; period indicates accounting period in the billing system

{% enddocs %}

{% docs mart_payments_refunds_against_future_invoices %}

View sums of payment or refunds made towards future dated invoices; period indicates accounting period in the billing system 

{% enddocs %}

{% docs mart_processed_cba_monthly %}

Table showing the cba increase, decrease, the overpayment, the running totals of the increase and decrease and final credit balance in the accounting period; period indicates accounting period in the billing system

{% enddocs %}

{% docs mart_historical_balance_by_payment_terms_bucket_monthly %}

View of historical balances with payment terms and aging bucket by month; period indicates accounting period in the billing system

{% enddocs %}

{% docs mart_invoices_in_preview_monthly %}

The count and amount of invoices to be billed in the future aka invoices in preview or pending invoices

{% enddocs %}

{% docs mart_ar_per_ssp_channel_monthly %}

Shows balances and count per SSP channel vs. the total balances for all open invoices monthly; period indicates accounting period in the billing system

{% enddocs %}

{% docs mart_payment_type_monthly %}

Table comparing total payment amounts and volumes per type per month; period indicates accounting period in the billing system 

{% enddocs %}

{% docs mart_current_ar_by_aging_payment_terms %}

Table showing currently overdue invoices with aging bucket and payment terms

{% enddocs %}

{% docs mart_booking_by_order_type_monthly %}

Booking amount and count by sales type; period indicates accounting period in the billing system; period indicates accounting period in the billing system 

{% enddocs %}

{% docs mart_booking_by_payment_schedule_opportunity_category_monthly %}

Compares standard vs. all bookings to determine percentage of non-standard booking where non-standard booking is defined as non-prepaid for multiyear orders and ramps; period indicates accounting period in the billing system

{% enddocs %}

{% docs mart_collections_monthly %}

The table shows how percentually the payment received in one month is applied to invoices in various periods

{% enddocs %}

{% docs mart_cdot_invoices_manual_intervention_monthly %}

The table shows how percentually the payment received in one month is applied to invoices in various periods; period indicates accounting period in the billing system

{% enddocs %}

{% docs mart_public_sector_ar_monthly %}

Compare balances and count per segment vs. the total balances for all open invoices monthly; period indicates accounting period in the billing system

{% enddocs %}

{% docs mart_web_direct_vs_sales_assisted_ar_monthly %}

Compare balances and count per path vs. the total balances for all open invoices monthly; period indicates accounting period in the billing system

{% enddocs %}

{% docs mart_booking_billing_ar_monthly %}

Comparison of booking, invoicing and payment amounts and counts

{% enddocs %}

{% docs mart_available_to_renew_variance %}

Aggregated model built based on fct_available_to_renew reflecting the variance between Zuora and SFDC ATR amounts

Columns:
fiscal_quarter_name_fy
dim_crm_account_id
available_to_renew - Zuora subscriptions reflecting the amount available to renew in a given quarter
arr_basis - SFDC opportunity amounts reflecting the amount available to renew in a given quarter

{% enddocs %}