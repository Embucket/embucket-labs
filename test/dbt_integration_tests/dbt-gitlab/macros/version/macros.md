{% docs sales_wave_2_3_metrics %}

This macro contains transformations for Health Score metrics and is used in the free Self-Managed branch of the lineage. In theory, it should be have the
same metrics as the paid version of the lineage, although visual inspection shows that is not the case.

This macro should be reevaluated and possibly dropped in a future iteration.

{% enddocs %}

{% docs ping_instance_wave_metrics %}

This macro contains transformations for Health Score metrics and is used in the paid Self-Managed branch of the lineage.

The `null_negative_numbers` macro is encoded in this macro, which obscures when metrics have timed out by nulling the value instead of showing a negative number.

This entire macro should be reevaluated and possibly dropped in a future iteration.

{% enddocs %}
