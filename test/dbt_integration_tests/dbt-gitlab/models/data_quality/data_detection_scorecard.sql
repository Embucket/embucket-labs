

WITH scorecard AS (

    SELECT 
      'Product Data Quality Scorecard'                                                                                                        AS scorecard_name,
      'The Dashboard displays the overall quality of Product Usage Data as measured by the status of individual Data Quality Detection Rules' AS scorecard_description,
      '2021-06-24'                                                                                                                            AS release_date,
      'V1.0'                                                                                                                                  AS version_number          
)

{{ dbt_audit(
    cte_ref="scorecard",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2021-07-20",
    updated_date="2021-07-20"
) }}