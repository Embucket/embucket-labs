with sfdc_opportunity_xf  AS (

        SELECT *
        FROM {{ref('sfdc_opportunity_xf')}}

), filtered AS (

        SELECT account_id,
                max(CLOSE_DATE) AS close_date
        FROM sfdc_opportunity_xf
        WHERE close_date < CURRENT_DATE
        AND is_won = True
        AND stage_is_closed = True
        GROUP BY 1

), xf AS (

        SELECT filtered.*, 
                max(sfdc_opportunity_xf.incremental_acv) AS incremental_acv
        FROM filtered
        LEFT JOIN sfdc_opportunity_xf
        ON sfdc_opportunity_xf.account_id = filtered.account_id
        AND sfdc_opportunity_xf.close_date::date = filtered.close_date::date
        GROUP BY 1, 2

), final AS (

        SELECT *, 
                {{sfdc_deal_size('incremental_acv', 'deal_size')}}
        FROM xf

)

SELECT *
FROM final