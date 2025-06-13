{{ config(
     materialized = "table"
) }}

WITH oppt_latest_date AS (

    SELECT MAX(score_date) AS score_date
    FROM MY_SNOWFLAKE_PREP_DATABASE.workspace_data_science.opportunity_forecasting_scores

), oppt AS (

   SELECT
      dim_crm_opportunity_id AS crm_opportunity_id,
      score_date AS forecast_score_date,
      forecasted_days_remaining AS forecasted_days_remaining,
      forecasted_close_won_date AS forecasted_close_won_date,
      forecasted_close_won_quarter AS forecasted_close_won_quarter,
      score_group AS forecasted_score_group,
      insights AS forecast_insights,
      submodel AS forecast_submodel,
      model_version AS forecast_model_version
    FROM MY_SNOWFLAKE_PREP_DATABASE.workspace_data_science.opportunity_forecasting_scores

)

SELECT
  a.crm_opportunity_id,
  a.forecast_score_date::DATE AS forecast_score_date,
  a.forecasted_days_remaining,
  a.forecasted_close_won_date,
  a.forecasted_close_won_quarter,
  a.forecasted_score_group,
  a.forecast_insights,
  a.forecast_submodel,
  a.forecast_model_version
 FROM oppt a
INNER JOIN oppt_latest_date b
   ON a.forecast_score_date = b.score_date
