
![DBT SNowplow Web run results](https://raw.githubusercontent.com/Embucket/embucket/assets_dbt/assets_dbt_snowplow/dbt_success_badge.svg)
## DBT Snowplow Web run results:
![DBT Snowplow Web run results visualization](https://raw.githubusercontent.com/Embucket/embucket/assets_dbt/assets_dbt_snowplow/dbt_run_status.png)

# How to run dbt-snowplow -web?

1. cd into dbt-snowplow-web directory
```sh
cd test/dbt_integration_tests/dbt-snowplow-web
```

2. Run dbt-snowplow-web project
```sh
./run_snowplow_web.sh
```

Note:
It runs with default user and password 'embucket'. No need to add .env file. 
Also, by default it runs with embucket as a target database.