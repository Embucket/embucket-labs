
# How to run dbt-snowplow -web?

1. cd into dbt-gitlab directory
```sh
cd test/dbt_integration_tests/dbt-snowplow-web
```

2. Run dbt-gitlab project
```sh
./run_snowplow_web.sh
```

Note:
It runs with default user and password 'embucket'. No need to add .env file. 
Also, by default it runs with embucket as a target database.