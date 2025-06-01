# DBT integration tests

1. Copy .env_example inside dbt_integration_tests/packages_integration_tests folder file and rename it to .env file.

2. Set up a connection
	1. Snowflake - replace Snowflake credentials in .env file from test to your credentials.
	2. Embucket - launch Embucket locally, make sure connection parameters match Embucket launch parameters (if you have default settings, you don't need to change anything).
	3. Set the target database DBT_TARGET env (embucket or snowflake) by default it will be embucket

3. Make the sh file executable
```sh
chmod +x run_test.sh
```

4. Run integration test
```sh
./run_test.sh
```

In repos.yml there are packages with integrations tests. By default it runs integrations tests from the dbt-snowplow-web package.
Feel free to test any package from the repos_full_list.yml file.
