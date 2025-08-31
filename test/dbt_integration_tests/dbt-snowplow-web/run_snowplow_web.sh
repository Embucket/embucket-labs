#!/bin/bash

echo ""
echo "Cloning dbt-snowplow-web repository"
git clone https://github.com/snowplow/dbt-snowplow-web.git
echo ""

echo "###############################"
echo ""
echo "Copy files"

cp dbt_project.yml dbt-snowplow-web/
cp profiles.yml dbt-snowplow-web/
cp run.sh dbt-snowplow-web/
cp statistics.sh dbt-snowplow-web/
cp requirements.txt dbt-snowplow-web/
cp generate_dbt_test_assets.py dbt-snowplow-web/

chmod +x run.sh statistics.sh

# cp events.csv dbt-snowplow-web/seeds/
# cp seeds.yml dbt-snowplow-web/seeds/

#cp snowplow_web_base_events_this_run.sql dbt-snowplow-web/models/base/scratch/snowflake/
#cp snowplow_web_consent_events_this_run.sql dbt-snowplow-web/models/optional_modules/consent/scratch/snowflake/
echo ""

echo "###############################"
echo ""
echo "Run dbt-snowplow-web"

cd dbt-snowplow-web/
./run.sh --target embucket

echo ""