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
echo ""
#cp run.sh dbt-snowplow-web/
#cp statistics.sh dbt-snowplow-web/
#cp requirements.txt dbt-snowplow-web/
#cp generate_dbt_test_assets.py dbt-snowplow-web/

#chmod +x run.sh statistics.sh

# cp events.csv dbt-snowplow-web/seeds/
# cp seeds.yml dbt-snowplow-web/seeds/

#cp snowplow_web_base_events_this_run.sql dbt-snowplow-web/models/base/scratch/snowflake/
#cp snowplow_web_consent_events_this_run.sql dbt-snowplow-web/models/optional_modules/consent/scratch/snowflake/

# Determine which Python command to use
echo "###############################"
echo ""
echo "Determining which Python command to use..."
if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD="python"
else
    echo "Error: Neither python3 nor python found. Please install Python."
    exit 1
fi


echo ""

echo "###############################"
echo ""
echo "Run dbt-snowplow-web"

cd dbt-snowplow-web/
# Parse --target and --model arguments
while [[ "$#" -gt 0 ]]; do
  case $1 in
    --target) DBT_TARGET="$2"; shift ;;
    --model) DBT_MODEL="$2"; shift ;;
    *) echo "Unknown parameter: $1"; exit 1 ;;
  esac
  shift
done
# Set DBT_TARGET to "embucket" if not provided
export DBT_TARGET=${DBT_TARGET:-"embucket"}
echo ""

# Add env's
echo "###############################"
echo ""
echo "Adding environment variables"
export EMBUCKET_HOST=localhost
export EMBUCKET_PORT=3000
export EMBUCKET_PROTOCOL=http
export EMBUCKET_ACCOUNT=test
export EMBUCKET_USER=${EMBUCKET_USER:-embucket}
export EMBUCKET_PASSWORD=${EMBUCKET_PASSWORD:-embucket}
export EMBUCKET_ROLE=SYSADMIN
export EMBUCKET_DATABASE=EMBUCKET
export EMBUCKET_WAREHOUSE=COMPUTE_WH
export EMBUCKET_SCHEMA=public

echo ""
# Install DBT dependencies
echo "###############################"
echo ""
echo "Installing dbt core dbt-snowflake..."
$PYTHON_CMD -m pip install --upgrade pip >/dev/null 2>&1
$PYTHON_CMD -m pip install dbt-core==1.9.8 dbt-snowflake==1.9.1 >/dev/null 2>&1
echo ""

mkdir -p assets
mkdir -p logs

# Run DBT commands
echo "###############################"
echo ""
    dbt debug
    dbt clean
    dbt deps
# dbt seed
        dbt seed --full-refresh
#  dbt run
    if [ -n "$DBT_MODEL" ]; then
        dbt run --select +"$DBT_MODEL" 2>&1 | tee assets/run.log
    else
        dbt run  2>&1 | tee assets/run.log
	#dbt run --full-refresh
    fi 
    # dbt test

cd ..