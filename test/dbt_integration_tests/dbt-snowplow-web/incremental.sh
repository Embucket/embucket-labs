#!/bin/bash

# Set DBT_TARGET environment variable
export DBT_TARGET="embucket"

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

# Creating virtual environment
echo "###############################"
echo ""
echo "Creating virtual environment with $PYTHON_CMD..."
$PYTHON_CMD -m venv env
source env/bin/activate
echo ""

# Install requirements
echo ""
echo "###############################"
echo ""
echo "Installing the requirements"
$PYTHON_CMD -m pip install --upgrade pip >/dev/null 2>&1
pip install -r requirements.txt >/dev/null 2>&1
echo ""

# Set incremental flag from command line argument, default to true
is_incremental=${1:-false}
# Set number of rows to generate, default to 1000
num_rows=${2:-10000}

echo "Setting up Docker container"
./setup_docker.sh

sleep 5

# FIRST RUN
echo "Generating events"
$PYTHON_CMD gen_events.py $num_rows

echo "Loading events"
$PYTHON_CMD load_events.py events_day_before_yesterday.csv

# echo "Commenting out seed file copies in run_snowplow_web.sh"
# sed -i '' 's/^cp events.csv dbt-snowplow-web\/seeds\//#cp events.csv dbt-snowplow-web\/seeds\//' run_snowplow_web.sh
# sed -i '' 's/^cp seeds.yml dbt-snowplow-web\/seeds\//#cp seeds.yml dbt-snowplow-web\/seeds\//' run_snowplow_web.sh

echo "Running dbt"
./run_snowplow_web.sh

# Update the errors log and run results
echo "###############################"
echo ""
echo "Updating the errors log and total results"
if [ "$DBT_TARGET" = "embucket" ]; then
   ./statistics.sh
fi
echo ""

# Generate assets after the run
echo "###############################"
echo ""
echo "Updating the chart result"
if [ "$DBT_TARGET" = "embucket" ]; then
   $PYTHON_CMD generate_dbt_test_assets.py --output-dir dbt-snowplow-web/assets --errors-file dbt-snowplow-web/assets/top_errors.txt
fi
echo ""
echo "###############################"
echo ""

if [ "$is_incremental" == true ]; then

# SECOND RUN INCEREMENTAL

echo "Loading events"
$PYTHON_CMD load_events.py events_yesterday.csv

echo "Running dbt"
./run_snowplow_web.sh

# Update the errors log and run results
echo "###############################"
echo ""
echo "Updating the errors log and total results"
if [ "$DBT_TARGET" = "embucket" ]; then
   ./statistics.sh
fi
echo ""

# Generate assets after the run
echo "###############################"
echo ""
echo "Updating the chart result"
if [ "$DBT_TARGET" = "embucket" ]; then
   $PYTHON_CMD generate_dbt_test_assets.py --output-dir dbt-snowplow-web/assets --errors-file dbt-snowplow-web/assets/top_errors.txt
fi
echo ""
echo "###############################"
echo ""

fi

# test