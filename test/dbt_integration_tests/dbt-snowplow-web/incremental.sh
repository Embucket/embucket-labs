#!/bin/bash

# Set incremental flag from command line argument, default to true
is_incremental=${1:-false}
# Set number of rows to generate, default to 1000
num_rows=${2:-10000}

echo "Setting up Docker container"
./setup_docker.sh

sleep 5

# install requirements

python3 -m venv env
source env/bin/activate

python3 -m pip install --upgrade pip >/dev/null 2>&1

pip install -r requirements.txt >/dev/null 2>&1

# FIRST RUN
echo "Generating events"
python3 gen_events.py $num_rows

echo "Loading events"
python3 load_events.py events_day_before_yesterday.csv

# echo "Commenting out seed file copies in run_snowplow_web.sh"
# sed -i '' 's/^cp events.csv dbt-snowplow-web\/seeds\//#cp events.csv dbt-snowplow-web\/seeds\//' run_snowplow_web.sh
# sed -i '' 's/^cp seeds.yml dbt-snowplow-web\/seeds\//#cp seeds.yml dbt-snowplow-web\/seeds\//' run_snowplow_web.sh

echo "Running dbt"
./run_snowplow_web.sh

if [ "$is_incremental" == true ]; then

# SECOND RUN INCEREMENTAL

echo "Loading events"
python3 load_events.py events_yesterday.csv

echo "Running dbt"
./run_snowplow_web.sh

fi
