#!/bin/bash

# Load environment variables
source env_example

# Parse command line arguments
DBT_TARGET="embucket"  # default
is_incremental=false
num_rows=10000  # default

# Parse arguments in order: incremental rows target
if [[ "$1" == "true" || "$1" == "false" ]]; then
  is_incremental="$1"
  shift
fi

if [[ "$1" =~ ^[0-9]+$ ]]; then
  num_rows="$1"
  shift
fi

if [[ -n "$1" && "$1" != "--"* ]]; then
  DBT_TARGET="$1"
  shift
fi

# Parse any remaining --flags
while [[ "$#" -gt 0 ]]; do
  case $1 in
    --target) 
      DBT_TARGET="$2"
      shift 2 
      ;;
    --incremental)
      is_incremental=true
      shift
      ;;
    --rows)
      num_rows="$2"
      shift 2
      ;;
    *) 
      # Check if it's a number (for rows)
      if [[ "$1" =~ ^[0-9]+$ ]]; then
        num_rows="$1"
        shift
      else
        echo "Unknown parameter: $1"; exit 1
      fi
      ;;
  esac
done


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
echo "###############################"
echo ""
# Set incremental flag from command line argument, default to true

# FIRST RUN
echo "Generating events"
$PYTHON_CMD gen_events.py "$num_rows"

if [ "$DBT_TARGET" = "embucket" ]; then

    echo "Setting up Docker container"
        ./setup_docker.sh


    sleep 20

fi

echo ""
echo "###############################"
echo ""

echo "Loading events"
$PYTHON_CMD load_events.py events_yesterday.csv "$DBT_TARGET"

echo ""
echo "###############################"
echo ""

echo "Running dbt"
./run_snowplow_web.sh --target "$DBT_TARGET" 2>&1 | tee dbt_output.log

echo ""
echo "###############################"
echo ""

if [ "$is_incremental" == false ]; then
    # Parse dbt results and load into Snowflake
    echo "Parsing dbt results..."
    $PYTHON_CMD parse_dbt_simple.py dbt_output.log "$num_rows" "$is_incremental" "$DBT_TARGET"

    echo ""

    
    if [ "$DBT_TARGET" = "embucket" ]; then
    # Update the errors log and run results
        echo "###############################"
        echo ""
        echo "Updating the errors log and total results"
        ./statistics.sh
        echo ""

    # Generate assets after the run
        echo "###############################"
        echo ""
        echo "Updating the chart result"
            $PYTHON_CMD generate_dbt_test_assets.py --output-dir dbt-snowplow-web/assets --errors-file dbt-snowplow-web/assets/top_errors.txt
        echo ""
        echo "###############################"
        echo ""
    else
        echo "###############################"
        echo ""
        echo "It was snowflake run, no assets will be generated"
        echo ""
        echo "###############################"
        echo ""
    fi

fi


# SECOND RUN INCEREMENTAL
if [ "$is_incremental" == true ]; then

    echo "Loading events"
    $PYTHON_CMD load_events.py events_today.csv "$DBT_TARGET"

    echo "Running dbt"
    ./run_snowplow_web.sh --target "$DBT_TARGET"

    # Parse dbt results and load into Snowflake
    echo "Parsing dbt results..."
    $PYTHON_CMD parse_dbt_simple.py dbt_output.log "$num_rows" "$is_incremental" "$DBT_TARGET"

    if [ "$DBT_TARGET" = "embucket" ]; then
    # Update the errors log and run results
        echo "###############################"
        echo ""
        echo "Updating the errors log and total results"
        ./statistics.sh
        echo ""

    # Generate assets after the run
        echo "###############################"
        echo ""
        echo "Updating the chart result"
            $PYTHON_CMD generate_dbt_test_assets.py --output-dir dbt-snowplow-web/assets --errors-file dbt-snowplow-web/assets/top_errors.txt
        echo ""
        echo "###############################"
        echo ""
    else
        echo "It was snowflake run, no assets will be generated"
    fi

fi
