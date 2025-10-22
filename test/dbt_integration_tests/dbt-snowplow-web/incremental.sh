#!/bin/bash

# Incremental Test Script for dbt-snowplow-web
#
# Prerequisites: Run ./setup_embucket.sh first to:
#   - Set up embucket Docker container (local or EC2)
#   - Generate and upload CSV data files
#   - Load events_yesterday.csv into embucket
#
# Usage:
#   ./incremental.sh [incremental] [num_rows] [target]
#
# Examples:
#   ./incremental.sh false 10000 embucket  # Full run
#   ./incremental.sh true 10000 embucket   # Incremental run
#
# Flow:
#   Non-incremental (false):
#     1. Run dbt on events_yesterday.csv (already loaded)
#     2. Parse results and generate assets
#
#   Incremental (true):
#     1. Run dbt on events_yesterday.csv (already loaded)
#     2. Load events_today.csv
#     3. Run dbt again (incremental models will process new data)
#     4. Parse results and generate assets

# Load environment variables if .env exists
if [ -f .env ]; then
    source .env
else
    echo "Warning: .env file not found. Using default values."
fi

# Parse command line arguments first
DBT_TARGET="embucket"  # default
is_incremental=false
num_rows=10000  # default
run_type="manual"  # default, can be overridden by environment variable RUN_TYPE

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

# Check for RUN_TYPE environment variable (for GitHub Actions)
if [[ -n "$RUN_TYPE" ]]; then
  run_type="$RUN_TYPE"
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

# Setup target-specific environment  
echo ""
echo "###############################"
echo "Target: $DBT_TARGET"
echo "###############################"
echo ""

if [ "$DBT_TARGET" = "embucket" ]; then
    EMBUCKET_HOST=${EMBUCKET_HOST:-localhost}
    EMBUCKET_PORT=${EMBUCKET_PORT:-3000}
    
    # Auto-setup for local, health check for remote
    if [ "$EMBUCKET_HOST" = "localhost" ] || [ "$EMBUCKET_HOST" = "127.0.0.1" ]; then
        echo "Local Embucket setup detected (EMBUCKET_HOST=$EMBUCKET_HOST)"
        echo "Running setup to ensure fresh embucket container..."
        echo ""
        
        ./setup_embucket.sh -force
        
        if [ $? -ne 0 ]; then
            echo ""
            echo "❌ Embucket setup failed. Please check the error messages above."
            exit 1
        fi
        
        echo ""
        echo "✓ Embucket setup completed successfully"
        echo ""
    else
        echo "Remote Embucket setup detected (EMBUCKET_HOST=$EMBUCKET_HOST)"
        echo "Checking if Embucket is accessible at $EMBUCKET_HOST:$EMBUCKET_PORT..."
        
        if ! curl -sf "http://$EMBUCKET_HOST:$EMBUCKET_PORT/health" > /dev/null 2>&1; then
            echo ""
            echo "❌ Error: Embucket is not accessible at $EMBUCKET_HOST:$EMBUCKET_PORT"
            echo ""
            echo "Please ensure embucket is running on EC2."
            echo "To set it up, run: ./setup_embucket.sh"
            echo ""
            exit 1
        fi
        
        echo "✓ Embucket is accessible"
        echo ""
    fi
else
    echo "Target is $DBT_TARGET - skipping Embucket setup"
    echo ""
fi

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

# Note: Setup (generating events, Docker setup, initial data load) 
# should be done via ./setup_embucket.sh before running this script
# The health check at the top ensures embucket is ready

# For non-incremental runs, data is already loaded by setup_embucket.sh
# For incremental runs, we only need to load events_today.csv (done later)
echo "Loading events"
$PYTHON_CMD load_events.py events_yesterday.csv "$DBT_TARGET"

echo ""
echo "###############################"
echo ""
echo "FIRST RUN"
echo "Running dbt on initial data (events_yesterday.csv)..."
./run_snowplow_web.sh --target "$DBT_TARGET" 2>&1 | tee dbt_output.log

echo ""
echo "###############################"
echo ""

if [ "$is_incremental" == false ]; then
    # Parse dbt results and load into Snowflake
    echo "Parsing dbt results..."
    $PYTHON_CMD parse_dbt_simple.py dbt_output.log "$num_rows" "$is_incremental" "$DBT_TARGET" "$run_type"

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


# SECOND RUN INCREMENTAL
if [ "$is_incremental" == true ]; then

    echo ""
    echo "###############################"
    echo ""
    echo "Loading events_today.csv for incremental run..."
    $PYTHON_CMD load_events.py events_today.csv "$DBT_TARGET"

    echo ""
    echo "###############################"
    echo ""
    echo "Running dbt (incremental run #2)..."
    ./run_snowplow_web.sh --target "$DBT_TARGET" 2>&1 | tee dbt_output.log

    echo ""
    echo "###############################"
    echo ""
    
    # Parse dbt results and load into Snowflake
    echo "Parsing dbt results..."
    $PYTHON_CMD parse_dbt_simple.py dbt_output.log "$num_rows" "$is_incremental" "$DBT_TARGET" "$run_type"

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
