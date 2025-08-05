#!/bin/bash

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

# Determine which Python command to use
if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD="python"
else
    echo "Error: Neither python3 nor python found. Please install Python."
    exit 1
fi

# Creating virtual environment
echo "###############################"
echo ""
echo "Creating virtual environment with $PYTHON_CMD..."
$PYTHON_CMD -m venv env
source env/bin/activate
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

# Install DBT dependencies
echo "###############################"
echo ""
echo "Installing dbt core dbt-snowflake..."
$PYTHON_CMD -m pip install --upgrade pip >/dev/null 2>&1
$PYTHON_CMD -m pip install dbt-core==1.9.8 dbt-snowflake==1.9.1 >/dev/null 2>&1
echo ""

# # Load .env
# echo "###############################"
# echo ""
# echo "Loading environment from .env..."
# source .env
# echo ""

# Install requirements
echo ""
echo "###############################"
echo ""
echo "Installing the requirements"
pip install -r requirements.txt >/dev/null 2>&1
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
        dbt run --full-refresh --select +"$DBT_MODEL" 2>&1 | tee assets/run.log
    else
        dbt run --full-refresh  2>&1 | tee assets/run.log
	#dbt run --full-refresh
    fi 

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
   $PYTHON_CMD generate_dbt_test_assets.py 
fi
echo ""
echo "###############################"
echo ""

deactivate
