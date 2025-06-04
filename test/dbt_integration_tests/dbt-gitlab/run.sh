#!/bin/bash

# Parse --target argument
while [[ "$#" -gt 0 ]]; do
  case $1 in
    --target) DBT_TARGET="$2"; shift ;;
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


# Install DBT dependencies
echo "###############################"
echo ""
echo "Installing dbt core dbt-snowflake..."
$PYTHON_CMD -m pip install --upgrade pip >/dev/null 2>&1
$PYTHON_CMD -m pip install dbt-core dbt-snowflake >/dev/null 2>&1
echo ""


# Load .env
echo "###############################"
echo ""
echo "Loading environment from .env..."
source .env
echo ""

# Install requirements
echo ""
echo "###############################"
echo ""
echo "Installing the requirements"
pip install -r requirements.txt >/dev/null 2>&1
echo ""

# Load data and create embucket catalog if the embucket is a target 
echo "###############################"
echo ""
echo "Creating embucket database"
if [ "$DBT_TARGET" = "embucket" ]; then
   $PYTHON_CMD upload.py
fi
echo ""

# Run DBT commands
echo "###############################"
echo ""
    dbt debug
    dbt clean
    dbt deps
    if [ "$DBT_TARGET" = "embucket" ]; then
    dbt seed
    fi
#    dbt seed --select gitlab_db_project_statistics
#    dbt run --full-refresh
    # if [ "$DBT_TARGET" = "embucket" ]; then
    # dbt run --full-refresh --select result:success --state target_to_run 2>&1 | tee run.log
    # else 
    # dbt run --full-refresh --select result:success --state target_to_run 2>&1 | tee run.log
    # fi
    dbt run --full-refresh --select result:success --state target_to_run 2>&1 | tee run.log
deactivate