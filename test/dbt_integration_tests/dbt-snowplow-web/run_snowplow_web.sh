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

# Copy .env to the cloned directory before changing into it (if it exists)
if [ -f .env ]; then
    cp .env dbt-snowplow-web/
fi
cd dbt-snowplow-web/
# Parse --target and --model arguments
while [[ "$#" -gt 0 ]]; do
  case $1 in
    --target) 
      DBT_TARGET="$2"
      shift 2 
      ;;
    --model) 
      DBT_MODEL="$2"
      shift 2 
      ;;
    *) echo "Unknown parameter: $1"; exit 1 ;;
  esac
done
# Set DBT_TARGET to "embucket" if not provided
export DBT_TARGET=${DBT_TARGET:-"embucket"}
echo ""

# Add env's
echo "###############################"
echo ""
echo "Loading environment variables"
if [ -f .env ]; then
    source .env
else
    echo "Warning: .env file not found. Using default values."
fi

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
    dbt debug --target "$DBT_TARGET"
#    dbt clean --target "$DBT_TARGET"
    dbt deps --target "$DBT_TARGET"
# dbt seed
        dbt seed --full-refresh --target "$DBT_TARGET"
#  dbt run
    if [ -n "$DBT_MODEL" ]; then
        dbt run --select +"$DBT_MODEL" --target "$DBT_TARGET" 2>&1 | tee assets/run.log
    else
        dbt run --target "$DBT_TARGET" 2>&1 | tee assets/run.log
	#dbt run --full-refresh
    fi 
    #dbt test --target "$DBT_TARGET"

cd ..