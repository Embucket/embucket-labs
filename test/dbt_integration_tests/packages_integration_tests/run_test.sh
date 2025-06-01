#!/bin/bash

#set -e

# Set your DBT profiles directory relative to the script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export DBT_PROFILES_DIR="$SCRIPT_DIR"

# Skip if the repo alredy exists in target dir
SKIP_EXISTING=false

# Set default DBT target (can be overridden by .env)
DBT_TARGET="embucket"

# File with repo URLs
REPOS_FILE="$SCRIPT_DIR/repos.yml"


# Determine Python command
if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD="python"
else
    echo "Error: Neither python3 nor python found. Please install Python."
    exit 1
fi

# Check Python version
PYTHON_VERSION=$($PYTHON_CMD --version 2>&1)
if [[ "$PYTHON_VERSION" =~ "Python 2" ]]; then
    echo "Error: Python 3 is required."
    exit 1
fi

while IFS= read -r repo_url; do
  # Skip empty lines or comments
  [[ -z "$repo_url" || "$repo_url" =~ ^# ]] && continue

  repo_name=$(basename "$repo_url" .git)
  repo_dir="packages/$repo_name"

  # Setup logs directory and vars
  TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
  LOG_DIR="$SCRIPT_DIR/logs/$repo_name"
  mkdir -p "$LOG_DIR"   
  LOG_FILE="$LOG_DIR/${TIMESTAMP}.log"

  echo "###############################"
  echo ""

  # Skip if already cloned
  if [ "$SKIP_EXISTING" = true ] && [ -d "$repo_dir" ]; then
    echo "Skipping $repo_name — already exists in target/"
    continue
  fi

  echo "Cloning $repo_url..."
  git clone "$repo_url" "$repo_dir" >/dev/null 2>&1

  repo_path="$repo_dir/integration_tests"
  echo ""
  echo "###############################"
  echo ""

  if [ -d "$repo_path" ]; then
    echo "Setting up for $repo_name..."

    cd "$repo_path"
    echo ""
    # Create and activate virtual env
    echo "###############################"
    echo ""
    echo "Creating virtual environment with $PYTHON_CMD ($PYTHON_VERSION)..."
    $PYTHON_CMD -m venv env
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
        source env/Scripts/activate
    else
        source env/bin/activate
    fi
    echo ""

  
    echo "###############################"
    echo ""
    echo "Loading environment from .env..."
    source "$SCRIPT_DIR/.env"
    echo ""

    cd "$SCRIPT_DIR"


  # Install requirements
    echo ""
    echo "###############################"
    echo ""
    echo "Installing the requirements"
    pip install -r $SCRIPT_DIR/requirements.txt >/dev/null 2>&1

    echo ""
    
    echo "###############################"
    echo ""
    echo "Creating embucket database"
    # Load data and create embucket catalog if the embucket is a target 
    if [ "$DBT_TARGET" = "embucket" ]; then
       $PYTHON_CMD $SCRIPT_DIR/upload.py
    fi
    echo ""

    cd "$repo_path"

    DBT_TARGET="${DBT_TARGET:-snowflake}"

    # Force profile name to 'default'
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
      sed -i "s/^profile: .*/profile: 'default'/" dbt_project.yml
    else
      sed -i '' "s/^profile: .*/profile: 'default'/" dbt_project.yml
    fi

    echo "###############################"

    echo ""
    echo "Installing dbt core dbt-snowflake..."
    $PYTHON_CMD -m pip install --upgrade pip >/dev/null 2>&1
    $PYTHON_CMD -m pip install dbt-core dbt-snowflake > pip_install.log 2>&1
    echo ""

#set +e
    echo "###############################"
    echo ""
    echo "Running DBT tests for $repo_name with target '$DBT_TARGET'..."
    set -e
    dbt debug --target "$DBT_TARGET" 
    set +e
    dbt clean --target "$DBT_TARGET"

    if [ "$repo_name" == "snowplow_web" ]; then
      dbt deps --target "$DBT_TARGET"
      bash .scripts/integration_test.sh -d "$DBT_TARGET"
    elif [ "$repo_name" == "dbt-external-tables" ]; then
      dbt deps --target "$DBT_TARGET" | tee -a "$LOG_FILE" 
      dbt seed --full-refresh --target "$DBT_TARGET" | tee -a "$LOG_FILE"
      dbt run-operation prep_external --target "$DBT_TARGET" | tee -a "$LOG_FILE"
      dbt run-operation dbt_external_tables.stage_external_sources --vars 'ext_full_refresh: true' --target "$DBT_TARGET" | tee -a "$LOG_FILE"
      dbt run-operation dbt_external_tables.stage_external_sources --target "$DBT_TARGET" | tee -a "$LOG_FILE" 
      dbt test --target "$DBT_TARGET" | tee -a "$LOG_FILE"
    else
      dbt deps --target "$DBT_TARGET" | tee -a "$LOG_FILE"
      dbt seed --target "$DBT_TARGET" --full-refresh | tee -a "$LOG_FILE"
      seed=$(grep -E 'Done\. PASS=.*TOTAL=.*' "$LOG_FILE" | tail -n 1)
      dbt run --target "$DBT_TARGET" --full-refresh | tee -a "$LOG_FILE"
      run=$(grep -E 'Done\. PASS=.*TOTAL=.*' "$LOG_FILE" | tail -n 1)
      dbt test --target "$DBT_TARGET" | tee -a "$LOG_FILE"
      test=$(grep -E 'Done\. PASS=.*TOTAL=.*' "$LOG_FILE" | tail -n 1)
    fi
    echo "Done with $repo_name"
    echo ""

    deactivate
    cd "$SCRIPT_DIR"
  else
    echo "Skipping $repo_name — no integration_tests directory found"
  fi

echo ""
done < "$REPOS_FILE"

echo "dbt seed - $seed"
echo "dbt run - $run"
echo "dbt test - $test"


