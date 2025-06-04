"""
Module for checking data science exposures for models.
Automatically detects when dbt model changes could impact Data Science exposures by finding
any DS exposures that list the changed models in their depends_on configuration.
"""

from typing import List, Optional, Tuple
import argparse
import os
import sys

from dbt.cli.main import dbtRunner


# Constants
CI_PROFILE_TARGET = os.getenv("CI_PROFILE_TARGET", "").split()


def get_exposures_for_model(dbt: dbtRunner, model: str) -> List[str]:
    """
    Get all DS exposures that depend on a given model.

    Args:
        dbt: dbt runner instance to execute commands
        model: Name of the model to check

    Returns:
        List of exposure names that depend on this model

    Raises:
        Exception: If dbt command fails
    """
    cli_args = [
        "list",
        "--select",
        f"{model}+",
        "--resource-type=exposure",
    ] + CI_PROFILE_TARGET

    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        res = dbt.invoke(cli_args)
        sys.stdout = old_stdout

    # Check for dbt command failure
    if not res.success:
        raise Exception(f"dbt command failed: {res.exception}")

    return res.result if res.result is not None else []


def check_exposures(models: List[str]) -> None:
    """
    Check for data science exposures for each model.
    Prints findings and raises error if exposures are found.

    Args:
        models: List of models to check for DS exposure dependencies

    Raises:
        ValueError: If any model has downstream DS exposures
        Exception: If dbt command fails to execute
    """
    has_exposure = False
    dbt = dbtRunner()

    # Clean model names
    cleaned_models = []
    for model in models:
        cleaned_models.extend(
            name.strip() for name in model.split("\n") if name.strip()
        )

    if not cleaned_models:
        print("No models to check")
        return

    print(f"\nChecking model(s): {', '.join(cleaned_models)}")

    try:
        # Check each model for exposures
        for model in cleaned_models:
            exposures = get_exposures_for_model(dbt, model)

            count = len(exposures)

            if count > 0:
                has_exposure = True
                print(f"\nFound {count} DS exposures for {model}:")
                print("----------------------------------------")
            else:
                print(f"\nNo DS exposures found for {model}")

            for exposure in exposures:
                print(exposure)

            print("----------------------------------------")

    except Exception as e:
        print(f"Error checking exposures: {str(e)}")
        raise

    if has_exposure:
        raise ValueError(
            "One or more models have DS exposures that depend on them. Please check these models before proceeding!"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check if dbt models have downstream DS exposures"
    )
    parser.add_argument("models", nargs="+", help="List of model names to check")
    args = parser.parse_args()

    try:
        check_exposures(args.models)
    except Exception as e:
        print(f"Error running dependency check: {str(e)}")
        sys.exit(1)
