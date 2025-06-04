"""
Module returning a csv with downstream dbt dependencies and
data science exposures for a list of model provided by the user.
"""

import os

import pandas as pd
from dbt.cli.main import dbtRunner, dbtRunnerResult

ENCODING = "utf-8"


def dbt_model_dependencies_list(model_name_list: list, resource_type: str) -> list:
    """Takes in a list of models, returns the model and its dependencies"""

    # create empty lists to hold models
    model_dependencies = []

    # initialize dbt invoke
    dbt = dbtRunner()

    # for each create CLI args as a list of strings
    for model in model_name_list:
        cli_args = ["list", "--select", f"{model}+", f"--resource-type={resource_type}"]

        # run the command for models
        res_models: dbtRunnerResult = dbt.invoke(cli_args)

        # get counts
        if res_models.result is not None:
            count_dependencies = len(res_models.result)
        else:
            count_dependencies = 0

        # append results in table for models
        model_dependencies.append(
            {
                "model_name": f"{model}",
                "dependencies": res_models.result,
                "count_dependencies": count_dependencies,
            }
        )

    return model_dependencies


def get_file_name() -> str:
    """
    Find directory name

    Navigate to the file models_to_check.csv and add the dbt model names (ex. fct_behavior_structured_event).
    The file should contain a list of model names to test with no header, with one model name per line.

    example file list:
    gitlab_dotcom_audit_events
    dim_crm_opportunity
    """
    dirname = os.getcwd()
    return f"{dirname}/models_to_check.csv"


# Create empty set to append the cleaned model names to


def get_model_names(file_name: str) -> set:
    """
    open file and add models to the list
    """
    models = set()
    if os.path.exists(file_name):
        with open(file=file_name, mode="r", newline="", encoding=ENCODING) as file:
            lines = file.readlines()
            for line in lines:
                models.add(line.strip().lower())

    else:
        raise FileNotFoundError(
            f"File {file_name} not found! Please create one and fill the data as per instruction"
        )
    return models


def df_to_csv(file_name: str, df: pd.DataFrame) -> None:
    """
    create csv of the output called models_with_dependencies.csv
    """
    df.to_csv(file_name, index=False, encoding=ENCODING)


def generate_results(model_names: list) -> None:
    """
    generate results
    Note: model dependencies count includes the model
    requested + all downstream, so the minimum number of dependencies is 1
    """
    models_with_dependencies = dbt_model_dependencies_list(
        model_name_list=model_names, resource_type="model"
    )
    models_with_exposures = dbt_model_dependencies_list(
        model_name_list=model_names, resource_type="exposure"
    )

    # convert lists to dataframes in order to merge the results into a single output
    models_with_dependencies_df = pd.DataFrame(models_with_dependencies).rename(
        columns={
            "model_name": "model_name",
            "dependencies": "dbt_model_dependencies",
            "count_dependencies": "dbt_model_dependencies_count",
        }
    )
    models_with_exposures_df = pd.DataFrame(models_with_exposures).rename(
        columns={
            "model_name": "model_name",
            "dependencies": "ds_exposure_dependencies",
            "count_dependencies": "ds_exposure_dependencies_count",
        }
    )

    # merge the two results on model name to get a single output
    models_with_dependencies_and_exposures = pd.merge(
        models_with_dependencies_df, models_with_exposures_df, on="model_name"
    )

    df_to_csv(
        file_name="models_with_dependencies.csv",
        df=models_with_dependencies_and_exposures,
    )


def run():
    """
    Run the dependency check
    """
    file_name = get_file_name()

    model_names = get_model_names(file_name=file_name)
    generate_results(model_names=model_names)


if __name__ == "__main__":
    run()
