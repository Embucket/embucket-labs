#!/usr/bin/env python3
"""
SLT Targeted Runner - Selects and runs relevant SQL Logic Tests based on code changes in a PR.
Uses the existing SLT runner infrastructure to execute tests and process results.
"""

import os
import sys
import json
import subprocess
import glob
import argparse
import pandas as pd
import numpy as np
from openai import OpenAI
from pathlib import Path
import importlib.util


def import_slt_runner(runner_path="test/slt_runner/python_runner.py"):
    """
    Import the SLT runner module dynamically
    """
    spec = importlib.util.spec_from_file_location("python_runner", runner_path)
    module = importlib.util.module_from_spec(spec)  # This was the line with the error
    spec.loader.exec_module(module)
    return module


def get_changed_files(base_ref='main'):
    """Get files changed in the PR"""
    cmd = f"git diff --name-only origin/{base_ref}...HEAD"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    changed_files = result.stdout.strip().split('\n')

    # Read each changed file and get its content
    changed_file_contents = {}
    for file_path in changed_files:
        if not os.path.exists(file_path) or os.path.isdir(file_path):
            continue

        try:
            with open(file_path, 'r') as f:
                content = f.read()
                changed_file_contents[file_path] = content
        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    return changed_file_contents


def get_all_slt_files(slt_dir="test/sql"):
    """Get all SLT files in the project"""
    slt_files = glob.glob(f"{slt_dir}/**/*.slt", recursive=True)

    # Read SLT file contents
    slt_file_contents = {}
    for file_path in slt_files:
        try:
            with open(file_path, 'r') as f:
                content = f.read()
                slt_file_contents[file_path] = content
        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    return slt_file_contents


def select_relevant_slts(changed_files, all_slts, model="gpt-4-turbo"):
    """Use OpenAI to select relevant SLT files based on code changes"""
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    # Prepare message for OpenAI
    prompt = f"""
    I have made code changes to the following files in a Pull Request:

    ```
    {json.dumps(changed_files, indent=2)}
    ```

    I need to select the most relevant SQL Logic Test (SLT) files to run based on these changes.
    Here are all the available SLT files:

    ```
    {json.dumps({k: v[:1000] + ("..." if len(v) > 1000 else "") for k, v in all_slts.items()}, indent=2)}
    ```

    Please select the SLT files that are definitely needed to test the functionality affected by my code changes.
    Return a JSON array with the filenames. For example:
    ["test/sql/file1.slt", "test/sql/file2.slt"]
    Also return a text, explaining why you picked these files.
    """

    print(prompt)

    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=2000
    )

    # Extract JSON from response
    response_text = response.choices[0].message.content.strip()

    print(response_text)

    # Try to extract JSON array if it's not already a valid JSON
    if not response_text.startswith('['):
        import re
        json_match = re.search(r'\[(.*)\]', response_text, re.DOTALL)
        if json_match:
            response_text = json_match.group(0)

    try:
        selected_slts = json.loads(response_text)

        # Validate that all selected files exist
        valid_slts = [slt for slt in selected_slts if slt in all_slts]

        if not valid_slts:
            print("No valid SLTs selected, using all SLTs")
            return list(all_slts.keys())

        return valid_slts
    except json.JSONDecodeError:
        print(f"Failed to parse OpenAI response as JSON: {response_text}")
        return list(all_slts.keys())


def run_slt_files(slt_files, runner_module=None, output_dir="./artifacts"):
    """
    Run the selected SLT files using the SLT runner module
    Returns path to the CSV results file
    """
    # If no runner module is provided, import it
    if runner_module is None:
        runner_module = import_slt_runner()

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Save the list of files to run as a text file in the output directory
    test_file_list = os.path.join(output_dir, "test_file_list.txt")
    with open(test_file_list, 'w') as f:
        for slt_file in slt_files:
            f.write(f"{slt_file}\n")

    # Create a custom test directory that only contains symlinks to the target files
    test_dir = os.path.join(output_dir, "targeted_tests")
    os.makedirs(test_dir, exist_ok=True)

    # Clear the directory first (in case it already exists)
    for file in os.listdir(test_dir):
        file_path = os.path.join(test_dir, file)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)

    # Create a temporary directory structure with the selected SLTs
    for slt_file in slt_files:
        # Extract the basename
        basename = os.path.basename(slt_file)
        # Create a symlink to the original file
        target_path = os.path.join(test_dir, basename)
        # Create a symlink
        os.symlink(os.path.abspath(slt_file), target_path)

    print(f"Running {len(slt_files)} SLT files...")

    # Create a SQLLogicPythonRunner instance
    runner = runner_module.SQLLogicPythonRunner()

    # Override sys.argv to use the test directory option
    original_argv = sys.argv
    sys.argv = [
        "python_runner.py",
        "--test-dir", test_dir,
    ]

    try:
        # Run the tests
        runner.run()
    finally:
        # Restore original argv
        sys.argv = original_argv

    # The runner will save results to a CSV file named 'slt_results.csv' in the output directory
    results_csv = os.path.join(output_dir, "slt_results.csv")

    return results_csv


def analyze_test_results(results_csv, output_dir="./artifacts"):
    """Analyze the test results and generate visualizations"""
    # Import the visualization functions from generate_test_assets.py
    try:
        spec = importlib.util.spec_from_file_location(
            "generate_test_assets",
            "test/generate_test_assets.py"
        )
        generate_test_assets = importlib.util.spec_from_module_spec(spec)
        spec.loader.exec_module(generate_test_assets)

        # Generate coverage badge and visualization
        generate_test_assets.generate_badge(results_csv, output_dir)
        generate_test_assets.generate_visualization(results_csv, output_dir)

        # Read the test results
        results_df = pd.read_csv(results_csv)

        # Calculate overall stats
        total_tests = len(results_df)
        passed_tests = results_df['result'].sum()
        coverage = results_df['coverage'].mean()

        return {
            "total_tests": total_tests,
            "passed_tests": passed_tests,
            "failed_tests": total_tests - passed_tests,
            "overall_coverage": coverage,
            "badge_path": os.path.join(output_dir, "badge.svg"),
            "visualization_path": os.path.join(output_dir, "coverage.png"),
            "results_csv": results_csv
        }
    except Exception as e:
        print(f"Error analyzing test results: {e}")
        return {
            "error": str(e),
            "results_csv": results_csv
        }


def generate_pr_comment(test_results, model="gpt-4-turbo"):
    """Generate PR comment based on test results"""
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    # Load the results dataframe if we have a CSV file
    results_df = None
    if "results_csv" in test_results and os.path.exists(test_results["results_csv"]):
        results_df = pd.read_csv(test_results["results_csv"])

    # Prepare message for OpenAI
    prompt = f"""
    I ran SQL Logic Tests (SLTs) for a PR and got these results:

    ```
    {json.dumps(test_results, indent=2)}
    ```

    {'Here is the detailed results dataframe:' if results_df is not None else ''}
    {results_df.to_string() if results_df is not None else ''}

    Please generate a GitHub PR comment that:

    1. If all tests passed, congratulate the developer and mention the overall code coverage.
    2. If any tests failed, explain which functionality might be affected based on the failing test file names.
    3. Include a summary of the test run (# tests run, passed, failed, average coverage).
    4. Be concise and professional.

    The workflow URL can be referenced as: ${{{{ github.server_url }}}}/\
    ${{{{ github.repository }}}}/actions/runs/${{{{ github.run_id }}}}

    If there are coverage visualizations, mention they're available in the workflow artifacts.
    """

    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7,
        max_tokens=1000
    )

    return response.choices[0].message.content.strip()


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Run targeted SLT tests based on code changes.')
    parser.add_argument('--base-ref', default='main', help='Base ref to compare against')
    parser.add_argument('--slt-dir', default='test/sql', help='Directory containing SLT files')
    parser.add_argument('--output-dir', default='./artifacts', help='Directory to save output files')
    parser.add_argument('--model', default='gpt-4-turbo', help='OpenAI model to use')
    parser.add_argument('--run-all', action='store_true', help='Run all SLTs without selection')
    parser.add_argument('--test-list', help='Path to a test list file (one test per line)')
    return parser.parse_args()


def main():
    """Main function"""
    args = parse_args()

    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)

    # Import the SLT runner module
    runner_module = import_slt_runner()

    # Determine which SLT files to run
    if args.test_list:
        # Use the provided test list
        with open(args.test_list, 'r') as f:
            selected_slts = [line.strip() for line in f if line.strip()]
    else:
        # Get changed files and SLT files
        changed_files = get_changed_files(args.base_ref)
        all_slts = get_all_slt_files(args.slt_dir)

        # Save to files for debugging
        with open(os.path.join(args.output_dir, 'changed_files.json'), 'w') as f:
            json.dump(changed_files, f, indent=2)

        with open(os.path.join(args.output_dir, 'all_slts.json'), 'w') as f:
            json.dump({k: v[:100] + "..." for k, v in all_slts.items()}, f, indent=2)

        # Select relevant SLTs
        if args.run_all:
            selected_slts = list(all_slts.keys())
        else:
            selected_slts = select_relevant_slts(changed_files, all_slts, args.model)

    # Save selected SLTs for debugging
    with open(os.path.join(args.output_dir, 'selected_slts.json'), 'w') as f:
        json.dump(selected_slts, f, indent=2)

    print(f"Selected {len(selected_slts)} SLT files to run")

    # Run selected SLTs
    results_csv = run_slt_files(selected_slts, runner_module, args.output_dir)

    # Analyze test results
    test_results = analyze_test_results(results_csv, args.output_dir)

    # Save test results
    with open(os.path.join(args.output_dir, 'test_results.json'), 'w') as f:
        json.dump({k: str(v) if not isinstance(v, (int, float, bool)) else v
                   for k, v in test_results.items()}, f, indent=2)

    # Generate PR comment
    pr_comment = generate_pr_comment(test_results, args.model)

    # Save PR comment for GitHub Actions
    with open(os.path.join(args.output_dir, 'pr_comment.md'), 'w') as f:
        f.write(pr_comment)

    # Determine exit code based on test results
    if "failed_tests" in test_results and test_results["failed_tests"] > 0:
        return 1
    else:
        return 0


if __name__ == "__main__":
    sys.exit(main())
