import os
import sys
import json
import argparse
import re
import pandas as pd
import numpy as np
import subprocess
from openai import OpenAI


def import_slt_runner():
    """
    Import the SLT runner module from the slt_runner directory
    This is needed because the module is not installed in the Python path
    """
    # Get the absolute path to the test directory
    test_dir = os.path.abspath(os.path.dirname(__file__))

    # Add the slt_runner directory to the Python path
    slt_runner_dir = os.path.join(test_dir, "slt_runner")
    sys.path.insert(0, slt_runner_dir)

    # Import the SLT runner module
    from python_runner import SQLLogicPythonRunner

    return sys.modules['python_runner']




def get_changed_files(base_branch="main"):
    """
    Get the list of files changed in the current PR using git merge-base
    Optimized for GitHub Actions environment
    """
    try:
        # Get the base and head refs from GitHub Actions environment
        if os.environ.get('GITHUB_BASE_REF'):
            base_ref = os.environ.get('GITHUB_BASE_REF')
            print(f"GitHub Actions environment detected. Using base ref: {base_ref}")

            # Make sure the base ref is available
            subprocess.run(["git", "fetch", "origin", base_ref], check=True, capture_output=True)

            # Find the merge-base (common ancestor) between the base branch and the current HEAD
            merge_base_cmd = ["git", "merge-base", f"origin/{base_ref}", "HEAD"]
            merge_base = subprocess.run(merge_base_cmd, capture_output=True, text=True, check=True).stdout.strip()
            print(f"Found merge-base: {merge_base}")

            # Get changes between the merge-base and HEAD
            cmd = ["git", "diff", "--name-only", merge_base, "HEAD"]
            print(f"Running: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            changed_files = [f for f in result.stdout.strip().split("\n") if f]

            print(f"Found {len(changed_files)} changed files")
            file_contents = {}
            for file in changed_files:
                try:
                    if os.path.exists(file):
                        with open(file, 'r') as f:
                            file_contents[file] = f.read()
                    else:
                        print(f"File {file} doesn't exist (might have been deleted)")
                        file_contents[file] = "File no longer exists"
                except Exception as e:
                    print(f"Failed to read file {file}: {e}")
                    file_contents[file] = f"Failed to read file: {e}"
            return file_contents
        else:
            # Fallback to using the provided base_branch if not in GitHub Actions
            print(f"Not in GitHub Actions environment, using base branch: {base_branch}")
            subprocess.run(["git", "fetch", "origin", base_branch], check=True, capture_output=True)
            cmd = ["git", "diff", "--name-only", f"origin/{base_branch}", "HEAD"]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            changed_files = [f for f in result.stdout.strip().split("\n") if f]

            file_contents = {}
            for file in changed_files:
                try:
                    if os.path.exists(file):
                        with open(file, 'r') as f:
                            file_contents[file] = f.read()
                    else:
                        print(f"File {file} doesn't exist (might have been deleted)")
                        file_contents[file] = "File no longer exists"
                except Exception as e:
                    print(f"Failed to read file {file}: {e}")
                    file_contents[file] = f"Failed to read file: {e}"
            return file_contents
    except Exception as e:
        print(f"Error in get_changed_files: {e}")
        return {}


def get_all_slt_files(slt_dir="test/sql"):
    """
    Get all SLT files in the test directory
    """
    # Find all SLT files
    slt_files = {}
    for root, dirs, files in os.walk(slt_dir):
        for file in files:
            if file.endswith(".slt"):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r') as f:
                        content = f.read()
                    slt_files[file_path] = content
                except Exception as e:
                    print(f"Failed to read file {file_path}: {e}")

    return slt_files


def select_relevant_slts(changed_files, all_slts, model="gpt-4-turbo"):
    """Use OpenAI to select relevant SLT files based on code changes"""
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    # Extract meaningful paths from SLT file paths
    # This will convert paths like "test/sql/function/aggregate/test.slt" to "function/aggregate/test.slt"
    meaningful_paths = []
    slt_path_mapping = {}  # Keep a mapping to convert back to full paths

    for slt_path in all_slts.keys():
        if "/sql/" in slt_path:
            # Extract the part after /sql/
            path_after_sql = slt_path.split("/sql/", 1)[1]
            meaningful_paths.append(path_after_sql)
            slt_path_mapping[path_after_sql] = slt_path
        else:
            # If no /sql/ in the path, just use the filename
            filename = os.path.basename(slt_path)
            meaningful_paths.append(filename)
            slt_path_mapping[filename] = slt_path

    # Prepare message for OpenAI
    prompt = f"""
    I have made code changes to the following files in a Pull Request:

    ```
    {json.dumps(changed_files, indent=2)}
    ```

    I need to select the most relevant SQL Logic Test (SLT) files to run based on these changes.
    Here are all the available SLT files, with paths indicating their functionality:

    ```
    {json.dumps(meaningful_paths, indent=2)}
    ```

    The paths shown are relative to the /sql/ directory and indicate what functionality the test is for. For example:
    - "function/aggregate/test.slt" tests SQL aggregate functions
    - "type/numeric/test.slt" tests numeric type functionality

    Please select the SLT files that test the functionality affected by my code changes.
    Don't include any files where you don't see SPECIFIC evidence that the changes are relevant.
    IMPORTANT: if code changed are not related to database engine - don't output any tests.
    IMPORTANT: don't include files just because they are "basic" tests. Be very specific with the output.
    Return ONLY a JSON array with these relative paths, no explanations or other text. For example:
    ["function/aggregate/test.slt", "type/numeric/test.slt"]
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
        selected_relative_paths = json.loads(response_text)

        # Convert the relative paths back to full paths
        selected_slts = []
        for rel_path in selected_relative_paths:
            if rel_path in slt_path_mapping:
                selected_slts.append(slt_path_mapping[rel_path])
            else:
                # Try to find a matching path
                for path_key in slt_path_mapping:
                    if path_key.endswith(rel_path):
                        selected_slts.append(slt_path_mapping[path_key])
                        break

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

        # Check if symlink exists and remove it before creating a new one
        if os.path.exists(target_path) or os.path.islink(target_path):
            os.unlink(target_path)

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


def analyze_test_results(results_csv):
    """
    Analyze the test results and return a summary
    """
    # Load the test results
    df = pd.read_csv(results_csv)

    # Count total tests
    total_tests = len(df)

    # Count passed tests
    passed_tests = len(df[df['status'] == 'ok'])

    # Count failed tests
    failed_tests = len(df[df['status'] == 'not ok'])

    # Count skipped tests
    skipped_tests = len(df[df['status'] == 'skip'])

    # Create a summary
    summary = {
        'total_tests': total_tests,
        'passed_tests': passed_tests,
        'failed_tests': failed_tests,
        'skipped_tests': skipped_tests,
        'pass_rate': passed_tests / total_tests if total_tests > 0 else 0,
    }

    # Create a list of failed tests
    failed_test_details = []
    for _, row in df[df['status'] == 'not ok'].iterrows():
        failed_test_details.append({
            'statement': row['statement'],
            'expected': row['expected'],
            'actual': row['actual'],
            'filename': row['filename'],
            'line': row['line'],
        })

    summary['failed_test_details'] = failed_test_details

    return summary


def generate_pr_comment(summary, test_file_list):
    """
    Generate a comment for the PR with the test results
    """
    # Read the list of test files
    with open(test_file_list, 'r') as f:
        files = f.read().strip().split('\n')

    # Calculate pass rate percentage
    pass_rate_pct = summary['pass_rate'] * 100

    # Create a status emoji
    if summary['failed_tests'] == 0:
        status = "✅"
    else:
        status = "❌"

    # Create the comment
    comment = f"""
## SLT Test Results {status}

### Summary
- **Total Tests:** {summary['total_tests']}
- **Passed:** {summary['passed_tests']}
- **Failed:** {summary['failed_tests']}
- **Skipped:** {summary['skipped_tests']}
- **Pass Rate:** {pass_rate_pct:.2f}%

### Test Files
The following {len(files)} test files were executed:
{os.linesep.join(files)}

"""

    # Add failed test details if there are any
    if summary['failed_tests'] > 0:
        comment += """
### Failed Tests
<details>
<summary>Click to expand failed test details</summary>

"""

        for i, test in enumerate(summary['failed_test_details']):
            comment += f"""
#### Failed Test {i + 1}
- **File:** {test['filename']}
- **Line:** {test['line']}
- **Statement:**
```sql
{test['statement']}
```
- **Expected:**
{test['expected']}
- **Actual:**
{test['actual']}

"""

        comment += "</details>"

    return comment


def parse_args():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(description='Run SLT tests based on code changes')
    parser.add_argument('--base-branch', type=str, default='main',
                        help='Base branch to compare against')
    parser.add_argument('--output-dir', type=str, default='./artifacts',
                        help='Output directory for test results')
    parser.add_argument('--slt-dir', type=str, default='test/sql',
                        help='Directory containing SLT files')
    parser.add_argument('--model', type=str, default='gpt-4-turbo',
                        help='OpenAI model to use for selecting tests')
    parser.add_argument('--run-all', action='store_true',
                        help='Run all SLT files instead of selecting relevant ones')

    return parser.parse_args()


def main():
    """
    Main entry point for the script
    """
    args = parse_args()

    # Get the changed files
    changed_files = get_changed_files(args.base_branch)

    # If there are no changed files, exit
    if not changed_files:
        print("No changed files found")
        return 0

    # Get all SLT files
    all_slts = get_all_slt_files(args.slt_dir)

    # If there are no SLT files, exit
    if not all_slts:
        print("No SLT files found")
        return 0

    # Select relevant SLTs
    if args.run_all:
        print("Running all SLT files")
        selected_slts = list(all_slts.keys())
    else:
        selected_slts = select_relevant_slts(changed_files, all_slts, args.model)

    # Run the selected SLTs
    runner_module = import_slt_runner()
    results_csv = run_slt_files(selected_slts, runner_module, args.output_dir)

    # Analyze the test results
    summary = analyze_test_results(results_csv)

    # Generate a PR comment
    test_file_list = os.path.join(args.output_dir, "test_file_list.txt")
    comment = generate_pr_comment(summary, test_file_list)

    # Save the comment to a file
    comment_file = os.path.join(args.output_dir, "pr_comment.md")
    with open(comment_file, 'w') as f:
        f.write(comment)

    print(f"Test results saved to {results_csv}")
    print(f"PR comment saved to {comment_file}")

    # Return exit code based on test results
    return 1 if summary['failed_tests'] > 0 else 0


if __name__ == "__main__":
    sys.exit(main())