import os
import sys
import json
import argparse
import pandas as pd


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
    parser = argparse.ArgumentParser(description='Run selected SLT tests')
    parser.add_argument('--selection-file', type=str, default='./artifacts/selected_slts.json',
                        help='JSON file containing selected SLT files')
    parser.add_argument('--output-dir', type=str, default='./artifacts',
                        help='Output directory for test results')

    return parser.parse_args()


def main():
    """
    Main entry point for the script
    """
    args = parse_args()

    # Load the selected SLT files
    if not os.path.exists(args.selection_file):
        print(f"Selection file not found: {args.selection_file}")
        return 1

    with open(args.selection_file, 'r') as f:
        selected_slts = json.load(f)

    if not selected_slts:
        print("No SLT files selected for testing")
        return 0

    print(f"Running {len(selected_slts)} selected SLT files...")

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
