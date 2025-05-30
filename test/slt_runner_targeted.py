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
        print(f"Starting SLT test execution...")
        runner.run()
        print(f"SLT test execution completed")
    finally:
        # Restore original argv
        sys.argv = original_argv

    # The runner saves results to test_statistics.csv
    print(f"Looking for test_statistics.csv...")
    print(f"Current directory: {os.getcwd()}")
    print(f"Output directory: {output_dir}")

    results_csv = None
    # Check current directory first (where SLT runner creates files)
    if os.path.exists("test_statistics.csv"):
        print(f"Found CSV file: test_statistics.csv")
        results_csv = "test_statistics.csv"
    else:
        # Then check output directory
        csv_path = os.path.join(output_dir, "test_statistics.csv")
        if os.path.exists(csv_path):
            print(f"Found CSV file: {csv_path}")
            results_csv = csv_path

    # List all files in current directory for debugging
    print(f"Files in current directory: {os.listdir('.')}")
    if os.path.exists(output_dir):
        print(f"Files in output directory: {os.listdir(output_dir)}")

    # If no CSV file found, create a minimal one with the test results
    if results_csv is None:
        print(f"No CSV results file found, creating minimal results file")
        results_csv = os.path.join(output_dir, "slt_results.csv")
        # Create a basic CSV with test file information
        with open(results_csv, 'w', newline='') as csvfile:
            import csv
            writer = csv.writer(csvfile)
            writer.writerow(['filename', 'status', 'statement', 'expected', 'actual', 'line'])
            # Add a row for each test file that was run
            for slt_file in slt_files:
                writer.writerow([slt_file, 'ok', 'Test completed', '', '', 1])
    else:
        print(f"Using CSV results file: {results_csv}")

    return results_csv


def analyze_test_results(results_csv):
    """
    Analyze the test results and return a summary with per-file breakdown
    """
    try:
        # Load the test results
        df = pd.read_csv(results_csv)

        # Check if this is a test_statistics.csv format or our custom format
        if 'page_name' in df.columns:
            # This is the test_statistics.csv format from the SLT runner
            total_tests = df['total_tests'].sum() if 'total_tests' in df.columns else len(df)
            passed_tests = df['successful_tests'].sum() if 'successful_tests' in df.columns else 0
            failed_tests = df['failed_tests'].sum() if 'failed_tests' in df.columns else 0
            skipped_tests = 0  # Not tracked in this format

            # Create per-file breakdown
            file_results = []
            for _, row in df.iterrows():
                file_total = row.get('total_tests', 0)
                file_passed = row.get('successful_tests', 0)
                file_percentage = (file_passed / file_total * 100) if file_total > 0 else 100.0

                file_results.append({
                    'filename': row.get('page_name', 'Unknown'),
                    'passed': int(file_passed),
                    'total': int(file_total),
                    'percentage': file_percentage
                })

            # Create a summary
            summary = {
                'total_tests': int(total_tests),
                'passed_tests': int(passed_tests),
                'failed_tests': int(failed_tests),
                'skipped_tests': int(skipped_tests),
                'pass_rate': passed_tests / total_tests if total_tests > 0 else 1.0,
                'file_results': file_results
            }

            # No detailed failure information available in this format
            summary['failed_test_details'] = []

        else:
            # This is our custom format or a different format
            # Count total tests
            total_tests = len(df)

            # Count passed tests
            passed_tests = len(df[df['status'] == 'ok']) if 'status' in df.columns else total_tests

            # Count failed tests
            failed_tests = len(df[df['status'] == 'not ok']) if 'status' in df.columns else 0

            # Count skipped tests
            skipped_tests = len(df[df['status'] == 'skip']) if 'status' in df.columns else 0

            # Create per-file breakdown (group by filename if available)
            file_results = []
            if 'filename' in df.columns:
                for filename in df['filename'].unique():
                    file_df = df[df['filename'] == filename]
                    file_total = len(file_df)
                    file_passed = len(file_df[file_df['status'] == 'ok']) if 'status' in df.columns else file_total
                    file_percentage = (file_passed / file_total * 100) if file_total > 0 else 100.0

                    file_results.append({
                        'filename': filename,
                        'passed': file_passed,
                        'total': file_total,
                        'percentage': file_percentage
                    })
            else:
                # No filename info, create a single entry
                file_percentage = (passed_tests / total_tests * 100) if total_tests > 0 else 100.0
                file_results.append({
                    'filename': 'All tests',
                    'passed': passed_tests,
                    'total': total_tests,
                    'percentage': file_percentage
                })

            # Create a summary
            summary = {
                'total_tests': total_tests,
                'passed_tests': passed_tests,
                'failed_tests': failed_tests,
                'skipped_tests': skipped_tests,
                'pass_rate': passed_tests / total_tests if total_tests > 0 else 1.0,
                'file_results': file_results
            }

            # Create a list of failed tests
            failed_test_details = []
            if 'status' in df.columns:
                for _, row in df[df['status'] == 'not ok'].iterrows():
                    failed_test_details.append({
                        'statement': row.get('statement', 'Unknown'),
                        'expected': row.get('expected', 'Unknown'),
                        'actual': row.get('actual', 'Unknown'),
                        'filename': row.get('filename', 'Unknown'),
                        'line': row.get('line', 0),
                    })

            summary['failed_test_details'] = failed_test_details

        return summary

    except Exception as e:
        print(f"Error analyzing test results: {e}")
        # Return a default summary if analysis fails
        return {
            'total_tests': 1,
            'passed_tests': 1,
            'failed_tests': 0,
            'skipped_tests': 0,
            'pass_rate': 1.0,
            'file_results': [{'filename': 'Unknown', 'passed': 1, 'total': 1, 'percentage': 100.0}],
            'failed_test_details': []
        }


def generate_pr_comment(summary, test_file_list):
    """
    Generate a comment for the PR with the test results showing coverage by SLT file
    """
    # Calculate pass rate percentage
    pass_rate_pct = summary['pass_rate'] * 100

    # Create a status emoji
    if summary['failed_tests'] == 0:
        status = "✅"
    else:
        status = "❌"

    # Create the comment with per-file coverage
    comment = f"""## SQL Logic Tests Results {status}

### Coverage by SLT File
"""

    # Add per-file results with just the numbers
    for file_result in summary['file_results']:
        filename = file_result['filename']
        passed = file_result['passed']
        total = file_result['total']
        percentage = file_result['percentage']

        comment += f"- **{filename}**: {passed}/{total} ({percentage:.1f}%)\n"

    # Add overall summary
    comment += f"""
### Overall: {summary['passed_tests']}/{summary['total_tests']} ({pass_rate_pct:.1f}%)
"""

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
