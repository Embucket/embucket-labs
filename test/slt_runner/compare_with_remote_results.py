import os
import csv
from prettytable import PrettyTable

RED = "\033[91m"
RESET = "\033[0m"
GREEN = "\033[92m"


def compare_with_remote_results():
    """
    Compare local SLT runner test results with remote results from the main branch.

    This function:
    1. Retrieves test statistics from the remote 'assets' branch
    2. Compares local test results with remote results
    3. Identifies improvements and regressions in both coverage and success rates
    4. Displays summary tables highlighting changes in test coverage and success rates

    The comparison handles both old and new CSV formats:
    - Old format: Uses 'success_percentage' for both coverage and success rate
    - New format: Uses 'coverage_percentage' (includes Not Implemented tests) and
                  'success_rate_percentage' (excludes Not Implemented tests)

    Remote results are stored in the 'assets' branch under 'assets/test_statistics.csv'.
    """
    try:
        print("\nComparing with main branch results...")

        # Create a temporary file to store the main branch version of test statistics
        temp_file = "main_test_statistics.csv"

        # Retrieve test statistics from the 'assets' branch using git show
        import subprocess
        result = subprocess.run(
            ["git", "show", "origin/assets:assets/test_statistics.csv"],
            stdout=open(temp_file, "w"),
            stderr=subprocess.PIPE,
            text=True
        )

        # Verify successful retrieval of remote test statistics
        if result.returncode != 0 or not os.path.exists(temp_file) or os.stat(temp_file).st_size == 0:
            print(f"Could not retrieve test_statistics.csv from main branch: {result.stderr}")
            return

        # Load both local and remote results into dictionaries for comparison
        local_results = {}
        main_results = {}

        # Parse local test results from current test run
        with open("../test_statistics.csv", "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = f"{row['page_name']}_{row['category']}"
                local_results[key] = {
                    'page_name': row['page_name'],
                    'category': row['category'],
                    'total_tests': int(float(row['total_tests'])),
                    'successful_tests': int(float(row['successful_tests'])),
                    'failed_tests': int(float(row['failed_tests'])),
                    'not_implemented_tests': int(float(row.get('not_implemented_tests', 0))),
                    'coverage_percentage': float(row.get('coverage_percentage', row.get('success_percentage', 0))),
                    'success_rate_percentage': float(row.get('success_rate_percentage', row.get('success_percentage', 0)))
                }

        # Parse remote test results from 'assets' branch
        with open(temp_file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = f"{row['page_name']}_{row['category']}"
                main_results[key] = {
                    'page_name': row['page_name'],
                    'category': row['category'],
                    'total_tests': int(float(row['total_tests'])),
                    'successful_tests': int(float(row['successful_tests'])),
                    'failed_tests': int(float(row['failed_tests'])),
                    'not_implemented_tests': int(float(row.get('not_implemented_tests', 0))),
                    'coverage_percentage': float(row.get('coverage_percentage', row.get('success_percentage', 0))),
                    'success_rate_percentage': float(row.get('success_rate_percentage', row.get('success_percentage', 0)))
                }

        # Find tests that exist in both local and remote results
        common_tests = set(local_results.keys()) & set(main_results.keys())

        improved_tests = []  # Tests with better success rate in local version
        new_failures = []  # Tests that were passing in remote but failing locally

        # Compare each test's coverage and success rates between local and remote versions
        for key in common_tests:
            local = local_results[key]
            main = main_results[key]

            # Use coverage percentage for primary comparison (includes all tests)
            local_coverage = local['coverage_percentage']
            main_coverage = main['coverage_percentage']

            # Also track success rate percentage (excludes not implemented)
            local_success_rate = local['success_rate_percentage']
            main_success_rate = main['success_rate_percentage']

            if local_coverage > main_coverage:
                # Current version has better coverage than remote
                improved_tests.append({
                    'page_name': local['page_name'],
                    'category': local['category'],
                    'before_coverage': main_coverage,
                    'after_coverage': local_coverage,
                    'before_success_rate': main_success_rate,
                    'after_success_rate': local_success_rate,
                    'coverage_improvement': local_coverage - main_coverage,
                    'success_rate_improvement': local_success_rate - main_success_rate
                })
            elif local_coverage < main_coverage:
                # Current version has worse coverage than remote
                if main['successful_tests'] > local['successful_tests']:
                    newly_failed_count = main['successful_tests'] - local['successful_tests']
                    new_failures.append({
                        'page_name': local['page_name'],
                        'category': local['category'],
                        'failed_count': newly_failed_count,
                        'before_success': main['successful_tests'],
                        'after_success': local['successful_tests'],
                        'before_coverage': main_coverage,
                        'after_coverage': local_coverage,
                        'before_success_rate': main_success_rate,
                        'after_success_rate': local_success_rate
                    })

        # Display summary of comparison results
        print(f"\nComparison Results:")
        print(f"- {len(improved_tests)} improved tests")
        print(f"- {len(new_failures)} regression")

        # Display detailed table of improved tests
        if improved_tests:
            print(f"\n{GREEN}Increase in coverage:{RESET}")
            improved_table = PrettyTable()
            improved_table.field_names = ["Test", "Category", "Coverage Before (%)", "Coverage After (%)",
                                        "Success Rate Before (%)", "Success Rate After (%)", "Coverage Improvement (%)"]
            for test in sorted(improved_tests, key=lambda x: x['coverage_improvement'], reverse=True):
                improved_table.add_row([
                    test['page_name'],
                    test['category'],
                    f"{test['before_coverage']:.2f}",
                    f"{test['after_coverage']:.2f}",
                    f"{test['before_success_rate']:.2f}",
                    f"{test['after_success_rate']:.2f}",
                    f"{test['coverage_improvement']:.2f}"
                ])
            print(improved_table)

        # Display detailed table of tests with regressions
        if new_failures:
            print(f"\n{RED}Regression:{RESET}")
            failures_table = PrettyTable()
            failures_table.field_names = ["Test", "Category", "Newly Failed", "Before Success", "After Success",
                                         "Coverage Before (%)", "Coverage After (%)", "Success Rate Before (%)", "Success Rate After (%)"]
            for test in sorted(new_failures, key=lambda x: x['failed_count'], reverse=True):
                failures_table.add_row([
                    test['page_name'],
                    test['category'],
                    test['failed_count'],
                    test['before_success'],
                    test['after_success'],
                    f"{test['before_coverage']:.2f}",
                    f"{test['after_coverage']:.2f}",
                    f"{test['before_success_rate']:.2f}",
                    f"{test['after_success_rate']:.2f}"
                ])
            print(failures_table)

        # Clean up temporary file
        os.remove(temp_file)

    except Exception as e:
        print(f"Error comparing with main branch: {e}")

if __name__ == "__main__":
    compare_with_remote_results()