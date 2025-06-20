from prettytable import PrettyTable
from collections import defaultdict
from collections import Counter
import re
import os
import textwrap


def render_percentage_bar(successful, failed):
    bar_length = 50
    total = successful + failed
    if total == 0:
        return f"[{'=' * bar_length}] (No Tests Run)"

    success_ratio = successful / total
    failure_ratio = failed / total

    success_length = int(bar_length * success_ratio)
    failure_length = int(bar_length * failure_ratio)

    success_bar = f"\033[92m{'=' * success_length}\033[0m"
    failure_bar = f"\033[91m{'=' * failure_length}\033[0m"

    return f"[{success_bar}{failure_bar}]"

def display_page_results(all_results, total_tests, total_successful, total_failed):
    table = PrettyTable()
    table.field_names = ["Category", "Page name", "Total Tests", "Successful Tests", "Failed Tests", "Coverage",
                         "Success rate %"]

    for result in all_results:
        successful_color = f"\033[92m{result['successful_tests']}\033[0m"
        failed_color = f"\033[91m{result['failed_tests']}\033[0m"
        percentage_color = f"\033[92m{result['success_percentage']:.2f}%\033[0m" if result[
                                                                                        "success_percentage"] >= 50 else f"\033[91m{result['success_percentage']:.2f}%\033[0m"
        # Truncate category and page names if too long
        max_length = 20
        display_category = result['category'] if len(result['category']) <= max_length else result['category'][:max_length-3] + "..."
        display_page_name = result['page_name'] if len(result['page_name']) <= max_length else result['page_name'][:max_length-3] + "..."

        table.add_row([
            display_category,
            display_page_name,
            result["total_tests"],
            successful_color,
            failed_color,
            render_percentage_bar(result["successful_tests"], result["failed_tests"]),
            percentage_color
        ])

    total_bar = render_percentage_bar(total_successful, total_failed)
    total_success_color = f"\033[92m{total_successful}\033[0m"
    total_fail_color = f"\033[91m{total_failed}\033[0m"
    total_percentage_color = f"\033[92m{(total_successful / total_tests) * 100:.2f}%\033[0m" if total_successful / total_tests >= 0.5 else f"\033[91m{(total_successful / total_tests) * 100:.2f}%\033[0m"

    table.add_row(
        ["TOTAL", "", total_tests, total_success_color, total_fail_color, total_bar, total_percentage_color])
    print("\nPages and Categories Test Results:\n")
    print(table)

def display_category_results(all_results):
    table = PrettyTable()
    table.field_names = ["Category", "Total Tests", "Successful Tests", "Failed Tests", "Coverage",
                         "Success rate %"]

    category_totals = defaultdict(lambda: {"total_tests": 0, "successful_tests": 0, "failed_tests": 0})

    for result in all_results:
        category = result['category']
        category_totals[category]["total_tests"] += result["total_tests"]
        category_totals[category]["successful_tests"] += result["successful_tests"]
        category_totals[category]["failed_tests"] += result["failed_tests"]

    for category, totals in category_totals.items():
        success_percentage = (totals["successful_tests"] / totals["total_tests"]) * 100 if totals[
                                                                                               "total_tests"] > 0 else 0
        coverage = render_percentage_bar(totals["successful_tests"], totals["failed_tests"])

        success_color = f"\033[92m{totals['successful_tests']}\033[0m"
        fail_color = f"\033[91m{totals['failed_tests']}\033[0m"
        percentage_color = f"\033[92m{success_percentage:.2f}%\033[0m" if success_percentage >= 50 else f"\033[91m{success_percentage:.2f}%\033[0m"

        # Truncate category name if too long
        max_length = 20
        display_category = category if len(category) <= max_length else category[:max_length-3] + "..."

        table.add_row([
            display_category,
            totals["total_tests"],
            success_color,
            fail_color,
            coverage,
            percentage_color
        ])

    print("\nCategory-wise Test Results:\n")
    print(table)


def display_top_errors(error_log_file):
    """
    Parse the error log file and display the top 10 most frequent error messages.
    """

    # Check if the error log file exists
    if not os.path.exists(error_log_file):
        print(f"\nNo error log file found at {error_log_file}")
        return

    # Read the error log file
    with open(error_log_file, "r") as file:
        content = file.read()

    # Split the content by test blocks (delimited by ================)
    test_blocks = re.split(r'={80,}', content)

    # Extract error messages
    error_messages = []

    # Pattern to match error type lines (used to identify error blocks)
    error_type_pattern = re.compile(r'(Query unexpectedly failed!|Query failed with unexpected error!|'
                                    r'Query did not fail, but expected error!|Wrong column count in query!|'
                                    r'Wrong row count in query!|Error in test!|Wrong result in query!|'
                                    r'Wrong result hash!)')

    # Pattern to match actual error messages (DataFusion errors starting with digits)
    datafusion_error_pattern = re.compile(r'(\d{6}:.*?)(?=\n|$)')

    # Pattern to match mismatch errors
    mismatch_error_pattern = re.compile(r'(Mismatch.*?)(?=\n|$)')

    for i in range(len(test_blocks)):
        block = test_blocks[i]
        if not block.strip():
            continue

        # Look for error type to identify error blocks
        error_type_match = error_type_pattern.search(block)
        if error_type_match:
            error_type = error_type_match.group(1)

            # Look for different types of error messages
            message = ""

            # For DataFusion errors
            if "Query unexpectedly failed!" in error_type or "Query failed with unexpected error!" in error_type:
                msg_match = datafusion_error_pattern.search(block)
                if msg_match:
                    message = msg_match.group(1).strip()
                elif i + 1 < len(test_blocks) and datafusion_error_pattern.search(test_blocks[i + 1]):
                    msg_match = datafusion_error_pattern.search(test_blocks[i + 1])
                    message = msg_match.group(1).strip()

            # For result mismatch errors
            elif "Wrong result in query!" in error_type or "Wrong column count" in error_type or "Wrong row count" in error_type:
                msg_match = mismatch_error_pattern.search(block)
                if msg_match:
                    message = f"{error_type}: {msg_match.group(1).strip()}"
                else:
                    message = error_type

            # For other error types
            else:
                message = error_type

            if message:
                error_messages.append(message)

    # Count occurrences
    error_message_counts = Counter(error_messages)

    # Display top error messages
    print(f"\nTop 10 Errors:")
    print("-" * 150)

    if not error_messages:
        print("No detailed error messages found.")
        print("-" * 150)
        return

    # Display the top N errors with their counts
    for i, (error_msg, count) in enumerate(error_message_counts.most_common(10), 1):
        print(f"{i}. ERROR (occurs {count} times):")
        print("-" * 150)

        # Print full error message without truncation
        # Use textwrap to handle potential line wrapping
        wrapped_lines = textwrap.wrap(error_msg, width=150, replace_whitespace=False,
                                      break_long_words=False, break_on_hyphens=False)
        if not wrapped_lines:  # Handle empty strings
            print("<empty error message>")
        else:
            for line in wrapped_lines:
                print(line)

        print("-" * 150)
