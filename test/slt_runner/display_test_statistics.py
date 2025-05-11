from prettytable import PrettyTable
from collections import defaultdict


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

        table.add_row([
            result['category'],
            result['page_name'],
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

        table.add_row([
            category,
            totals["total_tests"],
            success_color,
            fail_color,
            coverage,
            percentage_color
        ])

    print("\nCategory-wise Test Results:\n")
    print(table)