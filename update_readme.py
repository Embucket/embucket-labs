import re

def strip_ansi_codes(text):
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

def extract_category_summary(text):
    """
    Extracts the 'Category-wise Test Results' table, keeps only 'Category' and 'Success rate %'
    columns, renames 'Success rate %' to 'Coverage', and uses percentage values.
    """
    match = re.search(
        r"Category-wise Test Results:[\s\n]*"
        r"\+[-+]+\+\s*\n"
        r"((?:.*\n)+)"  # Greedy capture for all table lines
        r"\+[-+]+\+",
        text,
        re.MULTILINE
    )

    if not match:
        return "Error: Could not find category summary table."

    raw_table_content = match.group(1).strip()
    raw_lines = raw_table_content.splitlines()

    if len(raw_lines) < 2:
        return "Error: Category summary table content doesn't have enough lines (expected header + ASCII separator line)."

    header_line_ascii = raw_lines[0]

    if not (header_line_ascii.startswith("|") and header_line_ascii.endswith("|")):
        return "Error: Table header line not in expected format (should start and end with '|')."

    original_header_cells = [strip_ansi_codes(cell.strip()) for cell in header_line_ascii.strip().split('|')[1:-1]]

    category_header_idx = -1
    success_rate_header_idx = -1

    try:
        category_header_idx = original_header_cells.index("Category")
        success_rate_header_idx = original_header_cells.index("Success rate %")
    except ValueError:
        return "Error: Could not find 'Category' or 'Success rate %' columns in the header after stripping ANSI codes."

    # Define the new Markdown header
    new_md_header_cells = ["Category", "Coverage"]
    md_header = "| " + " | ".join(new_md_header_cells) + " |"
    num_cols = len(new_md_header_cells)
    md_separator = "| " + " | ".join(["---"] * num_cols) + " |"

    md_data_rows = []
    # Process data rows, skipping the ASCII table separator line (raw_lines[1])
    for ascii_data_line in raw_lines[2:]:
        if ascii_data_line.startswith("|") and ascii_data_line.endswith("|"):
            original_data_cells_raw = [cell.strip() for cell in ascii_data_line.strip().split('|')[1:-1]]
            original_data_cells = [strip_ansi_codes(cell) for cell in original_data_cells_raw]


            if len(original_data_cells) != len(original_header_cells):
                # Skip malformed rows or rows that don't match the header structure
                continue

            # Ensure indices are valid for the current row's original_data_cells
            if not (category_header_idx < len(original_data_cells) and
                    success_rate_header_idx < len(original_data_cells)):
                continue # Skip if essential columns are missing in a data row

            category_value = original_data_cells[category_header_idx]
            percentage_value = original_data_cells[success_rate_header_idx]

            new_md_data_cells = [category_value, percentage_value]
            md_data_rows.append("| " + " | ".join(new_md_data_cells) + " |")

    if not md_data_rows:
        return "Error: No data rows could be processed for the category summary."

    md_table_lines = [md_header, md_separator] + md_data_rows
    return "\n".join(md_table_lines)


def update_readme(markdown_table_content):
    with open("README.md", "r") as file:
        content = file.read()

    new_content = re.sub(
        r"<!-- TEST_RESULTS_START -->.*?<!-- TEST_RESULTS_END -->",
        f"<!-- TEST_RESULTS_START -->\n{markdown_table_content}\n<!-- TEST_RESULTS_END -->",
        content,
        flags=re.DOTALL
    )

    with open("README.md", "w") as file:
        file.write(new_content)


if __name__ == "__main__":
    try:
        with open("test_results.txt", "r") as file:
            raw_results = file.read()
    except FileNotFoundError:
        print("Error: 'test_results.txt' not found.")
        exit(1)

    category_summary_md = extract_category_summary(raw_results)

    if category_summary_md.startswith("Error:"):
        print(f"Error: Could not generate table for README: {category_summary_md}")
        exit(1)
    else:
        update_readme(category_summary_md)
        print("README.md updated successfully with simplified category summary (Category and Coverage).")