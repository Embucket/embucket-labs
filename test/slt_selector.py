import os
import sys
import json
import argparse
import subprocess
from openai import OpenAI


def get_file_changes(base_branch="main"):
    """
    Get the actual changes (diffs) for files changed in the current PR using git merge-base
    Optimized for GitHub Actions environment and focused on database-relevant changes
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

            # Get the actual diff with context lines
            diff_cmd = ["git", "diff", "-U3", merge_base, "HEAD"]
            print(f"Running: {' '.join(diff_cmd)}")
            diff_result = subprocess.run(diff_cmd, capture_output=True, text=True, check=True)

            return _process_git_diff(diff_result.stdout)
        else:
            # Fallback to using the provided base_branch if not in GitHub Actions
            print(f"Not in GitHub Actions environment, using base branch: {base_branch}")
            subprocess.run(["git", "fetch", "origin", base_branch], check=True, capture_output=True)

            # Get the actual diff with context lines
            diff_cmd = ["git", "diff", "-U3", f"origin/{base_branch}", "HEAD"]
            print(f"Running: {' '.join(diff_cmd)}")
            diff_result = subprocess.run(diff_cmd, capture_output=True, text=True, check=True)

            return _process_git_diff(diff_result.stdout)
    except Exception as e:
        print(f"Error in get_file_changes: {e}")
        return {}


def _process_git_diff(diff_output):
    """
    Process git diff output and filter for database-relevant files
    Returns a dictionary with file paths as keys and their diffs as values
    """
    if not diff_output.strip():
        print("No changes found in git diff")
        return {}

    # Split diff into individual file diffs
    file_diffs = {}
    current_file = None
    current_diff_lines = []

    for line in diff_output.split('\n'):
        if line.startswith('diff --git'):
            # Save previous file diff if exists
            if current_file and current_diff_lines:
                if _is_database_relevant_file(current_file):
                    file_diffs[current_file] = '\n'.join(current_diff_lines)

            # Start new file diff
            # Extract file path from "diff --git a/path/to/file b/path/to/file"
            parts = line.split(' ')
            if len(parts) >= 4:
                current_file = parts[2][2:]  # Remove "a/" prefix
                current_diff_lines = [line]
            else:
                current_file = None
                current_diff_lines = []
        elif current_file:
            current_diff_lines.append(line)

    # Don't forget the last file
    if current_file and current_diff_lines:
        if _is_database_relevant_file(current_file):
            file_diffs[current_file] = '\n'.join(current_diff_lines)

    print(f"Found {len(file_diffs)} database-relevant changed files")
    for file_path in file_diffs.keys():
        print(f"  - {file_path}")

    return file_diffs


def _is_database_relevant_file(file_path):
    """
    Check if a file is likely relevant to database functionality
    """
    # Skip obviously irrelevant files
    irrelevant_patterns = [
        '.md',           # Documentation
        '.yml', '.yaml', # CI/CD configs
        '.json',         # Config files
        '.txt',          # Text files
        '.gitignore',    # Git files
        'LICENSE',       # License files
        'README',        # Readme files
        '.github/',      # GitHub workflows
        'docs/',         # Documentation
        'ui/',           # UI components (unless they contain SQL)
        'frontend/',     # Frontend code
        'web/',          # Web assets
    ]

    # Check if file should be skipped
    for pattern in irrelevant_patterns:
        if pattern in file_path.lower():
            # Special case: UI files might contain SQL, so include them if they mention SQL
            if 'ui/' in file_path.lower() or 'frontend/' in file_path.lower() or 'web/' in file_path.lower():
                if any(sql_term in file_path.lower() for sql_term in ['sql', 'query', 'database', 'db']):
                    return True
            return False

    # Include files that are likely database-relevant
    relevant_patterns = [
        '.rs',           # Rust source files
        '.py',           # Python files
        '.sql',          # SQL files
        '.slt',          # SLT test files
        'src/',          # Source code
        'crates/',       # Rust crates
        'test/',         # Test files
        'query',         # Query-related files
        'executor',      # Executor-related files
        'engine',        # Engine-related files
        'database',      # Database-related files
        'sql',           # SQL-related files
    ]

    for pattern in relevant_patterns:
        if pattern in file_path.lower():
            return True

    # Default to including the file if we're unsure
    return True


def _format_changes_for_prompt(file_changes):
    """
    Format the git diff output for better readability in the OpenAI prompt
    """
    if not file_changes:
        return "No relevant changes found."

    formatted_output = []
    for file_path, diff in file_changes.items():
        formatted_output.append(f"### File: {file_path}")
        formatted_output.append("```diff")
        formatted_output.append(diff)
        formatted_output.append("```")
        formatted_output.append("")  # Empty line for separation

    return "\n".join(formatted_output)


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


def select_relevant_slts(file_changes, all_slts, model="gpt-4-turbo"):
    """Use OpenAI to select relevant SLT files based on code changes (diffs)"""
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: OPENAI_API_KEY environment variable not set")
        print("Cannot perform SLT selection without OpenAI API key")
        return []

    try:
        client = OpenAI(api_key=api_key)
    except Exception as e:
        print(f"ERROR: Failed to initialize OpenAI client: {e}")
        return []

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

    # Format the changes for better readability in the prompt
    formatted_changes = _format_changes_for_prompt(file_changes)

    # Prepare message for OpenAI
    prompt = f"""
    I have made code changes to the following files in a Pull Request. Below are the actual diffs showing what changed:

    {formatted_changes}

    I need to select the most relevant SQL Logic Test (SLT) files to run based on these changes.
    Here are all the available SLT files, with paths indicating their functionality:

    ```
    {json.dumps(meaningful_paths, indent=2)}
    ```

    The paths shown are relative to the /sql/ directory and indicate what functionality the test is for. For example:
    - "function/aggregate/test.slt" tests SQL aggregate functions
    - "type/numeric/test.slt" tests numeric type functionality

    Please analyze the code changes and select the SLT files that test the functionality affected by these changes.
    Don't include any files where you don't see SPECIFIC evidence that the changes are relevant.
    IMPORTANT: if code changes are not related to database engine functionality - don't output any tests.
    IMPORTANT: don't include files just because they are "basic" tests. Be very specific with the output.
    Return an explanation for selection followed by a JSON array with these relative paths. For example:
    ```
    EXPLANATION: I changed the aggregate function code, so I need to run the aggregate function tests.
    JSON:
    ["function/aggregate/test.slt", "type/numeric/test.slt"]
    ```
    """

    print(prompt)

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=2000
        )

        # Extract JSON from response
        response_text = response.choices[0].message.content.strip()
        print(response_text)
    except Exception as e:
        print(f"ERROR: OpenAI API call failed: {e}")
        return []

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

        return valid_slts
    except json.JSONDecodeError:
        print(f"Failed to parse OpenAI response as JSON: {response_text}")
        return []


def parse_args():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(description='Select SLT tests based on code changes')
    parser.add_argument('--base-branch', type=str, default='main',
                        help='Base branch to compare against')
    parser.add_argument('--output-dir', type=str, default='./artifacts',
                        help='Output directory for results')
    parser.add_argument('--slt-dir', type=str, default='test/sql',
                        help='Directory containing SLT files')
    parser.add_argument('--model', type=str, default='gpt-4-turbo',
                        help='OpenAI model to use for selecting tests')

    return parser.parse_args()


def main():
    """
    Main entry point for the script
    """
    args = parse_args()

    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)

    # Get the file changes (diffs)
    file_changes = get_file_changes(args.base_branch)

    # If there are no file changes, exit
    if not file_changes:
        print("No relevant file changes found")
        # Create empty selection file
        selection_file = os.path.join(args.output_dir, "selected_slts.json")
        with open(selection_file, 'w') as f:
            json.dump([], f)
        return 0

    # Get all SLT files
    all_slts = get_all_slt_files(args.slt_dir)

    # If there are no SLT files, exit
    if not all_slts:
        print("No SLT files found")
        # Create empty selection file
        selection_file = os.path.join(args.output_dir, "selected_slts.json")
        with open(selection_file, 'w') as f:
            json.dump([], f)
        return 0

    # Select relevant SLTs
    selected_slts = select_relevant_slts(file_changes, all_slts, args.model)

    # Save the selected SLTs to a JSON file
    selection_file = os.path.join(args.output_dir, "selected_slts.json")
    with open(selection_file, 'w') as f:
        json.dump(selected_slts, f, indent=2)

    print(f"Selected {len(selected_slts)} SLT files:")
    for slt in selected_slts:
        print(f"  - {slt}")

    print(f"Selection saved to {selection_file}")

    # Always return 0 (success) - let the workflow handle the logic based on the JSON file
    return 0


if __name__ == "__main__":
    sys.exit(main())
