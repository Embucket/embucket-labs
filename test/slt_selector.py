import os
import sys
import json
import argparse
import subprocess
from openai import OpenAI


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

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
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

    # Get the changed files
    changed_files = get_changed_files(args.base_branch)

    # If there are no changed files, exit
    if not changed_files:
        print("No changed files found")
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
    selected_slts = select_relevant_slts(changed_files, all_slts, args.model)

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
