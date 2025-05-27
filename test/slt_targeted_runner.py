#!/usr/bin/env python3
"""
SLT Targeted Runner - Selects and runs relevant SQL Logic Tests based on code changes in a PR.
"""

import os
import sys
import json
import subprocess
import glob
import argparse
from openai import OpenAI


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

    Please select the SLT files that are most likely to test the functionality affected by my code changes.
    Return ONLY a JSON array with the filenames, no explanations or other text. For example:
    ["test/sql/file1.slt", "test/sql/file2.slt"]
    """

    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=2000
    )

    # Extract JSON from response
    response_text = response.choices[0].message.content.strip()

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


def run_slt_files(slt_files, slt_runner_dir="./test"):
    """Run the selected SLT files and capture results"""
    results = {}

    for slt_file in slt_files:
        print(f"Running test: {slt_file}")
        cmd = f"python -u -m slt_runner --test-file {slt_file}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=slt_runner_dir)
        success = result.returncode == 0

        results[slt_file] = {
            "success": success,
            "output": result.stdout,
            "error": result.stderr
        }

        print(f"Test {slt_file} {'succeeded' if success else 'failed'}")

    return results


def generate_pr_comment(test_results, model="gpt-4-turbo"):
    """Generate PR comment based on test results"""
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    all_success = all(result["success"] for result in test_results.values())

    # Prepare message for OpenAI
    prompt = f"""
    I ran SQL Logic Tests (SLTs) for a PR and got these results:

    ```
    {json.dumps(test_results, indent=2)}
    ```

    Please generate a GitHub PR comment that:

    1. If all tests passed, congratulate the developer and mention which functionality was validated.
    2. If any tests failed, explain which parts of functionality might be affected and direct the developer to check the workflow logs for details.
    3. Be concise and professional.

    The workflow URL can be referenced as: ${{{{ github.server_url }}}}/\
    ${{{{ github.repository }}}}/actions/runs/${{{{ github.run_id }}}}
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
    parser.add_argument('--output-dir', default='.', help='Directory to save output files')
    parser.add_argument('--model', default='gpt-4-turbo', help='OpenAI model to use')
    parser.add_argument('--run-all', action='store_true', help='Run all SLTs without selection')
    return parser.parse_args()


def main():
    """Main function"""
    args = parse_args()

    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)

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
    test_results = run_slt_files(selected_slts)

    # Save test results for debugging
    with open(os.path.join(args.output_dir, 'test_results.json'), 'w') as f:
        json.dump(test_results, f, indent=2)

    # Generate PR comment
    pr_comment = generate_pr_comment(test_results, args.model)

    # Save PR comment for GitHub Actions
    with open(os.path.join(args.output_dir, 'pr_comment.md'), 'w') as f:
        f.write(pr_comment)

    # Determine exit code based on test results
    if all(result["success"] for result in test_results.values()):
        return 0
    else:
        return 1


if __name__ == "__main__":
    sys.exit(main())
