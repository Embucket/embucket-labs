#!/usr/bin/env python3
"""
Setup Query Failure Detector

This script analyzes SLT runner error logs to identify .slt files where
at least one setup query (marked with "exclude-from-coverage") is failing.
Setup queries are typically CREATE TABLE, INSERT, DROP statements that prepare
the test environment but are not actual test queries.

Instead of running SLT tests, this tool parses existing error logs from slt_runner.

Usage:
    python test/setup_query_failure_detector.py [--test-dir TEST_DIR] [--error-log ERROR_LOG] [--output OUTPUT_FILE] [--verbose] [--append]

Examples:
    # Run with default settings
    python test/setup_query_failure_detector.py

    # Specify custom test directory and error log
    python test/setup_query_failure_detector.py --test-dir test/sql/sql-reference-functions --error-log my_errors.log

    # Save detailed report to custom file
    python test/setup_query_failure_detector.py --output my_failures.json

    # Run with verbose output
    python test/setup_query_failure_detector.py --verbose

    # Append results to existing JSON file instead of overwriting
    python test/setup_query_failure_detector.py --append
"""

import os
import sys
import re
import time
import argparse
from collections import defaultdict
from typing import List, Dict, Set, Optional, Tuple
import json


class SetupQueryFailureDetector:
    def __init__(self, test_directory: str = "sql", error_log_path: str = "errors.log"):
        self.test_directory = test_directory
        self.error_log_path = error_log_path
        self.failed_setup_files = set()
        self.setup_failure_details = defaultdict(list)
        
        # Patterns to identify setup queries (typically marked with exclude-from-coverage)
        self.setup_query_patterns = [
            r'CREATE\s+(OR\s+REPLACE\s+)?(TABLE|VIEW|SCHEMA|DATABASE)',
            r'INSERT\s+INTO',
            r'DROP\s+(TABLE|VIEW|SCHEMA|DATABASE)',
            r'USE\s+SCHEMA',
            r'CREATE\s+(OR\s+REPLACE\s+)?TEMP\s+TABLE'
        ]
        
    def validate_inputs(self):
        """Validate that required files exist."""
        if not os.path.exists(self.error_log_path):
            raise FileNotFoundError(f"Error log file not found: {self.error_log_path}")
        
        if not os.path.exists(self.test_directory):
            raise FileNotFoundError(f"Test directory not found: {self.test_directory}")
            
        print(f"✅ Error log found: {self.error_log_path}")
        print(f"✅ Test directory found: {self.test_directory}")

    def is_setup_query(self, query: str) -> bool:
        """Check if a query is likely a setup query based on patterns."""
        query_upper = query.upper().strip()
        for pattern in self.setup_query_patterns:
            if re.search(pattern, query_upper, re.IGNORECASE):
                return True
        return False
    
    def parse_error_log(self) -> Dict[str, List[Dict]]:
        """Parse the error log and extract failed queries by file."""
        print("📖 Parsing error log...")
        
        failed_queries_by_file = defaultdict(list)
        
        with open(self.error_log_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Split content into individual error entries
        # Each error starts with a line like "decode: 2" or similar, followed by error blocks
        error_pattern = r'={80,}\nQuery (?:unexpectedly failed|failed with unexpected error)! \(([^)]+\.slt):(\d+)\)!\n={80,}\n(.*?)\n={80,}\n(.*?)\n={80,}'
        
        matches = re.finditer(error_pattern, content, re.DOTALL)
        
        for match in matches:
            file_path = match.group(1)
            line_number = int(match.group(2))
            error_message = match.group(3).strip()
            query = match.group(4).strip()
            
            # Check if this is a setup query
            if self.is_setup_query(query):
                failed_queries_by_file[file_path].append({
                    'line_number': line_number,
                    'query': query,
                    'error': error_message,
                    'is_setup_query': True
                })
        
        print(f"📊 Found {sum(len(queries) for queries in failed_queries_by_file.values())} potential setup query failures")
        return failed_queries_by_file
    
    def verify_setup_queries_in_file(self, file_path: str, failed_queries: List[Dict]) -> List[Dict]:
        """Verify that failed queries are actually marked as exclude-from-coverage in the SLT file."""
        if not os.path.exists(file_path):
            return failed_queries  # Return as-is if file doesn't exist
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            verified_failures = []
            
            for failure in failed_queries:
                line_num = failure['line_number']
                
                # Check if there's an "exclude-from-coverage" directive before this line
                exclude_found = False
                for i in range(max(0, line_num - 10), min(len(lines), line_num)):
                    if 'exclude-from-coverage' in lines[i].lower():
                        exclude_found = True
                        break
                
                if exclude_found:
                    failure['verified_setup_query'] = True
                    verified_failures.append(failure)
                else:
                    # Still include it but mark as unverified
                    failure['verified_setup_query'] = False
                    verified_failures.append(failure)
            
            return verified_failures
            
        except Exception as e:
            print(f"  ⚠️  Could not verify setup queries in {file_path}: {e}")
            return failed_queries
        
    def analyze_file_for_setup_failures(self, file_path: str, failed_queries: List[Dict]) -> bool:
        """
        Analyze a single SLT file for setup query failures based on parsed error log data.
        
        Returns:
            bool: True if the file has failing setup queries, False otherwise
        """
        print(f"Analyzing: {file_path}")
        
        if not failed_queries:
            print(f"  ✅ No setup query failures found in error log")
            return False
        
        # Verify that the failed queries are actually setup queries
        verified_failures = self.verify_setup_queries_in_file(file_path, failed_queries)
        
        # Filter to only verified setup queries
        setup_failures = [f for f in verified_failures if f.get('verified_setup_query', False)]
        
        if setup_failures:
            self.failed_setup_files.add(file_path)
            self.setup_failure_details[file_path] = setup_failures
            print(f"  ❌ Found {len(setup_failures)} failing setup queries")
            return True
        else:
            unverified_count = len([f for f in verified_failures if not f.get('verified_setup_query', False)])
            if unverified_count > 0:
                print(f"  ⚠️  Found {unverified_count} failed queries that appear to be setup queries but are not marked with exclude-from-coverage")
            print(f"  ✅ No verified setup query failures")
            return False

    def run_detection(self) -> Dict:
        """
        Run setup query failure detection by analyzing error logs.
        
        Returns:
            Dict: Summary of results
        """
        start_time = time.time()
        
        print("=" * 60)
        print("SETUP QUERY FAILURE DETECTION (LOG ANALYSIS)")
        print("=" * 60)
        
        # Validate inputs
        self.validate_inputs()
        
        # Parse error log to find failed queries
        failed_queries_by_file = self.parse_error_log()
        
        if not failed_queries_by_file:
            print("🎉 No setup query failures found in error log!")
            return {'total_files': 0, 'failed_files': 0, 'failed_file_list': [], 'execution_time': 0}
        
        print(f"\n📊 Found potential setup query failures in {len(failed_queries_by_file)} files")
        print(f"🔍 Analyzing files to verify setup queries...")
        print("-" * 60)
        
        # Process each file with failures
        failed_count = 0
        for i, (file_path, failed_queries) in enumerate(failed_queries_by_file.items(), 1):
            print(f"[{i}/{len(failed_queries_by_file)}] ", end="")
            if self.analyze_file_for_setup_failures(file_path, failed_queries):
                failed_count += 1
        
        # Generate summary
        total_time = time.time() - start_time
        
        print("\n" + "=" * 60)
        print("DETECTION RESULTS")
        print("=" * 60)
        
        print(f"Total files with errors in log: {len(failed_queries_by_file)}")
        print(f"Files with failing setup queries: {failed_count}")
        print(f"Files with no setup query failures: {len(failed_queries_by_file) - failed_count}")
        print(f"Total execution time: {total_time:.2f} seconds")
        
        if self.failed_setup_files:
            print(f"\n📋 FILES WITH FAILING SETUP QUERIES:")
            print("-" * 40)
            for file_path in sorted(self.failed_setup_files):
                failure_count = len(self.setup_failure_details[file_path])
                print(f"  • {file_path} ({failure_count} failures)")
        else:
            print(f"\n🎉 All setup queries are passing!")
        
        return {
            'total_files': len(failed_queries_by_file),
            'failed_files': failed_count,
            'failed_file_list': sorted(list(self.failed_setup_files)),
            'execution_time': total_time
        }
    
    def save_detailed_report(self, output_file: str = "setup_query_failures.json", append_mode: bool = False):
        """Save detailed failure report to JSON file."""
        report = {
            'summary': {
                'total_failed_files': len(self.failed_setup_files),
                'failed_files': sorted(list(self.failed_setup_files))
            },
            'details': dict(self.setup_failure_details)
        }

        if append_mode and os.path.exists(output_file):
            # Load existing data and merge
            try:
                with open(output_file, 'r') as f:
                    existing_data = json.load(f)

                # Merge failed files (union of sets)
                existing_failed_files = set(existing_data.get('summary', {}).get('failed_files', []))
                new_failed_files = set(report['summary']['failed_files'])
                merged_failed_files = sorted(list(existing_failed_files.union(new_failed_files)))

                # Merge details (new data overwrites existing for same files)
                existing_details = existing_data.get('details', {})
                existing_details.update(report['details'])

                # Create merged report
                report = {
                    'summary': {
                        'total_failed_files': len(merged_failed_files),
                        'failed_files': merged_failed_files
                    },
                    'details': existing_details
                }

                print(f"\n📄 Appending to existing report: {output_file}")

            except (json.JSONDecodeError, KeyError) as e:
                print(f"\n⚠️  Could not parse existing file {output_file}: {e}")
                print(f"📄 Creating new report instead")
        else:
            if append_mode:
                print(f"\n📄 File {output_file} does not exist, creating new report")
            else:
                print(f"\n📄 Detailed report saved to: {output_file}")

        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
        
    def print_detailed_failures(self):
        """Print detailed information about each failure."""
        if not self.failed_setup_files:
            return
            
        print(f"\n" + "=" * 60)
        print("DETAILED FAILURE INFORMATION")
        print("=" * 60)
        
        for file_path in sorted(self.failed_setup_files):
            failures = self.setup_failure_details[file_path]
            print(f"\n📁 {file_path}")
            print("-" * len(file_path))
            
            for i, failure in enumerate(failures, 1):
                print(f"  {i}. Line {failure.get('line_number', 'unknown')}: {failure['query'][:100]}{'...' if len(failure['query']) > 100 else ''}")
                print(f"     Error: {failure['error']}")
                if failure.get('verified_setup_query'):
                    print(f"     Status: ✅ Verified setup query (exclude-from-coverage)")
                else:
                    print(f"     Status: ⚠️  Appears to be setup query but not marked with exclude-from-coverage")
                print()


def main():
    # Get the directory where this script is located (test/)
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Default paths relative to the script location
    default_test_dir = os.path.join(script_dir, 'sql')
    default_error_log = os.path.join(script_dir, 'errors.log')

    parser = argparse.ArgumentParser(
        description='Detect SLT files with failing setup queries by analyzing error logs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Run with default settings
  %(prog)s --test-dir test/sql/other          # Test specific directory
  %(prog)s --error-log my_errors.log         # Use custom error log
  %(prog)s --output failures.json            # Custom output file
  %(prog)s --verbose                          # Show detailed failures
  %(prog)s --append                           # Append to existing JSON file
  %(prog)s --append --output my_report.json  # Append to custom file
        """
    )

    parser.add_argument(
        '--test-dir',
        type=str,
        default=default_test_dir,
        help=f'Directory containing SLT test files (default: {default_test_dir})'
    )

    parser.add_argument(
        '--error-log',
        type=str,
        default=default_error_log,
        help=f'Path to SLT runner error log file (default: {default_error_log})'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default='setup_query_failures.json',
        help='Output file for detailed failure report (default: setup_query_failures.json)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show detailed failure information after detection'
    )
    
    parser.add_argument(
        '--list-only',
        action='store_true',
        help='Only list failed files, do not show detailed errors'
    )

    parser.add_argument(
        '--append',
        action='store_true',
        help='Append results to existing JSON file instead of overwriting'
    )
    
    args = parser.parse_args()
    
    # Validate inputs
    if not os.path.exists(args.test_dir):
        print(f"Error: Test directory '{args.test_dir}' does not exist.")
        return 1
    
    if not os.path.isdir(args.test_dir):
        print(f"Error: '{args.test_dir}' is not a directory.")
        return 1
        
    if not os.path.exists(args.error_log):
        print(f"Error: Error log file '{args.error_log}' does not exist.")
        print(f"Please run the SLT runner first to generate error logs.")
        return 1
    
    # Create detector instance
    detector = SetupQueryFailureDetector(test_directory=args.test_dir, error_log_path=args.error_log)
    
    try:
        print(f"🔍 Starting setup query failure detection...")
        print(f"📂 Test directory: {args.test_dir}")
        print(f"📋 Error log: {args.error_log}")
        print(f"📄 Output file: {args.output}")
        
        # Run the detection
        results = detector.run_detection()
        
        # Save detailed report
        detector.save_detailed_report(args.output, append_mode=args.append)
        
        # Show detailed failures if requested
        if args.verbose and results['failed_files'] > 0:
            detector.print_detailed_failures()
        elif results['failed_files'] > 0 and not args.list_only:
            print(f"\n💡 Use --verbose flag to see detailed failure information.")
        
        # Print final summary
        print(f"\n" + "=" * 60)
        print("FINAL SUMMARY")
        print("=" * 60)
        print(f"✅ Total files processed: {results['total_files']}")
        print(f"❌ Files with failing setup queries: {results['failed_files']}")
        print(f"⏱️  Execution time: {results['execution_time']:.2f} seconds")
        
        if results['failed_files'] > 0:
            print(f"\n📋 Failed files list:")
            for file_path in results['failed_file_list']:
                print(f"  • {file_path}")
            print(f"\n📄 Detailed report: {args.output}")
        
        # Return appropriate exit code
        return 1 if results['failed_files'] > 0 else 0
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Detection interrupted by user.")
        return 1
    except Exception as e:
        print(f"\n❌ Error during detection: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
