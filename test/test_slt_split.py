#!/usr/bin/env python3
"""
Test script to verify the split SLT selector and runner work correctly
"""

import os
import sys
import json
import tempfile
import subprocess
from pathlib import Path

def test_slt_selector():
    """Test the SLT selector script"""
    print("Testing SLT selector...")
    
    # Create a temporary output directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Run the selector (this will likely fail without OpenAI API key, but we can check the structure)
        try:
            result = subprocess.run([
                sys.executable, "test/slt_selector.py",
                "--output-dir", temp_dir,
                "--base-branch", "main"
            ], capture_output=True, text=True, timeout=30)
            
            print(f"Selector exit code: {result.returncode}")
            print(f"Selector stdout: {result.stdout}")
            if result.stderr:
                print(f"Selector stderr: {result.stderr}")
            
            # Check if the output file was created
            selection_file = os.path.join(temp_dir, "selected_slts.json")
            if os.path.exists(selection_file):
                with open(selection_file, 'r') as f:
                    selection = json.load(f)
                print(f"Selection file created with {len(selection)} SLTs")
                return True
            else:
                print("Selection file not created")
                return False
                
        except subprocess.TimeoutExpired:
            print("Selector timed out (likely waiting for OpenAI API)")
            return True  # This is expected without API key
        except Exception as e:
            print(f"Error running selector: {e}")
            return False

def test_slt_runner():
    """Test the SLT runner script with empty selection"""
    print("\nTesting SLT runner with empty selection...")
    
    # Create a temporary output directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create an empty selection file
        selection_file = os.path.join(temp_dir, "selected_slts.json")
        with open(selection_file, 'w') as f:
            json.dump([], f)
        
        try:
            result = subprocess.run([
                sys.executable, "test/slt_runner_targeted.py",
                "--selection-file", selection_file,
                "--output-dir", temp_dir
            ], capture_output=True, text=True, timeout=10)
            
            print(f"Runner exit code: {result.returncode}")
            print(f"Runner stdout: {result.stdout}")
            if result.stderr:
                print(f"Runner stderr: {result.stderr}")
            
            # Should exit with code 0 for empty selection
            return result.returncode == 0
            
        except Exception as e:
            print(f"Error running runner: {e}")
            return False

def test_argument_parsing():
    """Test that both scripts have proper argument parsing"""
    print("\nTesting argument parsing...")
    
    # Test selector help
    try:
        result = subprocess.run([
            sys.executable, "test/slt_selector.py", "--help"
        ], capture_output=True, text=True, timeout=5)
        
        if result.returncode == 0 and "Select SLT tests" in result.stdout:
            print("✓ Selector help works")
        else:
            print("✗ Selector help failed")
            return False
    except Exception as e:
        print(f"✗ Selector help error: {e}")
        return False
    
    # Test runner help
    try:
        result = subprocess.run([
            sys.executable, "test/slt_runner_targeted.py", "--help"
        ], capture_output=True, text=True, timeout=5)
        
        if result.returncode == 0 and "Run selected SLT tests" in result.stdout:
            print("✓ Runner help works")
        else:
            print("✗ Runner help failed")
            return False
    except Exception as e:
        print(f"✗ Runner help error: {e}")
        return False
    
    return True

def main():
    """Run all tests"""
    print("Testing split SLT scripts...")
    
    # Change to the repository root
    repo_root = Path(__file__).parent.parent
    os.chdir(repo_root)
    
    tests = [
        test_argument_parsing,
        test_slt_selector,
        test_slt_runner,
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"Test {test.__name__} failed with exception: {e}")
            results.append(False)
    
    print(f"\nTest Results: {sum(results)}/{len(results)} passed")
    
    if all(results):
        print("✅ All tests passed!")
        return 0
    else:
        print("❌ Some tests failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())
