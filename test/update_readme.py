#!/usr/bin/env python3
"""
Script to update README.md files with test statistics visualization.
This script updates both the main README in the project root and the README in the test folder.
"""

import os
import re
import sys
import pandas as pd
import plotly.express as px


def generate_visualization(stats_file='test_statistics.csv', output_dir='assets'):
    """
    Generate visualization from test statistics CSV file and save it as an image.

    Args:
        stats_file (str): Path to the CSV file containing test statistics
        output_dir (str): Directory to save the visualization image

    Returns:
        str: Path to the saved visualization image or None if failed
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'test_coverage_visualization.png')

    try:
        # Read the CSV file
        df = pd.read_csv(stats_file)

        # Calculate success rate if not already present
        if 'category_success_rate' not in df.columns:
            df['category_success_rate'] = (df['successful_tests'] / df['total_tests']) * 100

        # Create the treemap visualization
        fig = px.treemap(
            df,
            path=['category', 'page_name'],
            values='total_tests',
            color='success_percentage',
            color_continuous_scale='RdYlGn',
            hover_data=['successful_tests', 'failed_tests'],
            range_color=[0, 100]
        )

        # Add title and adjust layout
        fig.update_layout(
            title="SQL Logic Test Coverage and Success Rate",
            margin=dict(t=50, l=25, r=25, b=25)
        )

        # Save the figure as a static image
        fig.write_image(output_file, width=1200, height=800)
        print(f"Visualization saved to {output_file}")

        return output_file

    except FileNotFoundError:
        print(f"Error: Test statistics file not found at {stats_file}")
        return None
    except Exception as e:
        print(f"Error generating visualization: {str(e)}")
        return None


def update_readme_with_visualization(readme_file, image_path, relative_image_path=None):
    """
    Update a README.md file to include the visualization image between
    specific comment markers: <!-- SLT_COVERAGE_START --> and <!-- SLT_COVERAGE_END -->

    Args:
        readme_file (str): Path to the README.md file
        image_path (str): Absolute path to the visualization image
        relative_image_path (str, optional): Path to the image relative to the README file location
                                            If None, uses image_path

    Returns:
        bool: True if successful, False otherwise
    """
    if not image_path or not os.path.exists(image_path):
        print(f"Error: Visualization image not found at {image_path}")
        return False

    # If relative path not provided, use the image_path
    if relative_image_path is None:
        relative_image_path = image_path

    try:
        # Check if README file exists
        if not os.path.exists(readme_file):
            print(f"Error: README file not found at {readme_file}")
            return False

        # Read the current README content
        with open(readme_file, 'r') as file:
            content = file.read()

        # Define the markers
        start_marker = "<!-- SLT_COVERAGE_START -->"
        end_marker = "<!-- SLT_COVERAGE_END -->"

        # Check if both markers exist in the README
        if start_marker not in content or end_marker not in content:
            print(f"Warning: Could not find both markers in {readme_file}.")
            print("Please add these markers to indicate where the visualization should be inserted.")
            return False

        # Define the visualization content to insert between markers
        viz_content = (
            f"{start_marker}\n"
            f"## SQL Logic Test Coverage Visualization\n\n"
            f"![Test Statistics Visualization]({relative_image_path})\n\n"
            f"*This visualization is automatically updated by CI/CD when tests are run.*\n"
            f"{end_marker}"
        )

        # Replace the content between markers
        pattern = f"{re.escape(start_marker)}.*?{re.escape(end_marker)}"
        updated_content = re.sub(pattern, viz_content, content, flags=re.DOTALL)

        # Write the updated content back to the README
        with open(readme_file, 'w') as file:
            file.write(updated_content)

        print(f"Successfully updated {readme_file} with visualization between markers")
        return True

    except Exception as e:
        print(f"Error updating README {readme_file}: {str(e)}")
        return False


def main():
    """Main function to generate visualization and update both README files."""
    # Get the current working directory
    current_dir = os.getcwd()

    # Define paths
    stats_file = os.path.join(current_dir, 'test_statistics.csv')
    output_dir = os.path.join(current_dir, 'assets')

    # Determine project root (assuming we're either in the project root or the test directory)
    if os.path.basename(current_dir) == 'test':
        # We're in the test directory
        project_root = os.path.dirname(current_dir)
        test_dir = current_dir
    else:
        # We're in the project root
        project_root = current_dir
        test_dir = os.path.join(project_root, 'test')

    # Define README paths
    root_readme = os.path.join(project_root, 'README.md')
    test_readme = os.path.join(test_dir, 'README.md')

    # Generate the visualization
    image_path = generate_visualization(stats_file, output_dir)
    if not image_path:
        sys.exit(1)

    # Define relative paths for the image from each README
    root_relative_path = os.path.relpath(image_path, project_root)
    test_relative_path = os.path.relpath(image_path, test_dir)

    # Update the root README
    if os.path.exists(root_readme):
        root_success = update_readme_with_visualization(root_readme, image_path, root_relative_path)
        if root_success:
            print(f"Updated root README at {root_readme}")
        else:
            print(f"Failed to update root README at {root_readme}")
    else:
        print(f"Root README not found at {root_readme}")

    # Update the test README
    if os.path.exists(test_readme):
        test_success = update_readme_with_visualization(test_readme, image_path, test_relative_path)
        if test_success:
            print(f"Updated test README at {test_readme}")
        else:
            print(f"Failed to update test README at {test_readme}")
    else:
        print(f"Test README not found at {test_readme}")

    print("README update process completed")


if __name__ == "__main__":
    main()
