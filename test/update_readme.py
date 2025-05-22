#!/usr/bin/env python3
"""
Script to update the README.md file with test statistics visualization.
This script is intended to be run as part of CI/CD after tests have generated statistics.
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


def update_readme_with_visualization(readme_file='README.md', image_path=None):
    """
    Update the README.md file to include the visualization image.

    Args:
        readme_file (str): Path to the README.md file
        image_path (str): Path to the visualization image

    Returns:
        bool: True if successful, False otherwise
    """
    if not image_path or not os.path.exists(image_path):
        print("Error: Visualization image not found")
        return False

    try:
        # Read the current README content
        with open(readme_file, 'r') as file:
            content = file.read()

        # Define the visualization section header
        viz_section_header = "## Test Statistics Visualization"

        # Define the new visualization section content
        relative_image_path = image_path  # Adjust path to be relative to README location
        viz_section_content = (
            f"{viz_section_header}\n\n"
            f"Below is the current test coverage and success rate visualization:\n\n"
            f"![Test Statistics Visualization]({relative_image_path})\n\n"
            f"*This visualization is automatically updated by CI/CD when tests are run.*\n\n"
        )

        # Check if the visualization section already exists
        if viz_section_header in content:
            # Replace the existing section using regex
            pattern = f"{viz_section_header}.*?(?=\n##|$)"
            updated_content = re.sub(pattern, viz_section_content, content, flags=re.DOTALL)
        else:
            # Append the visualization section at the end
            updated_content = content + "\n" + viz_section_content

        # Write the updated content back to the README
        with open(readme_file, 'w') as file:
            file.write(updated_content)

        print(f"Successfully updated {readme_file} with visualization")
        return True

    except Exception as e:
        print(f"Error updating README: {str(e)}")
        return False


def main():
    """Main function to generate visualization and update README."""
    # Default paths
    stats_file = 'test_statistics.csv'
    readme_file = 'README.md'
    output_dir = 'assets'

    # Generate the visualization
    image_path = generate_visualization(stats_file, output_dir)
    if not image_path:
        sys.exit(1)

    # Update the README with the visualization
    success = update_readme_with_visualization(readme_file, image_path)
    if not success:
        sys.exit(1)

    print("README successfully updated with test statistics visualization")


if __name__ == "__main__":
    main()
