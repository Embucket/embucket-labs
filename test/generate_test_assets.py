#!/usr/bin/env python3
import os
import sys
import argparse
import re
import json
import pandas as pd
import plotly.express as px
import numpy as np


def generate_badge(coverage_pct, output_dir='assets'):
    """
    Generate SVG badge showing test coverage percentage.

    Args:
        coverage_pct (float): The test coverage percentage
        output_dir (str): Directory to save the badge

    Returns:
        str: Path to the saved badge SVG file or None if failed
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'badge.svg')

    # Base bronze color
    bronze_base = "#CD7F32"

    # Determine opacity based on coverage percentage
    # Higher coverage = more solid (higher opacity)
    opacity = min(1.0, max(0.2, coverage_pct / 100))

    # Convert hex to RGB for the bronze color
    r = int(bronze_base[1:3], 16)
    g = int(bronze_base[3:5], 16)
    b = int(bronze_base[5:7], 16)

    # Create color with opacity
    color = f"rgba({r}, {g}, {b}, {opacity:.2f})"

    # Format percentage to one decimal place
    pct_text = f"{coverage_pct:.1f}%"

    # The label text is now "SLT coverage"
    label_text = "SLT coverage"

    # Calculate widths - adjust for the new label length
    label_width = len(label_text) * 7 + 10  # Width based on text length plus padding
    pct_width = max(len(pct_text) * 8, 40)  # Width based on percentage text length
    total_width = label_width + pct_width

    # Create SVG badge
    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{total_width}" height="20">
  <linearGradient id="b" x2="0" y2="100%">
    <stop offset="0" stop-color="#bbb" stop-opacity=".1"/>
    <stop offset="1" stop-opacity=".1"/>
  </linearGradient>
  <mask id="a">
    <rect width="{total_width}" height="20" rx="3" fill="#fff"/>
  </mask>
  <g mask="url(#a)">
    <path fill="#555" d="M0 0h{label_width}v20H0z"/>
    <path fill="{bronze_base}" fill-opacity="{opacity:.2f}" d="M{label_width} 0h{pct_width}v20H{label_width}z"/>
    <path fill="url(#b)" d="M0 0h{total_width}v20H0z"/>
  </g>
  <g fill="#fff" text-anchor="middle" font-family="DejaVu Sans,Verdana,Geneva,sans-serif" font-size="11">
    <text x="{label_width / 2}" y="15" fill="#010101" fill-opacity=".3">{label_text}</text>
    <text x="{label_width / 2}" y="14">{label_text}</text>
    <text x="{label_width + pct_width / 2}" y="15" fill="#010101" fill-opacity=".3">{pct_text}</text>
    <text x="{label_width + pct_width / 2}" y="14">{pct_text}</text>
  </g>
</svg>'''

    try:
        # Write SVG to file
        with open(output_file, 'w') as f:
            f.write(svg)

        # Also write a text file with the percentage for the GitHub workflow
        with open(os.path.join(output_dir, 'badge.txt'), 'w') as f:
            f.write(f"Test Coverage: {pct_text}")

        print(f"Badge generated with {pct_text} coverage")
        return output_file

    except Exception as e:
        print(f"Error generating badge: {str(e)}")
        return None


def generate_visualization(stats_file='test_statistics.csv', output_dir='assets'):
    """
    Generate visualization from test statistics CSV file and save it as an image.

    Args:
        stats_file (str): Path to the CSV file containing test statistics
        output_dir (str): Directory to save the visualization image

    Returns:
        tuple: (Path to the saved visualization image, overall coverage percentage) or (None, 0) if failed
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

        # Calculate overall coverage percentage
        total_tests = df['total_tests'].sum()
        successful_tests = df['successful_tests'].sum()
        overall_coverage = (successful_tests / total_tests * 100) if total_tests > 0 else 0

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
            margin=dict(t=50, l=25, r=25, b=25)
        )

        # Save the figure as a static image
        fig.write_image(output_file, width=1200, height=800)
        print(f"Visualization saved to {output_file}")

        return output_file, overall_coverage

    except FileNotFoundError:
        print(f"Error: Test statistics file not found at {stats_file}")
        return None, 0
    except Exception as e:
        print(f"Error generating visualization: {str(e)}")
        return None, 0


def main():
    """
    Generate test assets (badge and visualization) from test statistics.
    """
    parser = argparse.ArgumentParser(description='Generate test coverage assets')
    parser.add_argument('--stats-file', default='test_statistics.csv', help='Path to test statistics CSV file')
    parser.add_argument('--output-dir', required=True, help='Directory to output the assets')
    args = parser.parse_args()

    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)

    # Generate visualization first to get the overall coverage percentage
    viz_path, overall_coverage = generate_visualization(
        stats_file=args.stats_file,
        output_dir=args.output_dir
    )

    if overall_coverage > 0:
        # Generate badge with the coverage percentage
        badge_path = generate_badge(
            coverage_pct=overall_coverage,
            output_dir=args.output_dir
        )

        if badge_path and viz_path:
            print(f"All assets generated successfully in {args.output_dir}")
            print(f"Overall coverage: {overall_coverage:.1f}%")
            return 0
        else:
            print("Failed to generate some assets")
            return 1
    else:
        print("Failed to calculate overall coverage")
        return 1


if __name__ == "__main__":
    sys.exit(main())
