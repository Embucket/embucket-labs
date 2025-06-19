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
    Only shows SLT files that have a success rate (excludes files with only "not implemented" tests).

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

        # Calculate overall coverage percentage from ALL files (including files with only "not implemented" tests)
        total_tests_all = df['total_tests'].sum()
        successful_tests_all = df['successful_tests'].sum()
        overall_coverage = (successful_tests_all / total_tests_all * 100) if total_tests_all > 0 else 0

        # Filter out files that have no success rate (only "not implemented" tests) for visualization only
        # These are files where successful_tests + failed_tests = 0
        df_filtered = df[(df['successful_tests'] + df['failed_tests']) > 0].copy()

        if len(df_filtered) == 0:
            print("No SLT files with testable content found")
            return None, 0

        # Use success_rate_percentage for color coding (excludes "not implemented" tests)
        color_column = 'success_rate_percentage'
        if color_column not in df_filtered.columns:
            # Fallback calculation if column doesn't exist
            df_filtered['success_rate_percentage'] = (
                df_filtered['successful_tests'] /
                (df_filtered['successful_tests'] + df_filtered['failed_tests']) * 100
            ).fillna(0)

        # Create the treemap visualization with color coding for success rates
        fig = px.treemap(
            df_filtered,
            path=['category', 'page_name'],
            values='total_tests',
            color='success_rate_percentage',
            color_continuous_scale='RdYlGn',
            hover_data=['successful_tests', 'failed_tests', 'not_implemented_tests'],
            range_color=[0, 100],
            title='SLT Test Coverage by Success Rate'
        )

        # Add title and adjust layout
        fig.update_layout(
            margin=dict(t=80, l=25, r=25, b=25),
            coloraxis_colorbar=dict(
                title=dict(text="Success Rate %", side="right")
            )
        )

        # Save the figure as a static image
        fig.write_image(output_file, width=1200, height=800)
        print(f"Success rate visualization saved to {output_file}")

        return output_file, overall_coverage

    except FileNotFoundError:
        print(f"Error: Test statistics file not found at {stats_file}")
        return None, 0
    except Exception as e:
        print(f"Error generating visualization: {str(e)}")
        return None, 0


def generate_not_implemented_visualization(stats_file='test_statistics.csv', output_dir='assets'):
    """
    Generate a bar chart showing "not implemented" tests vs other tests (successful + failed).

    Args:
        stats_file (str): Path to the CSV file containing test statistics
        output_dir (str): Directory to save the visualization image

    Returns:
        str: Path to the saved visualization image or None if failed
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'not_implemented_visualization.png')

    try:
        # Read the CSV file
        df = pd.read_csv(stats_file)

        # Calculate totals
        total_not_implemented = df['not_implemented_tests'].sum() if 'not_implemented_tests' in df.columns else 0
        total_other_tests = df['successful_tests'].sum() + df['failed_tests'].sum()
        total_all_tests = total_not_implemented + total_other_tests

        if total_all_tests == 0:
            print("No test data found")
            return None

        # Create data for the bar chart
        data = {
            'Test Type': ['Not Implemented', 'Other Tests (Success + Failed)'],
            'Count': [total_not_implemented, total_other_tests]
        }

        # Create DataFrame for plotting
        plot_df = pd.DataFrame(data)

        # Create horizontal bar chart
        fig = px.bar(
            plot_df,
            x='Count',
            y='Test Type',
            orientation='h',
            color='Test Type',
            color_discrete_map={
                'Not Implemented': '#2196F3',  # Blue
                'Other Tests (Success + Failed)': '#1976D2'  # Darker Blue
            },
            title='Test Distribution: Not Implemented vs Other Tests',
            text='Count'
        )

        # Update layout - remove x-axis and simplify
        fig.update_layout(
            margin=dict(t=80, l=25, r=25, b=25),
            showlegend=False,
            xaxis=dict(
                showticklabels=False,
                showgrid=False,
                zeroline=False,
                title=""
            ),
            yaxis_title="",
            height=400
        )

        # Update text position to show counts on bars
        fig.update_traces(textposition='inside', textfont_size=14, textfont_color='white')

        # Save the figure as a static image
        fig.write_image(output_file, width=1200, height=400)
        print(f"Not implemented visualization saved to {output_file}")

        return output_file

    except FileNotFoundError:
        print(f"Error: Test statistics file not found at {stats_file}")
        return None
    except Exception as e:
        print(f"Error generating not implemented visualization: {str(e)}")
        return None


def main():
    """
    Generate test assets (badge and visualizations) from test statistics.
    """
    parser = argparse.ArgumentParser(description='Generate test coverage assets')
    parser.add_argument('--stats-file', default='test_statistics.csv', help='Path to test statistics CSV file')
    parser.add_argument('--output-dir', required=True, help='Directory to output the assets')
    args = parser.parse_args()

    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)

    # Generate main visualization first to get the overall coverage percentage
    viz_path, overall_coverage = generate_visualization(
        stats_file=args.stats_file,
        output_dir=args.output_dir
    )

    # Generate not implemented visualization
    not_impl_viz_path = generate_not_implemented_visualization(
        stats_file=args.stats_file,
        output_dir=args.output_dir
    )

    success_count = 0

    if overall_coverage > 0:
        # Generate badge with the coverage percentage
        badge_path = generate_badge(
            coverage_pct=overall_coverage,
            output_dir=args.output_dir
        )

        if badge_path:
            success_count += 1
            print(f"Badge generated successfully")
        else:
            print("Failed to generate badge")

    if viz_path:
        success_count += 1
        print(f"Success rate visualization generated successfully")
    else:
        print("Failed to generate success rate visualization")

    if not_impl_viz_path:
        success_count += 1
        print(f"Not implemented visualization generated successfully")
    else:
        print("Failed to generate not implemented visualization")

    if success_count >= 2:  # At least 2 out of 3 assets generated
        print(f"Assets generated successfully in {args.output_dir}")
        if overall_coverage > 0:
            print(f"Overall coverage: {overall_coverage:.1f}%")
        return 0
    else:
        print("Failed to generate sufficient assets")
        return 1


if __name__ == "__main__":
    sys.exit(main())
