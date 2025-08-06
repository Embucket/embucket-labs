#!/usr/bin/env python3
import os
import argparse
import pandas as pd
import plotly.graph_objects as go
import re

def parse_top_errors(file_path='assets/top_errors.txt'):
    """
    Parse top_errors.txt to extract PASS, ERROR, SKIP, and TOTAL values.

    Args:
        file_path (str): Path to top_errors.txt

    Returns:
        dict: Dictionary with PASS, ERROR, SKIP, TOTAL, WARN values
    """
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        data = {
            'PASS': int(re.search(r'PASS: (\d+)', content).group(1)),
            'ERROR': int(re.search(r'ERROR: (\d+)', content).group(1)),
            'SKIP': int(re.search(r'SKIP: (\d+)', content).group(1)),
            'TOTAL': int(re.search(r'TOTAL: (\d+)', content).group(1)),
            'WARN': int(re.search(r'WARN: (\d+)', content).group(1))
        }
        return data
    except Exception as e:
        print(f"Error parsing {file_path}: {str(e)}")
        return None

def generate_badge(success_rate, output_dir='assets'):
    """
    Generate SVG badge showing DBT success rate percentage.

    Args:
        success_rate (float): The DBT success rate percentage (PASS / TOTAL * 100)
        output_dir (str): Directory to save the badge

    Returns:
        str: Path to the saved badge SVG file or None if failed
    """
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'dbt_success_badge.svg')

    # Green color
    green_base = "#B2FFB2"

    # Determine opacity based on success rate
    opacity = min(1.0, max(0.2, success_rate / 100))

    # Convert hex to RGB for the green color
    r = int(green_base[1:3], 16)
    g = int(green_base[3:5], 16)
    b = int(green_base[5:7], 16)

    # Create color with opacity
    color = f"rgba({r}, {g}, {b}, {opacity:.2f})"

    # Format percentage to one decimal place
    pct_text = f"{success_rate:.1f}%"

    # Label text for the badge
    label_text = "DBT Snowplow Web Success Rate"

    # Calculate widths
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
    <path fill="{color}" fill-opacity="{opacity:.2f}" d="M{label_width} 0h{pct_width}v20H{label_width}z"/>
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

        # Write a text file with the percentage for the GitHub workflow
        with open(os.path.join(output_dir, 'dbt_success_badge.txt'), 'w') as f:
            f.write(f"DBT Snowplow Web Success Rate: {pct_text}")

        print(f"Badge generated with {pct_text} success rate")
        return output_file

    except Exception as e:
        print(f"Error generating badge: {str(e)}")
        return None

def generate_dbt_chart(output_dir='assets', errors_file='assets/top_errors.txt'):
    """
    Generate a horizontal stacked bar chart for DBT run status with custom colors and labels,
    and a badge for DBT success rate.

    Args:
        output_dir (str): Directory to save the chart and badge
        errors_file (str): Path to top_errors.txt

    Returns:
        tuple: (Path to chart, Path to badge) or (None, None) if failed
    """
    os.makedirs(output_dir, exist_ok=True)
    chart_output_file = os.path.join(output_dir, 'dbt_run_status.png')

    try:
        # Parse data from top_errors.txt
        data = parse_top_errors(errors_file)
        if not data:
            raise ValueError("Failed to parse top_errors.txt")

        # Calculate success rate (PASS / TOTAL * 100)
        success_rate = (data['PASS'] / data['TOTAL'] * 100) if data['TOTAL'] > 0 else 0

        # Generate badge
        badge_path = generate_badge(success_rate, output_dir)

        # Exclude WARN if 0
        statuses = ['PASS', 'ERROR', 'SKIP']
        counts = [data['PASS'], data['ERROR'], data['SKIP']]

        df = pd.DataFrame({
            'Status': statuses,
            'Count': counts
        })

        fig = go.Figure(data=[
            go.Bar(
                y=['DBT Run'],
                x=[df.loc[df['Status'] == status, 'Count'].iloc[0]],
                name=status,
                marker_color=color,
                orientation='h',
                text=[df.loc[df['Status'] == status, 'Count'].iloc[0]],
                textposition='inside',
                textfont=dict(size=14, color='white')
            ) for status, color in zip(
                statuses,
                ['#008000', '#FF0000', '#FFA500']  # Dark Green, Red, Orange
            )
        ])

        fig.update_layout(
            barmode='stack',
            title=dict(
                text=f"",
                font=dict(size=12, color='black'),
                y=0.9,
                x=0.5,
                xanchor='center'
            ),
            xaxis_title='',
            xaxis=dict(range=[0, data['TOTAL']]),
            legend=dict(
                orientation='h',
                yanchor='bottom',
                y=-0.7,
                xanchor='center',
                x=0.5,
                font=dict(size=8)
            ),
            margin=dict(l=30, r=30, t=30, b=30),
            width=900,
            height=150,
            plot_bgcolor='white',
            paper_bgcolor='white',
            showlegend=True
        )
        fig.write_image(chart_output_file)
        print(f"Chart saved to {chart_output_file}")
        return chart_output_file, badge_path

    except Exception as e:
        print(f"Error generating chart or badge: {str(e)}")
        return None, None

def main():
    parser = argparse.ArgumentParser(description='Generate DBT run status chart and badge')
    parser.add_argument('--output-dir', default='assets', help='Directory to output the chart and badge')
    parser.add_argument('--errors-file', default='assets/top_errors.txt', help='Path to top_errors.txt')
    args = parser.parse_args()

    chart_path, badge_path = generate_dbt_chart(output_dir=args.output_dir, errors_file=args.errors_file)

    if chart_path and badge_path:
        print(f"Chart and badge generated successfully in {args.output_dir}")
        return 0
    else:
        print("Failed to generate chart or badge")
        return 1

if __name__ == '__main__':
    import sys
    sys.exit(main())