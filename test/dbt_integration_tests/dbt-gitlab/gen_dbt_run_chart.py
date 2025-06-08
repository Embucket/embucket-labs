#!/usr/bin/env python3
import os
import argparse
import pandas as pd
import plotly.graph_objects as go
import re

def parse_top_errors(file_path='top_errors.txt'):
    """
    Parse top_errors.txt to extract PASS, ERROR, SKIP, and TOTAL values.

    Args:
        file_path (str): Path to top_errors.txt

    Returns:
        dict: Dictionary with PASS, ERROR, SKIP, TOTAL values
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

def generate_dbt_chart(output_dir='charts', errors_file='top_errors.txt'):
    """
    Generate a horizontal stacked bar chart for DBT run status with custom colors and labels.

    Args:
        output_dir (str): Directory to save the chart image
        errors_file (str): Path to top_errors.txt

    Returns:
        str: Path to the saved chart image or None if failed
    """
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'dbt_run_status.png')

    try:
        # Parse data from top_errors.txt
        data = parse_top_errors(errors_file)
        if not data:
            raise ValueError("Failed to parse top_errors.txt")

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
                #yaxis=dict(title_font=dict(size=10, color='black'), tickfont=dict(color='white')),
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
                text=f"Embucket DBT-Gitlab Run Status (Total: {data['TOTAL']})",
                font=dict(size=12, color='black'),
                y=0.9,  # Moved down slightly
                x=0.5,
                xanchor='center'
            ),
            xaxis_title='',
            #yaxis_title='Run',
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
        fig.write_image(output_file)
        print(f"Chart saved to {output_file}")
        return output_file

    except Exception as e:
        print(f"Error generating chart: {str(e)}")
        return None

def main():
    parser = argparse.ArgumentParser(description='Generate DBT run status chart')
    parser.add_argument('--output-dir', default='charts', help='Directory to output the chart')
    parser.add_argument('--errors-file', default='top_errors.txt', help='Path to top_errors.txt')
    args = parser.parse_args()

    chart_path = generate_dbt_chart(output_dir=args.output_dir, errors_file=args.errors_file)

    if chart_path:
        print(f"Chart generated successfully in {args.output_dir}")
        return 0
    else:
        print("ErrorFailed to generate chart")
        return 1

if __name__ == '__main__':
    import sys
    sys.exit(main())