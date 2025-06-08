#!/usr/bin/env python3
import os
import argparse
import pandas as pd
import plotly.graph_objects as go

def generate_dbt_chart(output_dir='charts'):
    """
    Generate a horizontal stacked bar chart for DBT run status with custom colors and labels.

    Args:
        output_dir (str): Directory to save the chart image

    Returns:
        str: Path to the saved chart image or None if failed
    """
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'dbt_run_status.png')

    try:
        data = {
            'Status': ['PASS', 'ERROR', 'SKIP'],
            'Count': [576, 932, 1507]
        }
        df = pd.DataFrame(data)

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
                ['PASS', 'ERROR', 'SKIP'],
                ['#008000', '#FF0000', '#FFA500']  # Green, Red, Orange
            )
        ])

        fig.update_layout(
            barmode='stack',
            title='DBT Run Status (Total: 3015)',
            xaxis_title='',
            yaxis_title='Run',
            xaxis=dict(range=[0, 3015]),
            legend=dict(orientation='h', yanchor='bottom', y=-0.2, xanchor='center', x=0.5),
            margin=dict(t=50, l=50, r=50, b=50),
            width=900,
            height=400,
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
    args = parser.parse_args()

    chart_path = generate_dbt_chart(output_dir=args.output_dir)

    if chart_path:
        print(f"Chart generated successfully in {args.output_dir}")
        return 0
    else:
        print("Failed to generate chart")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())