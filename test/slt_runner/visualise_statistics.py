import pandas as pd
import plotly.express as px
import os

file_path = 'test_statistics.csv'
output_dir = 'assets'
output_file = os.path.join(output_dir, 'test_coverage_visualization.png')

# Create assets directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

try:
    df = pd.read_csv(file_path)
except FileNotFoundError:
    print(f"Error: File not found at {file_path}")
    exit()

df['category_success_rate'] = (df['successful_tests'] / df['total_tests']) * 100

# Use coverage_percentage if available, otherwise fall back to success_rate_percentage for backward compatibility
color_column = 'coverage_percentage' if 'coverage_percentage' in df.columns else 'success_rate_percentage'
if color_column not in df.columns:
    # Fallback for old CSV format
    color_column = 'success_percentage'

fig = px.treemap(df,
                 path=['category', 'page_name'],
                 values='total_tests',
                 color=color_column,
                 color_continuous_scale='RdYlGn',
                 hover_data=['successful_tests', 'failed_tests'],
                 range_color=[0, 100]
                 )

# Save the figure as a static image
fig.write_image(output_file, width=1200, height=800)
print(f"Visualization saved to {output_file}")

# Optionally, also display the figure
fig.show()
