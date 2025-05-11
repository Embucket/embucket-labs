import pandas as pd
import plotly.express as px

file_path = 'test_statistics.csv'

try:
    df = pd.read_csv(file_path)
except FileNotFoundError:
    print(f"Error: File not found at {file_path}")
    exit()

df['category_success_rate'] = (df['successful_tests'] / df['total_tests']) * 100

fig = px.treemap(df,
                 path=['category', 'page_name'],
                 values='total_tests',
                 color='success_percentage',
                 color_continuous_scale='RdYlGn',
                 hover_data=['successful_tests', 'failed_tests'],
                 range_color=[0, 100]
                 )

fig.show()
