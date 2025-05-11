import pandas as pd
import matplotlib.pyplot as plt

# Load Data (replace with your actual filenames)
snowflake_df = pd.read_csv("execution_times_snowflake.csv")
embucket_df = pd.read_csv("execution_times_embucket_c6a.24xlarge.csv")

# Data Processing (Optional): Rename columns for clarity
snowflake_df.columns = ['query_number', 'snowflake_time']
embucket_df.columns = ['query_number', 'embucket_time']

# Merge DataFrames (Assumes query numbers align)
merged_df = pd.merge(snowflake_df, embucket_df, on='query_number')

# Create Visualization
plt.figure(figsize=(12, 6))

# Set the width of each bar and positions of the bars
width = 0.35
x = merged_df['query_number']
x_pos = range(len(x))

# Create bars
plt.bar([i - width / 2 for i in x_pos], merged_df['snowflake_time'],
        width, label='Snowflake', color='skyblue')
plt.bar([i + width / 2 for i in x_pos], merged_df['embucket_time'],
        width, label='Embucket', color='lightgreen')

plt.xlabel('Query Number')
plt.ylabel('Time (seconds)')
plt.title('Snowflake vs. Embucket Benchmark')
plt.legend()
plt.grid(True, alpha=0.3)
plt.xticks(x_pos, merged_df['query_number'])

# Example cost calculation (replace with actual cost per second)
snowflake_cost_per_second = 0.01  # Example, replace with your value
embucket_cost_per_second = 0.005  # Example, replace with your value

merged_df['snowflake_cost'] = merged_df['snowflake_time'] * snowflake_cost_per_second
merged_df['embucket_cost'] = merged_df['embucket_time'] * embucket_cost_per_second

# Cost Visualization
plt.figure(figsize=(12, 6))

# Create cost comparison bars
plt.bar([i - width / 2 for i in x_pos], merged_df['snowflake_cost'],
        width, label='Snowflake Cost', color='skyblue')
plt.bar([i + width / 2 for i in x_pos], merged_df['embucket_cost'],
        width, label='Embucket Cost', color='lightgreen')

plt.xlabel('Query Number')
plt.ylabel('Cost')
plt.title('Snowflake vs. Embucket Cost Comparison')
plt.legend()
plt.grid(True, alpha=0.3)
plt.xticks(x_pos, merged_df['query_number'])
plt.show()
