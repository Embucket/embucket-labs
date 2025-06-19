import plotly.express as px
import pandas as pd

# Load the data directly from the CSV file
df = pd.read_csv("test_statistics.csv")

# Compute aggregated statistics for each category
category_aggregates = df.groupby("category").agg(
    total_tests=("total_tests", "sum"),
    successful_tests=("successful_tests", "sum"),
    failed_tests=("failed_tests", "sum"),
    skipped_tests=("skipped_tests", "sum")
).reset_index()

# Calculate success rate excluding "Not Implemented" tests for each category
category_aggregates["ran_tests"] = category_aggregates["successful_tests"] + category_aggregates["failed_tests"]
category_aggregates["success_rate_percentage"] = (
    (category_aggregates["successful_tests"] / category_aggregates["ran_tests"]) * 100
).fillna(0).round(2)

# Calculate success rate for individual rows (excluding "Not Implemented" tests)
df["ran_tests"] = df["successful_tests"] + df["failed_tests"]
df["success_rate_percentage"] = (
    (df["successful_tests"] / df["ran_tests"]) * 100
).fillna(0).round(2)

# Merge the aggregated data back into the original DataFrame
# This allows categories to have their aggregated stats during hover
df = df.merge(category_aggregates, on="category", suffixes=("", "_category"))

# Create the treemap visualization
fig = px.treemap(
    df,
    path=["category", "page_name"],  # Use page_name instead of subcategory to match CSV structure
    values="total_tests",  # Defines the size of each rectangle
    color="success_rate_percentage",  # Color reflects success rate percentage (excludes "Not Implemented")
    color_continuous_scale="RdYlGn",  # Red for low success, green for high success
    title="Test Coverage Mind Map by Success Rate",
    custom_data=[
        "total_tests", "successful_tests", "failed_tests", "success_rate_percentage",  # Individual file stats
        "total_tests_category", "successful_tests_category", "failed_tests_category",
        "success_rate_percentage_category"  # Aggregated category stats
    ]
)

# Update the hover template
fig.update_traces(
    hovertemplate=(
            "<b>%{label}</b><br>" +
            "Total Tests: %{customdata[0]}<br>" +
            "Successful Tests: %{customdata[1]}<br>" +
            "Failed Tests: %{customdata[2]}<br>" +
            "Success Rate: %{customdata[3]:.1f}%" +  # Individual file success rate
            "<br><b>Aggregated for Category:</b><br>" +
            "Category Total Tests: %{customdata[4]}<br>" +
            "Category Successful Tests: %{customdata[5]}<br>" +
            "Category Failed Tests: %{customdata[6]}<br>" +
            "Category Success Rate: %{customdata[7]:.1f}%"
    )
)

# Show the interactive visualization
fig.show()
