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

# Correctly calculate success percentage for each category
category_aggregates["success_percentage"] = (
        (category_aggregates["successful_tests"] / category_aggregates["total_tests"]) * 100
).round(2)

# Merge the aggregated data back into the original DataFrame
# This allows categories to have their aggregated stats during hover
df = df.merge(category_aggregates, on="category", suffixes=("", "_category"))

# Create the treemap visualization
fig = px.treemap(
    df,
    path=["category", "subcategory"],  # Define hierarchy: Category -> Subcategory
    values="total_tests",  # Defines the size of each rectangle
    color="success_percentage",  # Color reflects success percentage
    color_continuous_scale="RdYlGn",  # Red for low success, green for high success
    title="Test Coverage Mind Map",
    custom_data=[
        "total_tests", "successful_tests", "failed_tests", "skipped_tests", "success_percentage",  # Subcategory stats
        "total_tests_category", "successful_tests_category", "failed_tests_category", "skipped_tests_category",
        "success_percentage_category"  # Aggregated category stats
    ]
)

# Update the hover template
fig.update_traces(
    hovertemplate=(
            "<b>%{label}</b><br>" +
            "Total Tests: %{customdata[0]}<br>" +
            "Successful Tests: %{customdata[1]}<br>" +
            "Failed Tests: %{customdata[2]}<br>" +
            "Skipped Tests: %{customdata[3]}<br>" +
            "Success Percentage: %{customdata[4]}%" +  # Subcategory-level stats
            "<br><b>Aggregated for Category:</b><br>" +
            "Category Total Tests: %{customdata[5]}<br>" +
            "Category Successful Tests: %{customdata[6]}<br>" +
            "Category Failed Tests: %{customdata[7]}<br>" +
            "Category Skipped Tests: %{customdata[8]}<br>" +
            "Category Success Percentage: %{customdata[9]}%"
    )
)

# Show the interactive visualization
fig.show()
