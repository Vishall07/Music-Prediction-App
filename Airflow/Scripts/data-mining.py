import pandas as pd
import great_expectations as ge

# Load the dataset
df = pd.read_csv("E:/EPITA/Data Science in Production/DSP/Music-Prediction-App/airflow/Scripts/synthetic_songs.csv")

# Convert DataFrame to a Great Expectations DataFrame
df_ge = ge.from_pandas(df)

# Define expectation: 'year' should be between 1900 and 2025
expectation = df_ge.expect_column_values_to_be_between("year", min_value=1900, max_value=2025)

# Extract invalid rows where 'year' is incorrect
invalid_rows = df[df["year"] > 2025]

# Display incorrect rows
if not invalid_rows.empty:
    print("\nRows with incorrect 'year' values (greater than 2025):\n")
    print(invalid_rows)

# Remove invalid rows
df_cleaned = df[df["year"] <= 2025]

# Save cleaned data
df_cleaned.to_csv("./Music-Prediction-app/airflow/data/RawData/filtered_songs.csv", index=False)

# Display first few rows of cleaned dataset
print("\nFirst few rows of the cleaned dataset:\n")
print(df_cleaned.head())
