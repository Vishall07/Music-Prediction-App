import pandas as pd
import great_expectations as gx

# Load data
df = pd.read_csv("music-prediction.csv")

# Create Great Expectations context
context = gx.get_context()

# Validate data: Remove rows with null values
gx_df = gx.from_pandas(df)
gx_df.expect_column_values_to_not_be_null("column_name_1")
gx_df.expect_column_values_to_not_be_null("column_name_2")

df_cleaned = df.dropna()
df_cleaned.to_csv("cleaned_data.csv", index=False)
print("Cleaned data saved successfully!")
