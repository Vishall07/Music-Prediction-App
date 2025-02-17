import streamlit as st
import pandas as pd

st.title("📊 Past Prediction Page")

# Date selection widgets
start_date = st.date_input("Start Date")
end_date = st.date_input("End Date")

# Fetch past predictions from FastAPI
url = "http://127.0.0.1:8000/results/"

try:
    df = pd.read_json(url)  # Load Data
    st.write("### Raw Data from API:")
    st.write(df.head())  # Debugging step

    if "Date" not in df.columns:
        st.error("Error: The dataset does not contain a 'Date' column.")
        st.write("Available columns:", df.columns.tolist())
        st.stop()

    # Convert "Date" column to datetime
    df["Date"] = pd.to_datetime(df["Date"])

    # Apply date filters
    filtered_df = df[
        (df["Date"] >= pd.to_datetime(start_date)) &
        (df["Date"] <= pd.to_datetime(end_date))
    ]

    st.write("### Filtered Past Predictions")
    st.dataframe(filtered_df)

except Exception as e:
    st.error(f"Error loading data: {str(e)}")