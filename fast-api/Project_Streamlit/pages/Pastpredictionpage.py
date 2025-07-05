import streamlit as st
import requests
import pandas as pd

st.title("Past Predictions")

API_URL = "http://127.0.0.1:8000/past-predictions/"

def fetch_past_predictions(start_date=None, end_date=None):
    """Fetch past predictions from FastAPI"""
    params = {}
    if start_date:
        params["start_date"] = start_date.strftime("%Y-%m-%d")
    if end_date:
        params["end_date"] = end_date.strftime("%Y-%m-%d")

    try:
        response = requests.get(API_URL, params=params)
        if response.status_code != 200:
            st.error(f"API Error: {response.status_code} - {response.text}")
            return []
        return response.json().get("past_predictions", [])
    except requests.exceptions.RequestException as e:
        st.error(f"Error connecting to API: {e}")
        return []

# Fetch predictions initially
past_predictions = fetch_past_predictions()

if past_predictions:
    df = pd.DataFrame(past_predictions)

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        # User input for filtering
        start_date = st.date_input("Start Date", df["timestamp"].min().date())
        end_date = st.date_input("End Date", df["timestamp"].max().date())

        # Fetch data within date range
        filtered_data = fetch_past_predictions(start_date, end_date)
        filtered_df = pd.DataFrame(filtered_data)

        if not filtered_df.empty:
            st.dataframe(filtered_df)
        else:
            st.warning("No predictions found for the selected date range.")
    else:
        st.error("Timestamp column missing in response.")
else:
    st.write("No past predictions found.")
