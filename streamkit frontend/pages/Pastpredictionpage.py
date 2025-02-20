import streamlit as st
import requests
import pandas as pd

# Streamlit UI title
st.title("Past Predictions")

# Define the FastAPI backend URL
API_URL = "http://127.0.0.1:8000/past-predictions/"

# Function to fetch past predictions from FastAPI
def fetch_past_predictions():
    try:
        response = requests.get(API_URL)

        # Print raw response for debugging
        print("Raw API Response:", response.text)

        # Check for HTTP errors
        if response.status_code != 200:
            st.error(f"API Error: {response.status_code} - {response.reason}")
            return []

        # Parse JSON response
        data = response.json()

        # Ensure data is in expected format (list of dicts)
        if not isinstance(data, list):
            st.error("Unexpected API response format. Check FastAPI logs.")
            return []

        return data

    except requests.exceptions.RequestException as e:
        st.error(f"Error connecting to API: {e}")
        return []
    except ValueError as e:
        st.error(f"Error parsing JSON response: {e}")
        return []

# Fetch past predictions
past_predictions = fetch_past_predictions()

# Display results in a table if data exists
if past_predictions:
    df = pd.DataFrame(past_predictions)

    # Ensure necessary columns exist
    expected_columns = {"id", "feature1", "feature2", "prediction", "date"}
    if not expected_columns.issubset(df.columns):
        st.error("API response does not contain the required fields.")
    else:
        # Format and display the table
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        st.dataframe(df)
else:
    st.write("No past predictions found.")