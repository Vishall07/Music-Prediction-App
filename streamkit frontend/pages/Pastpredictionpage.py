import streamlit as st
import pandas as pd
import requests

st.title("📊 Past Prediction Page")

# Date selection widgets
start_date = st.date_input("Start Date")
end_date = st.date_input("End Date")

# Fetch past predictions from FastAPI
url = "http://127.0.0.1:8000/docs#/default/predict_predict__get"

try:
    response = requests.get(url)
    
    if response.status_code == 200:
        predictions = response.json()
        df = pd.DataFrame(predictions)
        
        if df.empty:
            st.warning("No past predictions found.")
        else:
            st.write("### Raw Data from API:")
            st.dataframe(df.head())

            # Convert "date" column to datetime
            df["date"] = pd.to_datetime(df["date"])

            # Apply date filters
            filtered_df = df[
                (df["date"] >= pd.to_datetime(start_date)) &
                (df["date"] <= pd.to_datetime(end_date))
            ]

            st.write("### Filtered Past Predictions")
            st.dataframe(filtered_df)
    else:
        st.error(f"Error: {response.status_code} - {response.text}")

except requests.exceptions.RequestException as e:
    st.error(f"Error loading data: {str(e)}")