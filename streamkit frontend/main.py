import streamlit as st
import pandas as pd
import requests
import io

# FastAPI Backend URL
FASTAPI_URL = "http://127.0.0.1:8000"

st.title("Spotify Data Filter")

# File uploader
uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

if uploaded_file is not None:
    # Read CSV file
    df = pd.read_csv(uploaded_file)
    st.write("### Uploaded Data:")
    st.dataframe(df.head())

    # Check if necessary columns exist
    required_columns = {"danceability", "genre", "artist"}
    if not required_columns.issubset(df.columns):
        st.error(f"CSV file must contain the following columns: {required_columns}")
    else:
        # Sidebar for interactive filters
        with st.sidebar:
            st.header("Filter Options")
            min_danceability, max_danceability = st.slider(
                "Select Danceability Range",
                min_value=float(df["danceability"].min()),
                max_value=float(df["danceability"].max()),
                value=(float(df["danceability"].min()), float(df["danceability"].max())),
                step=0.01
            )
            genre = st.selectbox("Select Genre", ["All"] + sorted(df["genre"].dropna().unique()))
            artist = st.selectbox("Select Artist", ["All"] + sorted(df["artist"].dropna().unique()))

        # Apply filters dynamically
        filtered_df = df[
            (df["danceability"] >= min_danceability) & (df["danceability"] <= max_danceability)
        ]

        if genre != "All":
            filtered_df = filtered_df[filtered_df["genre"].str.contains(genre, case=False, na=False)]

        if artist != "All":
            filtered_df = filtered_df[filtered_df["artist"].str.contains(artist, case=False, na=False)]

        st.write("### Filtered Data:")
        st.dataframe(filtered_df)

        # Send the filtered file to FastAPI for processing
        if st.button("🔮 Predict & Store in Database"):
            try:
                csv_bytes = uploaded_file.getvalue()
                files = {"file": ("filtered_data.csv", io.BytesIO(csv_bytes), "text/csv")}
                response = requests.post(f"{FASTAPI_URL}/upload/", files=files)

                if response.status_code == 200:
                    try:
                        results = response.json().get("results", [])
                        st.write("### Predictions:")
                        st.dataframe(pd.DataFrame(results))
                    except requests.exceptions.JSONDecodeError:
                        st.error("Error: FastAPI returned an empty or invalid response. Check the backend logs.")
                else:
                    st.error(f"Error: {response.status_code} - {response.text}")
                    st.error(f"Details: {response.json().get('detail', 'No additional info available.')}")
            except requests.exceptions.RequestException as e:
                st.error(f"Request failed: {str(e)}")

# Fetch past predictions from FastAPI
st.write("### Past Predictions")
try:
    response = requests.get(f"{FASTAPI_URL}/results/")
    
    if response.status_code == 200:
        try:
            past_predictions = pd.DataFrame(response.json())
            if not past_predictions.empty:
                st.dataframe(past_predictions)
            else:
                st.write("No past predictions found.")
        except requests.exceptions.JSONDecodeError:
            st.error("Error: FastAPI returned an empty response. Check the backend logs.")
    else:
        st.error(f"Error: {response.status_code} - {response.text}")

except requests.exceptions.RequestException as e:
    st.error(f"Unexpected error: {str(e)}") 