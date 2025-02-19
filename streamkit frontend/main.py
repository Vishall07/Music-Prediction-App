import streamlit as st
import pandas as pd
import requests
import io

# FastAPI Backend URL
FASTAPI_URL = "http://127.0.0.1:8000"

st.title("Spotify Data Filter & Prediction")

# File uploader
uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    st.write("### Uploaded Data:")
    st.dataframe(df.head())

    # Check if necessary columns exist
    required_columns = {"danceability", "tempo", "genre"}
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
            min_tempo, max_tempo = st.slider(
                "Select Tempo Range",
                min_value=float(df["tempo"].min()),
                max_value=float(df["tempo"].max()),
                value=(float(df["tempo"].min()), float(df["tempo"].max())),
                step=0.1
            )
            genre = st.selectbox("Select Genre", ["All"] + sorted(df["genre"].dropna().unique()))

        # Upload file to FastAPI
        if st.button("Upload & Predict"):
            try:
                csv_bytes = uploaded_file.getvalue()
                files = {"file": ("filtered_data.csv", io.BytesIO(csv_bytes), "text/csv")}
                upload_response = requests.post(f"{FASTAPI_URL}/upload/", files=files)

                if upload_response.status_code == 200:
                    params = {
                        "danceability_min": min_danceability,
                        "danceability_max": max_danceability,
                        "tempo_min": min_tempo,
                        "tempo_max": max_tempo,
                        "genre": genre
                    }
                    prediction_response = requests.get(f"{FASTAPI_URL}/predict/", params=params)

                    if prediction_response.status_code == 200:
                        try:
                            response_data = prediction_response.json()
                            results = response_data.get("results", [])
                            st.write("### Predictions:")
                            st.dataframe(pd.DataFrame(results))
                        except ValueError:
                            st.error(f"Error: Invalid JSON response. Raw response: {prediction_response.text}")
                    else:
                        st.error(f"Error: {prediction_response.status_code} - {prediction_response.text}")
                else:
                    st.error(f"Upload Error: {upload_response.status_code} - {upload_response.text}")
            except requests.exceptions.RequestException as e:
                st.error(f"Request failed: {str(e)}")