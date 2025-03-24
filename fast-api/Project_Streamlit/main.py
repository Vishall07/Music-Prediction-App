import streamlit as st
import pandas as pd
import requests

FASTAPI_URL = "http://127.0.0.1:8000"

st.title("🎵 Spotify Data Filter & Prediction")

# File Uploader
uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

# Ensure CSV is uploaded
if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)

    prediction_type = st.radio("Choose Prediction Type:", ["Single Prediction", "Multiple Predictions"])

    # SINGLE PREDICTION UI
    if prediction_type == "Single Prediction":
        st.subheader("🎯 Single Prediction")

        danceability = st.slider("Danceability:", float(df["danceability"].min()), float(df["danceability"].max()), step=0.01)
        tempo = st.slider("Tempo:", float(df["tempo"].min()), float(df["tempo"].max()), step=0.1)
        genre = st.selectbox("Select Genre", sorted(df["genre"].dropna().unique()))

        if st.button("Predict"):
            response = requests.post(f"{FASTAPI_URL}/single-predict/", files={"file": uploaded_file.getvalue()}, params={
                "danceability": danceability,
                "tempo": tempo,
                "genre": genre
            })

            if response.status_code == 200:
                result = response.json()
                st.success(f"🎵 Recommended Song: {result['prediction']}")
            else:
                st.error("⚠ No recommendations found.")

    # MULTIPLE PREDICTIONS UI
    else:
        st.subheader("📂 Multiple Predictions")

        with st.sidebar:
            st.header("⚙️ Filter Options")
            min_danceability, max_danceability = st.slider("Danceability Range", float(df["danceability"].min()), float(df["danceability"].max()), (float(df["danceability"].min()), float(df["danceability"].max())), step=0.01)
            min_tempo, max_tempo = st.slider("Tempo Range", float(df["tempo"].min()), float(df["tempo"].max()), (float(df["tempo"].min()), float(df["tempo"].max())), step=0.1)
            genre = st.selectbox("Select Genre", ["All"] + sorted(df["genre"].dropna().unique()))

        if st.button("Predict from CSV"):
            response = requests.post(f"{FASTAPI_URL}/predict/", files={"file": uploaded_file.getvalue()}, params={
                "danceability_min": min_danceability,
                "danceability_max": max_danceability,
                "tempo_min": min_tempo,
                "tempo_max": max_tempo,
                "genre": genre
            })

            if response.status_code == 200:
                response_data = response.json()
                if "results" in response_data:
                    st.write("### Predictions:")
                    st.dataframe(pd.DataFrame(response_data["results"]))
                else:
                    st.error("⚠ No results found.")
            else:
                st.error(f"❌ Error: {response.status_code} - {response.text}")