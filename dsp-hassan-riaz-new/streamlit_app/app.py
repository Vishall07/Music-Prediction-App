
import streamlit as st
import requests
import pandas as pd
from datetime import datetime

# API endpoint URLs
PREDICTION_API_URL = "http://localhost:8000/prediction"
PAST_PREDICTIONS_API_URL = "http://localhost:8000/past-predictions"

# Streamlit app title
st.title("Spotify Song Popularity Predictor")

# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Prediction", "Past Predictions"])

# Prediction Page
if page == "Prediction":
    st.header("Make a Prediction")

    # Tabs for single and multi predictions
    tab1, tab2 = st.tabs(["Single Prediction", "Multi Prediction"])

    with tab1:
        # Form for single prediction
        with st.form("single_prediction_form"):
            artist = st.text_input("Artist")
            song = st.text_input("Song")
            duration_ms = st.number_input("Duration (ms)", min_value=0)
            explicit = st.checkbox("Explicit")
            year = st.number_input("Year", min_value=1900, max_value=2023)
            danceability = st.slider("Danceability", min_value=0.0, max_value=1.0, step=0.01)
            energy = st.slider("Energy", min_value=0.0, max_value=1.0, step=0.01)
            key = st.number_input("Key", min_value=0, max_value=11)
            loudness = st.number_input("Loudness", min_value=-60.0, max_value=0.0, step=0.1)
            mode = st.selectbox("Mode", [0, 1])
            speechiness = st.slider("Speechiness", min_value=0.0, max_value=1.0, step=0.01)
            acousticness = st.slider("Acousticness", min_value=0.0, max_value=1.0, step=0.01)
            instrumentalness = st.slider("Instrumentalness", min_value=0.0, max_value=1.0, step=0.01)
            liveness = st.slider("Liveness", min_value=0.0, max_value=1.0, step=0.01)
            valence = st.slider("Valence", min_value=0.0, max_value=1.0, step=0.01)
            tempo = st.number_input("Tempo", min_value=0.0, max_value=300.0, step=0.1)
            genre = st.text_input("Genre")

            submitted = st.form_submit_button("Predict")

            if submitted:
                # Prepare input data
                input_data = {
                    "data": [
                        {
                            "artist": artist,
                            "song": song,
                            "duration_ms": duration_ms,
                            "explicit": explicit,
                            "year": year,
                            "danceability": danceability,
                            "energy": energy,
                            "key": key,
                            "loudness": loudness,
                            "mode": mode,
                            "speechiness": speechiness,
                            "acousticness": acousticness,
                            "instrumentalness": instrumentalness,
                            "liveness": liveness,
                            "valence": valence,
                            "tempo": tempo,
                            "genre": genre
                        }
                    ]
                }

                # Make a request to the prediction API
                response = requests.post(PREDICTION_API_URL, json=input_data)

                if response.status_code == 200:
                    predictions = response.json()["predictions"]
                    input_data["data"][0]["popularity"] = predictions[0]
                    df = pd.DataFrame(input_data["data"])
                    st.success("Prediction successful!")
                    st.dataframe(df)
                else:
                    st.error("Failed to make a prediction. Please try again.")

    with tab2:
        # File upload for multi prediction
        uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])
        if uploaded_file is not None:
            df = pd.read_csv(uploaded_file)
            st.write("Uploaded data:")
            st.dataframe(df)

            if st.button("Predict"):
                # Prepare input data
                input_data = {"data": df.to_dict(orient="records")}

                # Make a request to the prediction API
                response = requests.post(PREDICTION_API_URL, json=input_data)

                if response.status_code == 200:
                    predictions = response.json()["predictions"]
                    df["popularity"] = predictions
                    st.success("Predictions successful!")
                    st.dataframe(df)
                else:
                    st.error("Failed to make predictions. Please try again.")

# Past Predictions Page
elif page == "Past Predictions":
    st.header("Past Predictions")

    # Date range picker
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", datetime.now())
    with col2:
        end_date = st.date_input("End Date", datetime.now())

    # Prediction source dropdown
    prediction_source = st.selectbox("Prediction Source", ["All", "Webapp", "Scheduled Predictions"])

    # Retrieve past predictions
    if st.button("Retrieve Past Predictions"):
        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "source": prediction_source
        }
        response = requests.get(PAST_PREDICTIONS_API_URL, params=params)

        if response.status_code == 200:
            past_predictions = response.json()["past_predictions"]
            df = pd.DataFrame(past_predictions)
            st.dataframe(df)
        else:
            st.error("Failed to retrieve past predictions. Please try again.")