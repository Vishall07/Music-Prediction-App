#---------------------------------------------------------------------------------------------------------------#
#                                            STREAMLIT APP (FRONTEND)
#---------------------------------------------------------------------------------------------------------------#

import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

#---------------------------------------------------------------------------------------------------------------#
# API Configurations
#---------------------------------------------------------------------------------------------------------------#
PREDICTION_API_URL = "http://localhost:8000/predict"
MULTIPLE_PREDICTION_API_URL = "http://localhost:8000/multiple-predict"
SIMILAR_SONGS_API_URL = "http://localhost:8000/similar-songs"
PAST_PREDICTIONS_API_URL = "http://localhost:8000/past-predictions/{prediction_type}"
VALIDATE_GENRE_API_URL = "http://localhost:8000/validate-genre"
SCHEDULED_PREDICTIONS_API_URL = "http://localhost:8000/scheduled-predictions"
#---------------------------------------------------------------------------------------------------------------#

#---------------------------------------------------------------------------------------------------------------#
# Page Configurations
#---------------------------------------------------------------------------------------------------------------#
st.set_page_config(page_title="Spotify Song Popularity Predictor", layout="centered")
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Prediction", "Past Predictions", "About"])
#---------------------------------------------------------------------------------------------------------------#

#---------------------------------------------------------------------------------------------------------------#
# About Page
#---------------------------------------------------------------------------------------------------------------#
if page == "About":
    st.title("About the Spotify Recommendation System")
    st.write("""
    *Project Features:*
    - *Single Prediction*: Predict popularity for individual songs
    - *Multiple Prediction*: Process multiple songs via CSV upload
    - *Similar Songs*: Find musically similar tracks
    - *Prediction History*: View past predictions by type
    - *Scheduled Predictions*: View automated predictions from Airflow
    """)
#---------------------------------------------------------------------------------------------------------------#

#---------------------------------------------------------------------------------------------------------------#
# Prediction Page
#---------------------------------------------------------------------------------------------------------------#
elif page == "Prediction":
    st.title("Song Prediction & Recommendation System")
    
    tab1, tab2 = st.tabs(["Single Prediction", "Multiple Prediction"])

    with tab1:
        st.subheader("Song Popularity Prediction")
        
        with st.form("prediction_form"):
            col1, col2 = st.columns(2)
            with col1:
                song = st.text_input("Song Name", help="Enter the song title (letters only)")
                duration_ms = st.number_input("Duration (ms)", min_value=0, value=180000)
                year = st.number_input("Year", min_value=1900, max_value=datetime.now().year, value=2020)
            with col2:
                energy = st.slider("Energy", min_value=0.0, max_value=1.0, value=0.5)
                loudness = st.slider("Loudness (dB)", min_value=-60.0, max_value=0.0, value=-10.0)
                genre = st.text_input("Genre", placeholder="Enter genre (e.g., pop, rock)")
            
            if st.form_submit_button("Predict Popularity"):
                if not all([song, genre]):
                    st.error("Please fill in all required fields")
                elif not song.replace(" ", "").isalpha():
                    st.error("Song name should contain only letters and spaces")
                else:
                    with st.spinner("Validating genre..."):
                        try:
                            genre_check = requests.post(
                                VALIDATE_GENRE_API_URL,
                                json={"genre": genre}
                            )
                            genre_data = genre_check.json()
                            
                            if not genre_data.get("valid", False):
                                st.error("Invalid genre. Please check your spelling and try again.")
                                st.stop()
                            
                            with st.spinner("Making prediction..."):
                                response = requests.post(
                                    PREDICTION_API_URL,
                                    json={
                                        "song": song,
                                        "duration_ms": duration_ms,
                                        "year": year,
                                        "energy": energy,
                                        "loudness": loudness,
                                        "genre": genre
                                    }
                                )
                                response.raise_for_status()
                                
                                result = response.json()
                                if isinstance(result, dict):
                                    st.success(f"Predicted Popularity: {result.get('prediction', 'N/A')}")
                                else:
                                    st.error("Unexpected response format")
                        except requests.exceptions.RequestException as e:
                            st.error(f"Prediction failed: {str(e)}")

        st.subheader("Find Similar Songs")
        with st.form("similar_songs_form"):
            song_query = st.text_input("Song Name", placeholder="Example: In The End, Happy, Escape")
            
            if st.form_submit_button("Find Similar Songs"):
                if not song_query:
                    st.warning("Please enter a song name")
                else:
                    with st.spinner("Searching for similar songs..."):
                        try:
                            params = {"song": song_query}
                            response = requests.get(SIMILAR_SONGS_API_URL, params=params)
                            
                            if response.status_code == 200:
                                result = response.json()
                                
                                if "message" in result:
                                    st.info(result["message"])
                                elif "similar_songs" in result:
                                    if not result["similar_songs"]:
                                        st.info("No similar songs found")
                                    else:
                                        ref = result["reference"]
                                        st.success(f"Songs similar to: {ref.get('song', '')}")
                                        
                                        similar_df = pd.DataFrame(result["similar_songs"])
                                        similar_df.rename(columns={
                                            "song": "Song",
                                            "artist": "Artist",
                                            "similarity_score": "Similarity Score"
                                        }, inplace=True)
                                        
                                        similar_df["Similarity Score"] = similar_df["Similarity Score"].apply(
                                            lambda x: f"{float(x):.2f}")
                                        st.dataframe(similar_df[["Song", "Artist", "Similarity Score"]])
                            else:
                                st.error(f"Error: {response.json().get('detail', 'Unknown error')}")
                                
                        except requests.exceptions.RequestException as e:
                            st.error(f"Failed to connect to server: {str(e)}")

    with tab2:
        st.header("Multiple Prediction")
        
        uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])
        if uploaded_file is not None:
            try:
                df = pd.read_csv(uploaded_file, on_bad_lines='warn')
                
                if df.empty:
                    st.error("The uploaded file couldn't be processed. Please check the file format.")
                    st.stop()
                    
                st.write("Preview of uploaded data:")
                st.dataframe(df.head())
                
                required_cols = ["song", "duration_ms", "year", "energy", "loudness", "genre"]
                missing_cols = [col for col in required_cols if col not in df.columns]
                
                if missing_cols:
                    st.error(f"Missing required columns: {', '.join(missing_cols)}")
                else:
                    if st.button("Predict Popularity for All Songs"):
                        with st.spinner("Processing your request! Please wait..."):
                            try:
                                files = {'file': (uploaded_file.name, uploaded_file.getvalue())}
                                response = requests.post(MULTIPLE_PREDICTION_API_URL, files=files, timeout=30)
                                response.raise_for_status()
                                
                                results = response.json()
                                
                                if all(k in results for k in ["batch_id", "total_predictions", "average_popularity"]):
                                    st.success("Batch Prediction Completed Successfully!")
                                    
                                    col1, col2 = st.columns(2)
                                    col1.metric("Batch ID", results["batch_id"])
                                    col2.metric("Total Predictions", results["total_predictions"])

                                    st.metric("Average Popularity", f"{results['average_popularity']:.2f}")

                                    pred_data = results["predictions"]
                                    df_results = pd.DataFrame({
                                        'Song': pred_data['songs'],
                                        'Predicted Popularity': pred_data['predictions']
                                    })
                                    st.subheader("Detailed Statistics")
                                    st.dataframe(df_results)

                                    csv = df_results.to_csv(index=False)
                                    st.download_button(
                                        "Download Predictions",
                                        data=csv,
                                        file_name="song_predictions.csv",
                                        mime="text/csv"
                                    )
                                else:
                                    st.error("Unexpected Response Format")   

                            except requests.exceptions.RequestException as e:
                                st.error(f"Prediction failed: {str(e)}")
            except Exception as e:
                st.error(f"Error reading file: {str(e)}")

#---------------------------------------------------------------------------------------------------------------#
# Past Predictions Page
#---------------------------------------------------------------------------------------------------------------#
elif page == "Past Predictions":
    st.title("Prediction History")
    
    prediction_type = st.selectbox(
        "Prediction Type",
        ["Single Popularity", "Similar Songs", "Multiple Predictions", "Scheduled Predictions"],
        format_func=lambda x: x.replace("_", " ").title()
    )
    
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", datetime.now() - timedelta(days=7))
    with col2:
        end_date = st.date_input("End Date", datetime.now())
    
    if prediction_type != "Scheduled Predictions":
        source = st.selectbox("Source", ["ALL", "WEBAPP", "SCHEDULED"])
    
    if st.button("Retrieve Predictions"):
        with st.spinner("Loading..."):
            try:
                if prediction_type == "Scheduled Predictions":
                    response = requests.get(
                        SCHEDULED_PREDICTIONS_API_URL,
                        params={
                            "start_date": start_date.isoformat(),
                            "end_date": end_date.isoformat()
                        },
                        timeout=10
                    )
                    
                    if response.status_code != 200:
                        st.error("Failed to fetch scheduled predictions")
                        if response.status_code == 500:
                            st.error("Database connection error")
                        st.stop()
                    
                    results = response.json()
                    
                    if isinstance(results, dict) and "message" in results:
                        st.info(results["message"])
                    else:
                        df = pd.DataFrame(results)
                        
                        if 'timestamp' in df.columns:
                            df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d %H:%M')
                        
                        st.dataframe(df[['timestamp', 'song', 'genre', 'predicted_popularity']])
                        
                        col1, col2 = st.columns(2)
                        col1.metric("Total Predictions", len(df))
                        col2.metric("Avg Popularity", f"{df['predicted_popularity'].mean():.2f}")
                        
                else:
                    type_mapping = {
                        "Single Popularity": "single",
                        "Similar Songs": "similar",
                        "Multiple Predictions": "multiple"
                    }
                    
                    url = PAST_PREDICTIONS_API_URL.format(
                        prediction_type=type_mapping[prediction_type]
                    )
                    
                    params = {
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat(),
                        "source": source
                    }
                    
                    response = requests.get(url, params=params)
                    response.raise_for_status()
                    
                    results = response.json()
                    
                    if isinstance(results, dict) and "message" in results:
                        st.info(results["message"])
                    elif isinstance(results, list):
                        if not results:
                            st.info("No predictions found for selected criteria")
                        else:
                            if prediction_type == "Single Popularity":
                                df = pd.DataFrame(results)
                                st.dataframe(df)
                                
                                st.subheader("Statistics")
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.metric("Total Predictions", len(df))
                                with col2:
                                    avg_pop = df["predicted_popularity"].mean()
                                    st.metric("Average Popularity", f"{avg_pop:.2f}")
                            
                            elif prediction_type == "Similar Songs":
                                expanded_data = []
                                for pred in results:
                                    expanded_data.extend([
                                        {
                                            "Reference Song": pred.get("reference_song"),
                                            "Similar Song": song.get("song"),
                                            "Similar Artist": song.get("artist"),
                                            "Similarity Score": f"{song.get('similarity_score', 0):.2f}",
                                            "Date": pred.get("timestamp")[:10]
                                        }
                                        for song in pred.get("similar_songs", [])
                                    ])
                                st.dataframe(pd.DataFrame(expanded_data))
                            
                            elif prediction_type == "Multiple Predictions":
                                df = pd.DataFrame(results)
                                st.dataframe(df)
                                
                                st.subheader("Statistics")
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.metric("Total Batches", len(df))
                                with col2:
                                    avg_pop = df["average_popularity"].mean()
                                    st.metric("Overall Average Popularity", f"{avg_pop:.2f}")
                    else:
                        st.error("Unexpected response format")
                    
            except requests.exceptions.RequestException as e:
                st.error(f"Connection error: {str(e)}")
#---------------------------------------------------------------------------------------------------------------#