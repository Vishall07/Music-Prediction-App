import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta

API_URL = "http://fastapi-backend:8000"  # Update if your FastAPI is hosted elsewhere

st.set_page_config(page_title="Song Recommender", layout="wide")

# Sidebar navigation
page = st.sidebar.radio("Navigation", ["Prediction", "Past Predictions"])

# Prediction Page
if page == "Prediction":
    st.title("🎵 Song Recommendation Prediction")

    # Fetch list of songs
    @st.cache_data
    def fetch_songs():
        resp = requests.get(f"{API_URL}/songs?limit=500")
        return pd.DataFrame(resp.json())
    
    songs_df = fetch_songs()

    tabs = st.tabs(["Single Prediction", "Multi Prediction"])

    # Single Prediction
    with tabs[0]:
        st.subheader("Single Song Prediction")
        song = st.selectbox("Select a Song", options=songs_df["song"].unique())
        if st.button("Predict Recommendation"):
            response = requests.post(
                f"{API_URL}/predict",
                params={"request_source": "webapp"},
                json=[song]
            )
            res = response.json()
            st.write("**Prediction Result:**")
            st.json(res)

    # Multi Prediction
    with tabs[1]:
        st.subheader("Multiple Songs Prediction via CSV Upload")
        st.markdown("Upload a CSV file with a column `song` (without labels).")
        uploaded_file = st.file_uploader("Upload CSV", type=["csv"])
        if uploaded_file is not None:
            df = pd.read_csv(uploaded_file)
            if "song" not in df.columns:
                st.error("CSV must have a `song` column.")
            else:
                st.dataframe(df)
                if st.button("Predict for All Songs"):
                    song_list = df["song"].tolist()
                    response = requests.post(
                        f"{API_URL}/predict",
                        params={"request_source": "webapp"},
                        json=song_list
                    )
                    res = response.json()
                    # Convert to DataFrame
                    records = []
                    for input_song, pred in res.items():
                        if "error" in pred:
                            records.append({
                                "Input Song": input_song,
                                "Recommended Song": "N/A",
                                "Artist": "N/A",
                                "Genre": "N/A",
                                "Error": pred["error"]
                            })
                        else:
                            records.append({
                                "Input Song": input_song,
                                "Recommended Song": pred["recommended_song"],
                                "Artist": pred["artist"],
                                "Genre": pred["genre"],
                                "Error": ""
                            })
                    result_df = pd.DataFrame(records)
                    st.dataframe(result_df)
                    
elif page == "Past Predictions":
    st.title("📊 Past Predictions Explorer")

    # Date Range
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", datetime.now() - timedelta(days=7))
    with col2:
        end_date = st.date_input("End Date", datetime.now())

    # Source Selector
    source = st.selectbox(
        "Prediction Source",
        options=["all", "webapp", "scheduled predictions"]
    )

    # Fetch all past predictions
    resp = requests.get(f"{API_URL}/past-predictions")
    if resp.status_code != 200:
        st.error(f"API error: {resp.status_code} {resp.text}")
        st.stop()

    raw_preds = resp.json()

    if not raw_preds:
        st.warning("No predictions found.")
        st.stop()

    # Convert to DataFrame
    df_preds = pd.DataFrame(raw_preds)

    # Convert timestamp
    df_preds["timestamp"] = pd.to_datetime(df_preds["timestamp"])

    # Filter by date
    mask = (df_preds["timestamp"].dt.date >= start_date) & (df_preds["timestamp"].dt.date <= end_date)
    df_preds = df_preds[mask]

    # Filter by source
    if source != "all":
        df_preds = df_preds[df_preds["request_source"] == source]

    if df_preds.empty:
        st.warning("No predictions found for the selected filters.")
    else:
        st.dataframe(df_preds[["timestamp", "request_source", "input_songs", "output_predictions"]])

       
        