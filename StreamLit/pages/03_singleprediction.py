import streamlit as st

# Set page config
st.set_page_config(page_title="Song Popularity Predictor", layout="centered", initial_sidebar_state="auto")

# Page Title
st.title("Song Popularity Predictor")

# Create two tabs
tab1, tab2 = st.tabs(["Single Prediction", "Multi Prediction"])

# Single Prediction Tab
with tab1:
    st.header("Single Prediction")

    with st.container():
        st.subheader("Songs Popularity")

        # Song input with unique key
        song_name = st.text_input("Song", key="single_song_name")

        # Duration input
        duration = st.number_input("Duration (ms)", min_value=0, step=1)

        # Year input
        year = st.number_input("Year", min_value=1900, max_value=2100, step=1)

        # Energy slider
        energy = st.slider("Energy", min_value=0.0, max_value=1.0, step=0.01)

        # Loudness input
        loudness = st.number_input("Loudness", min_value=-60.0, max_value=0.0, step=0.01)

        # Genre dropdown list
        genre_options = [
            "Pop", "Hip-Hop", "Rock", "Metal", "Jazz", "Classical", "Electronic", "Reggae",
            "Blues", "Country", "R&B", "Soul", "Folk", "Latin", "Punk", "Disco", "Funk",
            "House", "Techno", "Dubstep", "Trance", "Gospel", "K-Pop", "Indie", "Ambient"
        ]

        genre = st.selectbox("Genre", options=genre_options, key="single_genre")

        # Button for prediction
        if st.button("Predict Popularity"):
            # Dummy output for now
            st.success(f"Predicted popularity for '{song_name}' (Genre: {genre}) is: 75%")

    st.markdown("---")

    with st.container():
        st.subheader("Songs Predictions")

        # Artist input with unique key
        artist_name = st.text_input("Artist", key="multi_artist_name")

        # Song input with unique key
        song_title = st.text_input("Song", key="multi_song_title")

        # Button for song prediction
        if st.button("Predict Songs"):
            # Dummy output for now
            st.info(f"Prediction for '{song_title}' by {artist_name}: Top Charts!")

# Multi Prediction Tab (Leave for you to fill logic later)
with tab2:
    st.header("Multi Prediction")

    st.write("Upload your dataset or provide the required data for multiple predictions.")

    # Placeholder for your multi prediction code
    uploaded_file = st.file_uploader("Upload your CSV file", type=["csv"])

    if uploaded_file is not None:
        st.success("File uploaded successfully!")
        # Later you can write your logic here
