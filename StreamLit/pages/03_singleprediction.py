import streamlit as st
import pandas as pd

# Page Configuration
st.set_page_config(page_title="Top Songs", page_icon="🎶", layout="centered")

# Heading
st.title("Top Songs")

# File Upload
uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

# Initialize a dictionary to store the play/pause state
song_states = {}

if uploaded_file is not None:
    # Read CSV into a DataFrame
    df = pd.read_csv(uploaded_file)

    # Check if the required columns exist in the CSV
    required_columns = {"danceability", "tempo", "genre", "song", "artist"}
    if not required_columns.issubset(df.columns):
        st.error(f"CSV file must contain the following columns: {required_columns}")
    else:
        # Apply filters on the sidebar
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

        # Filter data based on sidebar options
        if genre != "All":
            df = df[df["genre"] == genre]
        df = df[(df["danceability"] >= min_danceability) & (df["danceability"] <= max_danceability)]
        df = df[(df["tempo"] >= min_tempo) & (df["tempo"] <= max_tempo)]

        # Select only the Song and Artist columns
        songs_artists = df[['song', 'artist']].head(10)  # Show top 10 songs

        # Display table with Play/Pause icons
        for index, row in songs_artists.iterrows():
            song_name = row['song']
            artist_name = row['artist']

            # Set initial state for the song (False means paused, True means playing)
            if song_name not in song_states:
                song_states[song_name] = False

            # Create play/pause button
            if song_states[song_name]:
                button_label = "⏸️ Pause"
            else:
                button_label = "▶️ Play"
            
            # Display song and artist with play/pause button
            col1, col2, col3 = st.columns([3, 3, 1])  # 3:3:1 ratio for the song, artist, and button columns

            with col1:
                st.write(f"**Song**: {song_name}")
            with col2:
                st.write(f"**Artist**: {artist_name}")
            with col3:
                if st.button(button_label, key=song_name):
                    # When a song is played, pause all other songs
                    for key in song_states:
                        song_states[key] = False

                    # Toggle the play/pause state for the selected song
                    song_states[song_name] = not song_states[song_name]

else:
    st.info("Please upload a CSV file to display the songs.")
