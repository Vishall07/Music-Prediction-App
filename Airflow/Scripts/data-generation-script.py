import pandas as pd
import numpy as np


num_samples = 10000 
artists = ["Artist " + str(i) for i in range(num_samples)]
songs = ["Song " + str(i) for i in range(num_samples)]
years = np.random.randint(1990, 2023, num_samples)
popularity = np.random.randint(20, 100, num_samples)
danceability = np.random.rand(num_samples)
energy = np.random.rand(num_samples)
loudness = np.random.uniform(-10, 0, num_samples)
tempo = np.random.uniform(60, 200, num_samples)
genres = np.random.choice(["pop", "rock", "jazz", "hip-hop", "classical"], num_samples)


df = pd.DataFrame({
    "artist": artists,
    "song": songs,
    "year": years,
    "popularity": popularity,
    "danceability": danceability,
    "energy": energy,
    "loudness": loudness,
    "tempo": tempo,
    "genre": genres
})


df.to_csv("./Music-Prediction-app/airflow/data/RawData/synthetic_songs.csv", index=False)
print(df.head())
print(df.sample(5)) 