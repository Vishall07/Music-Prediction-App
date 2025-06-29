import pandas as pd

# Load CSV
df = pd.read_csv("backend\songs.csv")

# Show basic info
print(df.head())

from sklearn.preprocessing import MinMaxScaler

# Save song titles for final display
song_titles = df['song']

# Select numerical features
features = ['danceability','energy','loudness','speechiness','acousticness',
            'instrumentalness','liveness','valence','tempo','popularity']

# Fill missing values if any
df[features] = df[features].fillna(0)

# Normalize features
scaler = MinMaxScaler()
scaled_features = scaler.fit_transform(df[features])


from sklearn.metrics.pairwise import cosine_similarity

# Compute similarity matrix
similarity_matrix = cosine_similarity(scaled_features)


def recommend_songs(song_name, top_n=2):
    # Get the index of the song
    index = df[df['song'] == song_name].index[0]
    
    # Get similarity scores
    sim_scores = list(enumerate(similarity_matrix[index]))
    
    # Sort by similarity
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
    
    # Get top N excluding the song itself
    top_indices = [i for i, _ in sim_scores[1:top_n+1]]
    
    return df.iloc[top_indices][['artist', 'song', 'genre']]


print("Recommendations for 'Oops!...I Did It Again':")
print(recommend_songs("Oops!...I Did It Again"))

print("\nRecommendations for 'The Real Slim Shady':")
print(recommend_songs("The Real Slim Shady"))
