#---------------------------------------------------------------------------------------------------------------#
#                                  SCRIPT TO SPLIT DATASET (TEN DATASETS)
#---------------------------------------------------------------------------------------------------------------#



#---------------------------------------------------------------------------------------------------------------#
# Imports
#---------------------------------------------------------------------------------------------------------------#
# import pandas as pd
# import os
# import random
# #---------------------------------------------------------------------------------------------------------------#



# #---------------------------------------------------------------------------------------------------------------#
# # Data Configurations (Spliting and Pathing)
# #---------------------------------------------------------------------------------------------------------------#
# def split_dataset(dataset_path, raw_data_folder, num_files):
    
#     df = pd.read_csv(dataset_path)

#     os.makedirs(raw_data_folder, exist_ok=True)

#     for i in range(num_files):

#         sample = df.sample(frac=1.0/num_files, replace=False)

#         file_path = os.path.join(raw_data_folder, f"data_{i+11}.csv")
#         sample.to_csv(file_path, index=False)
#         print(f"Created file: {file_path}")

# if __name__ == "__main__":

#     dataset_path = "C:/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/data/datasets/spotify_dataset.csv"
#     raw_data_folder = "C:/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/data/raw_data"
#     num_files = 10 

#     split_dataset(dataset_path, raw_data_folder, num_files)
# #---------------------------------------------------------------------------------------------------------------#



import random
import pandas as pd
import os

def generate_music_data(num_rows=50):
    data = []
    genres = ["Pop", "Rock", "Hip Hop", "Electronic", "R&B", "Country", "Jazz", "Classical", "Folk", "Reggae"]
    
    for _ in range(num_rows):
        artist = f"Artist_{random.randint(1, 20)}"
        song = f"Song_{random.randint(1, 100)}"
        duration_ms = random.randint(120000, 360000)  # 2 to 6 minutes
        explicit = random.choice([True, False])
        year = random.randint(1990, 2024)
        popularity = random.randint(0, 100)
        danceability = round(random.uniform(0.0, 1.0), 3)
        energy = round(random.uniform(0.0, 1.0), 3)
        key = random.randint(0, 11)  # Representing pitch class, 0-11
        loudness = round(random.uniform(-60.0, 0.0), 3)  # dB
        mode = random.choice([0, 1])  # 0 for minor, 1 for major
        speechiness = round(random.uniform(0.0, 1.0), 3)
        acousticness = round(random.uniform(0.0, 1.0), 3)
        instrumentalness = round(random.uniform(0.0, 1.0), 3)
        liveness = round(random.uniform(0.0, 1.0), 3)
        valence = round(random.uniform(0.0, 1.0), 3)
        tempo = round(random.uniform(60.0, 200.0), 3) # BPM
        genre = random.choice(genres)
        
        data.append([
            artist, song, duration_ms, explicit, year, popularity, 
            danceability, energy, key, loudness, mode, speechiness, 
            acousticness, instrumentalness, liveness, valence, tempo, genre
        ])
    
    df = pd.DataFrame(data, columns=[
        "artist", "song", "duration_ms", "explicit", "year", "popularity", 
        "danceability", "energy", "key", "loudness", "mode", "speechiness", 
        "acousticness", "instrumentalness", "liveness", "valence", "tempo", "genre"
    ])
    return df

# Define the target directory
output_directory = "C:/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/data/raw_data"

# Create the directory if it doesn't exist
os.makedirs(output_directory, exist_ok=True)

# Generate and save 10 different files
num_files = 10
for i in range(1, num_files + 1):
    output_filename = f"music_data_{i}.csv"
    output_filepath = os.path.join(output_directory, output_filename)
    
    generated_data = generate_music_data(50) # Generate 50 rows for each file
    generated_data.to_csv(output_filepath, index=False)
    print(f"Successfully generated data and saved to: {output_filepath}")

print(f"\nCompleted generating {num_files} files in total.")