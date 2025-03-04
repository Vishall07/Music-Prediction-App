
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
import joblib

# Load the dataset
df = pd.read_csv("C:/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/data/raw/spotify_dataset.csv")

# Define features and target
X = df.drop(columns=["popularity"])  # Features
y = df["popularity"]  # Target

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Preprocessing pipeline
numeric_features = ["duration_ms", "year", "danceability", "energy", "key", "loudness", "mode", 
                    "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo"]
categorical_features = ["artist", "song", "genre"]

numeric_transformer = Pipeline(steps=[
    ("scaler", StandardScaler())
])

categorical_transformer = Pipeline(steps=[
    ("onehot", OneHotEncoder(handle_unknown="ignore"))
])

preprocessor = ColumnTransformer(
    transformers=[
        ("num", numeric_transformer, numeric_features),
        ("cat", categorical_transformer, categorical_features)
    ])

# Model pipeline
model = Pipeline(steps=[
    ("preprocessor", preprocessor),
    ("regressor", RandomForestRegressor(random_state=42))
])

# Train the model
model.fit(X_train, y_train)

# Evaluate the model
y_pred = model.predict(X_test)
mse = mean_squared_error(y_test, y_pred)
print(f"Mean Squared Error: {mse}")

# Save the model
joblib.dump(model, "C:/Users/hassa/Desktop/DSP/dsp-hassan-riaz-khan/models/model.pkl")
print("Model saved to models/model.pkl")