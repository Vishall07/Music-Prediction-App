# Music Prediction App

Architectural Diagram
![project-architecture](https://github.com/user-attachments/assets/50b238a6-168b-4459-a140-f7e4f57b1174)



This project predicts music preferences using machine learning and provides various features, including:

- Airflow-based workflow management
- FastAPI backend for prediction and data handling
- Streamlit frontend for interactive user experience

## Installation

### 1. **Install Docker**

Ensure you have Docker and Docker Compose installed. You can download them from the official [Docker website](https://docs.docker.com/get-docker/).

### 2. **Run Airflow in Docker**

To run Apache Airflow in Docker, follow these steps:

docker-compose -f docker-compose-airflow.yml up -d

2. Access the Airflow UI at http://localhost:8080. Default login credentials are:

Username: admin
Password: admin


### 3. **Run FastAPI Backend**


1. Clone this repository:
   ```bash
   git clone https://github.com/Vishall07/Music-Prediction-App.git
   cd Music-Prediction-App

To run the FastAPI backend:
Navigate to the FastAPI directory:


cd fastapi
Build and run the FastAPI app:


docker-compose up -d --build
Access the FastAPI application at http://localhost:8000.


### 4. **Run Streamlit App**

To run the Streamlit app:

Navigate to the Streamlit directory:


cd streamlit
Build and run the Streamlit app:

docker-compose up -d --build
Access the Streamlit app at http://localhost:8501.