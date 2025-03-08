import streamlit as st
import pandas as pd

st.title("📊 Past Prediction Page")

start_date = st.date_input("Start Date")
end_date = st.date_input("End Date")

prediction_source = st.selectbox(
    "Select Prediction Source",
    ["All", "WebApp", "Scheduled Predictions"]
)

# Past prediction parameters
data = {
    "Date": pd.date_range(start="2024-02-01", periods=10, freq="D"),
    "Source": ["WebApp", "Scheduled Predictions"] * 5,
    "Prediction": ["Positive", "Negative"] * 5
}
df = pd.DataFrame(data)

# date filters part
filtered_df = df[
    (df["Date"] >= pd.to_datetime(start_date)) &
    (df["Date"] <= pd.to_datetime(end_date))
]

if prediction_source != "All":
    filtered_df = filtered_df[filtered_df["Source"] == prediction_source]

# Resuts displayed
st.write("### Filtered Past Predictions")
st.dataframe(filtered_df)

# Past Prediction Button
if st.button("Past Prediction"):
    st.write("You clicked the Past Prediction button!")
