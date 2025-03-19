import streamlit as st
from PIL import Image

# Page Configuration
st.set_page_config(page_title="Our Team", page_icon="👥", layout="centered")

# Heading
st.title("Meet Our Team")

# Dummy Text
st.write("""
Welcome to our team page! We are a group of dedicated professionals working together
to bring you the best experience. Feel free to explore our app and learn more about us.
""")

# Team Member Photos in One Row
cols = st.columns(5)  # Create 5 equal columns

team_images = [
    "E:/Python tasks/streamlit/Music-Prediction-App/StreamLit/images/team1.jpg", 
    "E:/Python tasks/streamlit/Music-Prediction-App/StreamLit/images/team2.jpg", 
    "E:/Python tasks/streamlit/Music-Prediction-App/StreamLit/images/team3.jpg", 
    "E:/Python tasks/streamlit/Music-Prediction-App/StreamLit/images/team4.jpg", 
    "E:/Python tasks/streamlit/Music-Prediction-App/StreamLit/images/team5.jpg"
]

team_descriptions = [
    "<span style='color:blue;'>Lead Developer</span> - Vishal",
    "<span style='color:blue;'>Lead Developer</span> - Vivek",
    "<span style='color:blue;'>Project Manager</span> - Minhal Piyara",
    "<span style='color:blue;'>UI/UX Designer</span> - Hassan Budha",
    "<span style='color:blue;'>Marketing Head</span> - Khurrum"
]

def process_image(image_path, target_size=(300, 300)):
    image = Image.open(image_path)
    image = image.resize(target_size, Image.LANCZOS)  # Resize to fixed dimensions
    return image

# Display team members with images and descriptions
for i in range(5):
    with cols[i]:
        st.image(process_image(team_images[i]), use_container_width=True)
        st.markdown(team_descriptions[i], unsafe_allow_html=True)  # Use markdown to render HTML

# Button to Navigate to Main App
if st.button("Explore the App"):
    st.switch_page("main.py")  # Ensure you have multi-page functionality set up
