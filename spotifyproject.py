import os
import requests
from datetime import datetime
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv


### environment variables
# SPOTIFY_API_KEY

# Load environment variables from a .env file
load_dotenv()

# Define and create a temporary directory path if not exists
tmp_dir = os.path.join(os.getcwd(), "tmp")
os.makedirs(tmp_dir, exist_ok=True)

# API endpoint
SPOTIFY_API_URL = "https://api.spotify.com/v1/artists"  
SPOTIFY_API_KEY = os.getenv("SPOTIFY_API_KEY")

# Spotify artist ids
ids = {"23fqKkggKUBHNkbKtXEls4","1Cs0zKBU1kc0i8ypK3B9ai","07lpuAglRqTvLjElXLCVbW"}

# Function to fetch artist data from Spotify API
def fetch_artist_data():
    headers = {"Authorization": f"Bearer {SPOTIFY_API_KEY}" }
    params = {"ids":",".join(ids)}
    response = requests.get(SPOTIFY_API_URL, headers=headers, params = params)
    print(response.url)
    response.raise_for_status()
    return response.json()

# Function to transform data
def transform_data(raw_data,ids):
    transformed = []
    for i in range(ids):
        artist = {
            "name": raw_data["artists"][i]["name"],
            "type": raw_data["artists"][i]["type"],
            "popularity": raw_data["artists"][i]["popularity"],
            "followers": raw_data["artists"][i]["followers"]["total"],
            "timestamp": datetime.now().isoformat()
        }
        transformed.append(artist)
    return transformed

# Function to convert data to CSV
def convert_to_csv(data):
    df = pd.DataFrame(data)  # Convert a list of dictionaries to a pandas DataFrame
    file_name = f"spotify_artists_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    csv_path = os.path.join(tmp_dir, file_name)  # Temporary directory to store the file locally
    df.to_csv(csv_path, index=False)
    return csv_path, file_name

# Main job function
def artist_data_job():
    try:
        raw_data = fetch_artist_data()
        transformed_data = transform_data(raw_data, len(ids))
        convert_to_csv(transformed_data)  # Convert to CSV
        print(f"Job completed successfully at {datetime.now()}")
    except Exception as e:
        print(f"Job failed: {str(e)}")

if __name__ == "__main__":
    print("Executing artist data fetch job...")
    artist_data_job()