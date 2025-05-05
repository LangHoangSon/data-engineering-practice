import requests
import os

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def main():
    # Create a directory to store downloads if it doesn't exist
    download_dir = "divvy_tripdata"
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    
    for uri in download_uris:
        # Extract filename from URI
        filename = uri.split("/")[-1]
        file_path = os.path.join(download_dir, filename)
        
        # Skip if file already exists
        if os.path.exists(file_path):
            print(f"File {filename} already exists, skipping...")
            continue
            
        try:
            print(f"Downloading {filename}...")
            # Send GET request
            response = requests.get(uri, stream=True)
            
            # Check if request was successful
            if response.status_code == 200:
                # Save the file
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                print(f"Successfully downloaded {filename}")
            else:
                print(f"Failed to download {filename}. Status code: {response.status_code}")
                
        except requests.RequestException as e:
            print(f"Error downloading {filename}: {str(e)}")

if __name__ == "__main__":
    main()
