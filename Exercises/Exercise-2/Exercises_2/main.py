import requests
import pandas as pd
from bs4 import BeautifulSoup

def get_file_url():
    url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
    response = requests.get(url)

    if response.status_code != 200:
        print("Failed to retrieve the webpage.")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    rows = soup.find_all("tr")

    for row in rows:
        cols = row.find_all("td")
        if len(cols) >= 2:
            modified = cols[1].text.strip()
            if modified == "2024-01-19 10:27":
                link = cols[0].find("a")
                if link and link['href'].endswith('.csv'):
                    return url + link['href']
    
    return None

def download_file(file_url, output_path="weather.csv"):
    response = requests.get(file_url)
    if response.status_code == 200:
        with open(output_path, "wb") as f:
            f.write(response.content)
        print(f" Đã tải file: {output_path}")
        return output_path
    else:
        print(" Lỗi khi tải file.")
        return None

def find_max_temperature(filepath):
    try:
        df = pd.read_csv(filepath)
        if 'HourlyDryBulbTemperature' not in df.columns:
            print(" Cột 'HourlyDryBulbTemperature' không tồn tại trong dữ liệu.")
            return
        max_temp = df['HourlyDryBulbTemperature'].max()
        max_rows = df[df['HourlyDryBulbTemperature'] == max_temp]
        print(" Dữ liệu có nhiệt độ cao nhất:")
        print(max_rows)
    except Exception as e:
        print(" Lỗi khi xử lý file CSV:", e)

def main():
    file_url = get_file_url()
    if not file_url:
        print(" Không tìm thấy file với thời gian yêu cầu.")
        return
    
    filepath = download_file(file_url)
    if filepath:
        find_max_temperature(filepath)

if __name__ == "__main__":
    main()
