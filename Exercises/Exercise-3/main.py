import requests
import gzip
import io

def main():
    # URL file wet.paths.gz
    url = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-50/wet.paths.gz"
    print(f"Downloading {url}...")
    response = requests.get(url)
    response.raise_for_status()

    # Mở file .gz trong bộ nhớ và đọc dòng đầu tiên
    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
        first_path = gz.readline().decode('utf-8').strip()
    
    print(f"First WET file path: {first_path}")

    # Tạo URL đến file thực tế cần tải
    wet_file_url = f"https://data.commoncrawl.org/{first_path}"
    print(f"Downloading {wet_file_url}...")

    # Tải file thực tế và in từng dòng (stream)
    with requests.get(wet_file_url, stream=True) as wet_response:
        wet_response.raise_for_status()
        for line in wet_response.iter_lines():
            print(line.decode('utf-8'))

if __name__ == "__main__":
    main()
