import os
import requests
import zipfile
import aiohttp
import asyncio
from concurrent.futures import ThreadPoolExecutor
import unittest
from pathlib import Path

# Danh sách URI tải xuống
download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2022_Q1.zip",  # Có thể không hợp lệ
]

DOWNLOAD_DIR = "downloads"

def create_download_directory():
    """Tạo thư mục downloads nếu chưa tồn tại."""
    Path(DOWNLOAD_DIR).mkdir(exist_ok=True)

def get_filename_from_url(url: str) -> str:
    """Trích xuất tên file từ URL."""
    return url.split('/')[-1]

def download_file(url: str) -> bool:
    """Tải một file, giải nén CSV và xóa file zip."""
    try:
        filename = get_filename_from_url(url)
        zip_path = os.path.join(DOWNLOAD_DIR, filename)
        csv_filename = filename.replace('.zip', '.csv')
        csv_path = os.path.join(DOWNLOAD_DIR, csv_filename)

        # Bỏ qua nếu file CSV đã tồn tại
        if os.path.exists(csv_path):
            print(f"CSV {csv_filename} đã tồn tại, bỏ qua...")
            return True

        # Tải file
        print(f"Đang tải {filename}...")
        response = requests.get(url, stream=True)
        if response.status_code != 200:
            print(f"Tải {filename} thất bại. Mã trạng thái: {response.status_code}")
            return False

        # Lưu file zip
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        # Giải nén file zip
        print(f"Đang giải nén {filename}...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Tìm file CSV trong zip
            csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
            if not csv_files:
                print(f"Không tìm thấy CSV trong {filename}")
                os.remove(zip_path)
                return False
            zip_ref.extract(csv_files[0], DOWNLOAD_DIR)
            # Đổi tên file CSV nếu cần
            extracted_path = os.path.join(DOWNLOAD_DIR, csv_files[0])
            if extracted_path != csv_path:
                os.rename(extracted_path, csv_path)

        # Xóa file zip
        os.remove(zip_path)
        print(f"Xử lý thành công {filename}")
        return True

    except (requests.RequestException, zipfile.BadZipFile) as e:
        print(f"Lỗi khi xử lý {url}: {str(e)}")
        if os.path.exists(zip_path):
            os.remove(zip_path)
        return False

async def download_file_async(url: str, session: aiohttp.ClientSession) -> bool:
    """Tải một file bất đồng bộ, giải nén CSV và xóa file zip."""
    try:
        filename = get_filename_from_url(url)
        zip_path = os.path.join(DOWNLOAD_DIR, filename)
        csv_filename = filename.replace('.zip', '.csv')
        csv_path = os.path.join(DOWNLOAD_DIR, csv_filename)

        # Bỏ qua nếu file CSV đã tồn tại
        if os.path.exists(csv_path):
            print(f"CSV {csv_filename} đã tồn tại, bỏ qua...")
            return True

        # Tải file
        print(f"Đang tải {filename} (bất đồng bộ)...")
        async with session.get(url) as response:
            if response.status != 200:
                print(f"Tải {filename} thất bại. Mã trạng thái: {response.status}")
                return False

            # Lưu file zip
            with open(zip_path, 'wb') as f:
                while True:
                    chunk = await response.content.read(8192)
                    if not chunk:
                        break
                    f.write(chunk)

        # Giải nén file zip
        print(f"Đang giải nén {filename}...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
            if not csv_files:
                print(f"Không tìm thấy CSV trong {filename}")
                os.remove(zip_path)
                return False
            zip_ref.extract(csv_files[0], DOWNLOAD_DIR)
            extracted_path = os.path.join(DOWNLOAD_DIR, csv_files[0])
            if extracted_path != csv_path:
                os.rename(extracted_path, csv_path)

        # Xóa file zip
        os.remove(zip_path)
        print(f"Xử lý thành công {filename} (bất đồng bộ)")
        return True

    except (aiohttp.ClientError, zipfile.BadZipFile) as e:
        print(f"Lỗi khi xử lý {url}: {str(e)}")
        if os.path.exists(zip_path):
            os.remove(zip_path)
        return False

def download_files_threadpool(urls: list) -> list:
    """Tải file đồng thời sử dụng ThreadPoolExecutor."""
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(download_file, urls))
    return results

async def download_files_async(urls: list) -> list:
    """Tải file bất đồng bộ sử dụng aiohttp."""
    async with aiohttp.ClientSession() as session:
        tasks = [download_file_async(url, session) for url in urls]
        return await asyncio.gather(*tasks)

def main():
    """Hàm chính để điều phối quá trình tải."""
    create_download_directory()
    
    # Tải đồng bộ
    print("Bắt đầu tải đồng bộ...")
    for url in download_uris:
        download_file(url)
    
    # Tải với ThreadPoolExecutor
    print("\nBắt đầu tải với ThreadPoolExecutor...")
    download_files_threadpool(download_uris)
    
    # Tải bất đồng bộ
    print("\nBắt đầu tải bất đồng bộ...")
    asyncio.run(download_files_async(download_uris))

# Unit Test
class TestDownloadScript(unittest.TestCase):
    def setUp(self):
        create_download_directory()
        self.test_url = "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip"
        self.invalid_url = "https://divvy-tripdata.s3.amazonaws.com/Invalid_File.zip"

    def test_create_directory(self):
        self.assertTrue(os.path.exists(DOWNLOAD_DIR))

    def test_get_filename(self):
        self.assertEqual(get_filename_from_url(self.test_url), "Divvy_Trips_2018_Q4.zip")

    def test_invalid_url(self):
        result = download_file(self.invalid_url)
        self.assertFalse(result)

    def test_csv_exists(self):
        download_file(self.test_url)
        csv_path = os.path.join(DOWNLOAD_DIR, "Divvy_Trips_2018_Q4.csv")
        self.assertTrue(os.path.exists(csv_path))

if __name__ == "__main__":
    main()
    # Chạy unit test
    unittest.main(argv=[''], exit=False)