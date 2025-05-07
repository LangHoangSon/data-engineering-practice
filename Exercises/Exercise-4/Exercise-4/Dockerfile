FROM python:latest

# Chỉ định thư mục làm việc trong container là /app
WORKDIR /app

# Copy tất cả tệp trong thư mục hiện tại vào thư mục làm việc trong container
COPY . /app

# Cài đặt các thư viện từ requirements.txt
RUN python3 -m pip install -r requirements.txt
