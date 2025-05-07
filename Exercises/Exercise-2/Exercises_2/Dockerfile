# Sử dụng image chính thức của Python
FROM python:3.9-slim

# Đặt thư mục làm việc trong container
WORKDIR /app

# Sao chép mã nguồn vào container
COPY . /app

# Cài đặt các yêu cầu
RUN pip install --no-cache-dir -r requirements.txt

# Chạy ứng dụng chính
CMD ["python", "main.py"]
