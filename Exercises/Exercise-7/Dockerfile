# Dockerfile

FROM openjdk:11

# Cài đặt các công cụ cần thiết
RUN apt-get update && apt-get install -y python3 python3-pip unzip

# Cài đặt pyspark
RUN pip3 install pyspark

# Tạo thư mục làm việc
WORKDIR /app

# Copy tất cả file vào container
COPY . /app

# Chạy lệnh mặc định (nếu muốn)
CMD ["python3", "main.py"]
