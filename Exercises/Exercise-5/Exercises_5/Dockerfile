# Dockerfile
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy all project files
COPY . .

# Cài đặt pip packages
RUN pip install --upgrade pip \
    && pip install psycopg2-binary

# Lệnh mặc định (nếu cần)
CMD ["python", "main.py"]
