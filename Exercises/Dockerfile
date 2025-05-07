FROM ubuntu:18.04

# Cập nhật hệ thống và cài đặt các gói cơ bản, Python 3.8, pip3, Java, Scala, ...
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    apt-get install -y default-jdk scala wget vim curl unzip libpq-dev \
    build-essential libssl-dev libffi-dev python3.8 python3.8-dev python3-pip && \
    apt-get clean

# Tạo symlink để đảm bảo 'python' trỏ tới python3.8
RUN ln -s /usr/bin/python3.8 /usr/bin/python

# Nâng cấp pip và các công cụ liên quan để tránh lỗi cài package
RUN python3.8 -m pip install --upgrade pip setuptools wheel

# Cài pyspark (chú ý dùng pip3 hoặc python3.8 -m pip đều được)
RUN python3.8 -m pip install pyspark==3.3.1 --index-url https://pypi.org/simple

# Cài đặt các thư viện Python cần thiết từ requirements.txt
COPY requirements.txt /app/requirements.txt
RUN python3.8 -m pip install markupsafe==1.1.1 cryptography==3.3.2 cython==0.29.21 numpy==1.18.5 && \
    python3.8 -m pip install -r /app/requirements.txt

# Cài đặt Spark và các thư viện Hadoop cần thiết
RUN wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar xvf spark-3.5.0-bin-hadoop3.tgz && \
    mv spark-3.5.0-bin-hadoop3/ /usr/local/spark && \
    ln -s /usr/local/spark spark && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.890/aws-java-sdk-bundle-1.11.890.jar && \
    mv aws-java-sdk-bundle-1.11.890.jar /usr/local/spark/jars && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar && \
    mv hadoop-aws-3.2.0.jar /usr/local/spark/jars

# Đặt thư mục làm việc
WORKDIR /app

# Sao chép toàn bộ mã nguồn vào container
COPY . /app

# Đặt các biến môi trường cho PySpark
ENV PYSPARK_PYTHON=python3.8
ENV PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-core_2.12:0.8.0 pyspark-shell"

# Chạy ứng dụng
CMD ["python3.8", "main.py"]
