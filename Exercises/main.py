import os
import zipfile
import hashlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def extract_zip_file(zip_path, extract_to="data"):
    if not os.path.exists(extract_to):
        os.makedirs(extract_to)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

def init_spark():
    return SparkSession.builder \
        .appName("Bai7 CSV Processor") \
        .getOrCreate()

def process_data(spark):
    zip_file_path = "data/hard-drive-2022-01-01-failures.csv.zip"
    extract_zip_file(zip_file_path, "data")

    # Tìm tên file CSV sau khi giải nén
    csv_file_name = [f for f in os.listdir("data") if f.endswith(".csv")][0]
    csv_path = os.path.join("data", csv_file_name)

    # Đọc dữ liệu
    df = spark.read.option("header", True).csv(csv_path)

    # 1. Thêm cột source_file
    df = df.withColumn("source_file", lit(csv_file_name))

    # 2. Trích xuất file_date từ tên file
    df = df.withColumn("file_date",
        to_date(regexp_extract(col("source_file"), r"(\d{4}-\d{2}-\d{2})", 1), "yyyy-MM-dd")
    )

    # 3. Tạo cột brand từ model
    df = df.withColumn("brand",
        when(col("model").contains(" "), split(col("model"), " ")[0]).otherwise("unknown")
    )

    # 4. Xếp hạng model theo capacity
    model_capacity_df = df.groupBy("model") \
        .agg(max(col("capacity_bytes").cast("long")).alias("max_capacity"))
    
    window_spec = Window.orderBy(col("max_capacity").desc())
    model_capacity_df = model_capacity_df.withColumn("storage_ranking", dense_rank().over(window_spec))

    df = df.join(model_capacity_df.select("model", "storage_ranking"), on="model", how="left")

    # 5. Tạo primary_key từ các cột duy nhất
    def hash_row(*cols):
        return hashlib.sha256("||".join([str(c) for c in cols]).encode()).hexdigest()
    hash_udf = udf(hash_row)

    df = df.withColumn("primary_key", hash_udf(col("model"), col("capacity_bytes"), col("file_date")))

    # In kết quả
    df.show(10, truncate=False)

def main():
    spark = init_spark()
    process_data(spark)

if __name__ == "__main__":
    main()
