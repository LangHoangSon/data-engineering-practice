Báo cáo Lab 8
Case-study 1: Xây dựng pipeline tự động cào và trực quan dữ liệu
Phương pháp: Sử dụng mô hình ETL (Extract, Transform, Load) để thực hiện và sử dụng AirFlow để tự động hóa pipeline
Các bước:
B1: Thu thập dữ liệu (file csv, mysql)
B2: Viết script Python để xử lí trích xuất dữ liệu
B3: Tải (Load) dữ liệu lên phần mềm PostgreSQL + Pandas
B4: Trực quan hóa trên PowerBI
![image](https://github.com/user-attachments/assets/c89b1d4b-aff0-4c3b-966a-eb685d85e314)
![image](https://github.com/user-attachments/assets/0a58dbe6-6842-4ac8-ace5-6ddf6aa34670)
Đây là bức ảnh mô tả về Data Warehouse (Kho dữ liệu), với các bước từ nguồn dữ liệu ban đầu cho đến tầng cuối cùng là tầng ngữ nghĩa (semantic). Dưới đây là giải thích chi tiết từng thành phần:
1. Data Source: Đây là nơi dữ liệu bắt đầu (file CSV, file log, sql,…)
2. Landing: lưu trữ tạm thời trước khi xử lí, dùng để kiểm tra và xác minh
3. Staging: Dữ liệu sẽ được làm sạch, chuẩn hóa và chuẩn bị đưa vào kho dữ liệu
4. Dim – Fact – Agg (Bảng chiều – Bảng sự kiện – Tổng hợp)
5. Semantic: nơi các công cụ PowerBI hoặc ngươci dùng truy cập
![image](https://github.com/user-attachments/assets/6435e6f9-29b2-42ad-86c7-0ca0e1d54c5b)
Đây là cấu trúc thư mục của một dự án ETL & Data Warehouse cho E-commerce sử dụng Airflow và Python. Dưới đây là giải thích chi tiết cho từng phần:
1. config/: Có thể chứa các file cấu hình (config) như thông tin kết nối database, đường dẫn, biến môi trường,..
2. dags/: Chứa các DAGs dùng cho AirFlow.
Các file DAGs gồm:
(+) e_commerce_dw_dag.py 
(+) extract_data.py
(+) transform_dim_*.py
(+) transform_fact_orders.py
3. dataset/: Chứa dữ liệu đầu vào (có thể là .csv, .sql)
4. logs/: thư mục mặc định của AirFlow chạy các task
5. plugin/: Dùng để mở rộng AirFlow bằng cách tạo ra các custom operator
Các file plugin gồm: 
(+) mysql_operator.py (Operator tùy chỉnh để tương tác với MySQL)
(+) postgresql_operator.py (Operator tùy chỉnh để tương tác với PostgreSQL)
(+) support_processing.py
![image](https://github.com/user-attachments/assets/a88e6d3d-f96c-4ae8-81f9-e1de5595f96d)


Tổng kết
Dự án này mô phỏng một hệ thống ETL hoàn chỉnh cho e-commerce:
Trích xuất dữ liệu từ file/sql → xử lý (biến đổi) → nạp vào warehouse (Postgres/MySQL).
Sử dụng Apache Airflow để lên lịch và theo dõi quá trình xử lý.
Có hỗ trợ plugin mở rộng cho các database phổ biến.
Case-study 2: Xây dựng pipeline tự động cào dữ liệu và huấn luyện mô hình
Mục tiêu: Xây dựng và chạy thử được các DAGs cơ bản đến nâng cao
Công cụ: Sử dụng AirFlow để chạy pipeline với cài đặt thời gian
![image](https://github.com/user-attachments/assets/ab6cd058-8a94-4a3d-9ee3-471ff54fd0bf)



