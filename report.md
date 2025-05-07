### Báo cáo Lab 8
### Case-study 1: Xây dựng pipeline tự động cào và trực quan dữ liệu 
Phương pháp: Sử dụng mô hình ETL (Extract, Transform, Load) để thực hiện và sử dụng AirFlow để tự động hóa pipeline <br>
Các bước: <br?
B1: Thu thập dữ liệu (file csv, mysql) <br>
B2: Viết script Python để xử lí trích xuất dữ liệu <br>
B3: Tải (Load) dữ liệu lên phần mềm PostgreSQL + Pandas <br>
B4: Trực quan hóa trên PowerBI <br>


![image](https://github.com/user-attachments/assets/c89b1d4b-aff0-4c3b-966a-eb685d85e314)
![image](https://github.com/user-attachments/assets/0a58dbe6-6842-4ac8-ace5-6ddf6aa34670)

Đây là bức ảnh mô tả về Data Warehouse (Kho dữ liệu), với các bước từ nguồn dữ liệu ban đầu cho đến tầng cuối cùng là tầng ngữ nghĩa (semantic). Dưới đây là giải thích chi tiết từng thành phần: <br>
1. Data Source: Đây là nơi dữ liệu bắt đầu (file CSV, file log, sql,…) <br>
2. Landing: lưu trữ tạm thời trước khi xử lí, dùng để kiểm tra và xác minh <br>
3. Staging: Dữ liệu sẽ được làm sạch, chuẩn hóa và chuẩn bị đưa vào kho dữ liệu <br>
4. Dim – Fact – Agg (Bảng chiều – Bảng sự kiện – Tổng hợp) <br>
5. Semantic: nơi các công cụ PowerBI hoặc ngươci dùng truy cập <br>

Đây là cấu trúc thư mục của một dự án ETL & Data Warehouse cho E-commerce sử dụng Airflow và Python. Dưới đây là giải thích chi tiết cho từng phần: <br>
1. config/: Có thể chứa các file cấu hình (config) như thông tin kết nối database, đường dẫn, biến môi trường,.. <br>
2. dags/: Chứa các DAGs dùng cho AirFlow. <br>
Các file DAGs gồm: <br> 
(+) e_commerce_dw_dag.py <br> 
(+) extract_data.py <br>
(+) transform_dim_*.py <br>
(+) transform_fact_orders.py <br>
3. dataset/: Chứa dữ liệu đầu vào (có thể là .csv, .sql) <br>
4. logs/: thư mục mặc định của AirFlow chạy các task <br>
5. plugin/: Dùng để mở rộng AirFlow bằng cách tạo ra các custom operator <br>
Các file plugin gồm:  <br>
(+) mysql_operator.py (Operator tùy chỉnh để tương tác với MySQL) <br>
(+) postgresql_operator.py (Operator tùy chỉnh để tương tác với PostgreSQL) <br>
(+) support_processing.py <br>



### Tổng kết
Dự án này mô phỏng một hệ thống ETL hoàn chỉnh cho e-commerce:<br> 
Trích xuất dữ liệu từ file/sql → xử lý (biến đổi) → nạp vào warehouse (Postgres/MySQL). <br> 
Sử dụng Apache Airflow để lên lịch và theo dõi quá trình xử lý. <br> 
Có hỗ trợ plugin mở rộng cho các database phổ biến. <br>

### Case-study 2: Xây dựng pipeline tự động cào dữ liệu và huấn luyện mô hình
Mục tiêu: Xây dựng và chạy thử được các DAGs cơ bản đến nâng cao <br>
Công cụ: Sử dụng AirFlow để chạy pipeline với cài đặt thời gian <br>
Cấu trúc mô tả của 1 AirFlow <br>
![image](https://github.com/user-attachments/assets/1c40c9fc-d74c-47f6-9b61-5d852e5b6f5a)

![image](https://github.com/user-attachments/assets/64ebbf9c-180e-4f88-8670-582ca8d6a4b5)

### Tổng kết 
Hiểu định nghĩa và cách hoạt động của DAGs, các Operators <br>
Nắm rõ được các ví dụ về việc khởi tạo và chạy thử pipeline <br>
Hiểu rõ hơn về việc huấn luyện model  trả về kết quả sau khi chạy <br>


### Báo cáo Lab 9
### Exercise 1: Downloading Files with Python.
Mục tiêu <br> 
Trong bài tập này, mục tiêu là tải về các tệp từ các URL HTTP được cung cấp, giải nén các tệp zip, và lưu chúng vào thư mục downloads dưới dạng tệp CSV. Quá trình này cũng yêu cầu việc sử dụng thư viện Python như requests để tải tệp và zipfile để giải nén. <br>
Các công cụ sử dụng <br>
requests: Để tải các tệp từ các URL HTTP. <br>
zipfile: Để giải nén các tệp zip. <br>
os: Để tạo thư mục nếu nó chưa tồn tại. <br>
asyncio và aiohttp (tuỳ chọn nâng cao): Để tải các tệp đồng thời (asynchronously) nhằm cải thiện hiệu suất. <br>
ThreadPoolExecutor (tuỳ chọn nâng cao): Để thực hiện tải tệp song song, tận dụng nhiều luồng. <br>
Các bước thực hiện: <br>
Bước 1: Tạo thư mục downloads <br>
Bước 2: Tải tệp từ URL <br>
Bước 3: Giải nén tệp zip <br>
Bước 4: Kết hợp các bước và thực hiện tải và giải nén <br>
Bước 5: Nâng cao với Async và ThreadPoolExecutor <br>
Để cải thiện hiệu suất, ta có thể sử dụng aiohttp và asyncio để tải các tệp đồng thời.  <br>

### Exercise 2: WebScraping and File Downloading with Python. 
Mục tiêu <br>
Trong bài tập này, mục tiêu là tải về tệp dữ liệu thời tiết từ một trang web của chính phủ, sử dụng kỹ thuật WebScraping để tìm tệp dữ liệu dựa trên dấu thời gian "Last Modified". Sau đó, tải tệp xuống, mở nó bằng Pandas và tìm các bản ghi có nhiệt độ cao nhất, cụ thể là thuộc tính HourlyDryBulbTemperature. <br>
Các công cụ sử dụng <br>
requests: Để tải trang HTML. <br>
BeautifulSoup: Để phân tích và trích xuất thông tin từ HTML (WebScraping). <br>
pandas: Để xử lý và phân tích dữ liệu trong tệp CSV. <br>
os: Để lưu tệp tải xuống. <br>
Các bước thực hiện <br>
Bước 1: Web Scraping trang HTML để tìm tệp phù hợp <br>
Bước 2: Tải tệp về và lưu tệp vào thư mục <br>
Bước 3: Mở tệp với Pandas và tìm bản ghi có nhiệt độ cao nhất <br>
Bước 4: Kết hợp các bước trên trong hàm main <br>
Bước 5: Chạy mã <br>

### Exercise 3: Boto3 AWS + s3 + Python. 
Mục tiêu: <br>
(+) Exercise 3 hướng dẫn bạn cách làm việc với AWS S3 thông qua thư viện boto3 trong Python. <br> 
(+) Nhiệm vụ chính của bài tập này là tải một file từ S3, giải nén file .gz và tải một file khác từ URL bên trong file đó. <br> 
(+) Cụ thể, bạn cần tải một file .gz từ S3 chứa một danh sách các file (theo đường dẫn của chúng). Sau khi tải và giải nén file này, bạn sẽ lấy đường dẫn đến file .wet đầu tiên và tiếp tục tải nó về từ S3 rồi in các dòng trong file đó ra. <br> 
Vấn đề gặp phải: <br> 
(+) Khi chạy ứng dụng trong Docker, bạn gặp phải lỗi AccessDenied khi sử dụng boto3 để truy cập S3, mặc dù dữ liệu Common Crawl thường là công khai. <br>
(+) Giải pháp là tải file trực tiếp qua HTTP(S) thay vì dùng boto3 để tránh vấn đề quyền truy cập. <br>
 
### Exercise 4: Convert JSON to CSV + Ragged Directories.
Mục tiêu <br> 
Trong bài tập này, mục tiêu là tìm tất cả các tệp JSON trong thư mục dữ liệu (data) và sau đó chuyển đổi các tệp JSON đó thành tệp CSV. Quá trình này yêu cầu: <br>
Duyệt qua cấu trúc thư mục để tìm tất cả các tệp JSON. <br>
Đọc các tệp JSON và giải nén dữ liệu từ cấu trúc JSON. <br>
Chuyển đổi dữ liệu JSON thành dạng phẳng và ghi vào tệp CSV. <br>
Các công cụ sử dụng <br>
Để hoàn thành bài tập này, tôi đã sử dụng các gói Python sau: <br>
glob: Để tìm các tệp trong một thư mục và cấu trúc con. <br>
json: Để đọc và xử lý các tệp JSON. <br>
csv: Để chuyển đổi dữ liệu JSON thành CSV và ghi vào tệp <br>

### Exercise 5: Data Modeling for Postgres + Python. 
Mục tiêu <br>
Trong bài tập này, tôi sẽ thực hành các kỹ năng liên quan đến mô hình dữ liệu, lập trình Python và làm việc với cơ sở dữ liệu Postgres. Mục tiêu là tạo các câu lệnh CREATE SQL cho các bảng dựa trên dữ liệu trong các tệp CSV, sau đó sử dụng psycopg2 để kết nối đến Postgres và thực thi các câu lệnh SQL để tạo bảng và nhập dữ liệu. <br>
Các công cụ sử dụng <br>
psycopg2: Thư viện Python để kết nối và tương tác với cơ sở dữ liệu Postgres. <br>
PostgreSQL: Cơ sở dữ liệu quan hệ để lưu trữ dữ liệu. <br>
CSV: Tệp dữ liệu cần được chuyển đổi thành bảng trong cơ sở dữ liệu. <br>
Các bước thực hiện <br>
Bước 1: Phân tích các tệp CSV <br>
Bước 2: Thiết kế câu lệnh SQL CREATE TABLE <br>
Bước 3: Kết nối với Postgres sử dụng psycopg2 <br>
Bước 4: Nhập dữ liệu từ các tệp CSV vào bảng <br>
Bước 5: Kiểm tra và hoàn thanh <br>
 
### Exercise 6: Ingestion and Aggregation with PySpark.
Mục tiêu <br>
Trong bài tập này, tôi làm việc với PySpark, một công cụ quan trọng trong hệ sinh thái Big Data, để phân tích dữ liệu hành trình xe đạp từ các tệp CSV được nén (.zip). Mục tiêu là sử dụng PySpark để:
Đọc dữ liệu nén. <br>
Thực hiện các phép tính thống kê. <br>
Xuất kết quả ra thư mục reports/ dưới dạng CSV. <br>
Công cụ sử dụng  <br>
Apache Spark (thông qua PySpark) <br>
Python (trong Docker) <br>
Docker/Docker Compose (để thiết lập môi trường chạy Spark không phụ thuộc máy chủ) <br>
CSV / ZIP File Handling <br>
Vấn đề gặp phải <br>
Chưa không thể cài đặt Java trên máy tính cá nhân, dẫn đến việc không thể chạy được Spark cục bộ ngoài Docker. Spark yêu cầu Java để hoạt động (thường là Java 8 hoặc 11) <br>

### Exercise 7: Using Various PySpark Functions
Mục tiêu <br>
Bài tập này yêu cầu xử lý và phân tích dữ liệu lỗi ổ cứng từ file .zip chứa CSV với PySpark bằng các hàm có sẵn trong pyspark.sql.functions mà không được sử dụng UDF hoặc phương thức Python.
Công cụ và công nghệ sử dụng <br>
PySpark <br>
Spark SQL Functions (from pyspark.sql.functions import *) <br>
Docker / Docker Compose (thiết lập môi trường giả lập Spark) <br>
Zip handling + CSV parsing <br>
DataFrame transformations <br>











