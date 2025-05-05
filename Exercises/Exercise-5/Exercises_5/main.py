import psycopg2
import csv
# Kết nối đến PostgreSQL

def connect_to_db():
    conn = psycopg2.connect(
        dbname="exercise_db",
        user="postgres",
        password="postgres",
        host="localhost",   # CHỈNH Ở ĐÂY
        port="5433"         # CHỈNH Ở ĐÂY
    )
    return conn


# Tạo bảng
def create_tables(conn):
    cursor = conn.cursor()
    
    # Tạo bảng customers
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INT PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            address_1 VARCHAR(255),
            address_2 VARCHAR(255),
            city VARCHAR(100),
            state VARCHAR(50),
            zip_code VARCHAR(20),
            join_date DATE
        );
        CREATE INDEX IF NOT EXISTS idx_customers_zip_code ON customers(zip_code);
    """)
    
    # Tạo bảng products
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id INT PRIMARY KEY,
            product_code VARCHAR(20),
            product_description VARCHAR(255)
        );
        CREATE INDEX IF NOT EXISTS idx_products_product_code ON products(product_code);
    """)
    
    # Tạo bảng transactions
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id VARCHAR(50) PRIMARY KEY,
            transaction_date DATE,
            product_id INT,
            product_code VARCHAR(20),
            product_description VARCHAR(255),
            quantity INT,
            account_id INT,
            FOREIGN KEY (product_id) REFERENCES products(product_id),
            FOREIGN KEY (account_id) REFERENCES customers(customer_id)
        );
        CREATE INDEX IF NOT EXISTS idx_transactions_account_id ON transactions(account_id);
    """)
    
    conn.commit()

# Chèn dữ liệu từ CSV vào bảng
def insert_data_from_csv(conn, table_name, csv_file):
    cursor = conn.cursor()
    with open(csv_file, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Bỏ qua dòng tiêu đề
        
        for row in reader:
            try:
                # Chèn dữ liệu vào bảng tương ứng
                if table_name == "customers":
                    cursor.execute("""
                        INSERT INTO customers (customer_id, first_name, last_name, address_1, address_2, city, state, zip_code, join_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (customer_id) DO NOTHING  -- Bỏ qua khi trùng khóa chính
                    """, row)
                elif table_name == "products":
                    cursor.execute("""
                        INSERT INTO products (product_id, product_code, product_description)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (product_id) DO NOTHING  -- Bỏ qua khi trùng khóa chính
                    """, row)
                elif table_name == "transactions":
                    cursor.execute("""
                        INSERT INTO transactions (transaction_id, transaction_date, product_id, product_code, product_description, quantity, account_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (transaction_id) DO NOTHING  -- Bỏ qua khi trùng khóa chính
                    """, row)
            except Exception as e:
                print(f"Error inserting row: {e}")
                
    conn.commit()



def main():
    # Kết nối đến cơ sở dữ liệu
    conn = connect_to_db()
    
    # Tạo các bảng
    create_tables(conn)
    
    # Chèn dữ liệu từ CSV vào các bảng
    insert_data_from_csv(conn, "customers", "data/customers.csv")
    insert_data_from_csv(conn, "products", "data/products.csv")
    insert_data_from_csv(conn, "transactions", "data/transactions.csv")
    
    # Đóng kết nối
    conn.close()

if __name__ == "__main__":
    main()
