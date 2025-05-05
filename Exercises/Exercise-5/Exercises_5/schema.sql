-- Tạo bảng customers
CREATE TABLE customers (
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

-- Tạo chỉ mục cho bảng customers
CREATE INDEX idx_customers_zip_code ON customers(zip_code);

-- Tạo bảng products
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_code VARCHAR(20),
    product_description VARCHAR(255)
);

-- Tạo chỉ mục cho bảng products
CREATE INDEX idx_products_product_code ON products(product_code);

-- Tạo bảng transactions
CREATE TABLE transactions (
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

-- Tạo chỉ mục cho bảng transactions
CREATE INDEX idx_transactions_account_id ON transactions(account_id);
