{{ config(materialized='table') }}

WITH source_data AS (
  SELECT
    CAST(column1 AS INTEGER) AS sale_id,
    CAST(column2 AS TEXT) AS sale_date,
    CAST(column3 AS REAL) AS sale_amount,
    CAST(column4 AS INTEGER) AS quantity,
    CAST(column5 AS INTEGER) AS customer_id,
    CAST(column6 AS TEXT) AS customer_name,
    CAST(column7 AS TEXT) AS customer_email,
    CAST(column8 AS TEXT) AS customer_city,
    CAST(column9 AS TEXT) AS customer_state,
    CAST(column10 AS INTEGER) AS product_id,
    CAST(column11 AS TEXT) AS product_name,
    CAST(column12 AS TEXT) AS product_category,
    CAST(column13 AS REAL) AS product_price
  FROM (
    VALUES
      -- January sales - Customer 1001 in Seattle
      (1, '2024-01-05', 129.99, 1, 1001, 'Alice Johnson', 'alice@example.com', 'Seattle', 'WA', 101, 'Laptop Stand', 'Accessories', 129.99),
      (2, '2024-01-12', 49.99, 1, 1002, 'Bob Smith', 'bob@example.com', 'Austin', 'TX', 102, 'Wireless Mouse', 'Accessories', 49.99),
      (3, '2024-01-18', 299.99, 1, 1003, 'Carol Williams', 'carol@example.com', 'Chicago', 'IL', 103, 'Monitor 27inch', 'Electronics', 299.99),
      (4, '2024-01-25', 49.99, 2, 1001, 'Alice Johnson', 'alice@example.com', 'Seattle', 'WA', 104, 'USB Cable', 'Accessories', 24.99),
      
      -- February sales
      (5, '2024-02-03', 1599.99, 1, 1004, 'David Brown', 'david@example.com', 'Denver', 'CO', 105, 'Laptop Pro', 'Electronics', 1599.99),
      (6, '2024-02-14', 149.99, 1, 1002, 'Bob Smith', 'bob@example.com', 'Austin', 'TX', 106, 'Keyboard Mechanical', 'Accessories', 149.99),
      (7, '2024-02-20', 79.99, 1, 1005, 'Emma Davis', 'emma@example.com', 'Boston', 'MA', 107, 'Webcam HD', 'Electronics', 79.99),
      
      -- March sales
      (8, '2024-03-05', 399.98, 2, 1003, 'Carol Williams', 'carol@example.com', 'Chicago', 'IL', 108, 'Desk Chair', 'Furniture', 199.99),
      (9, '2024-03-15', 599.99, 1, 1006, 'Frank Miller', 'frank@example.com', 'Miami', 'FL', 109, 'Standing Desk', 'Furniture', 599.99),
      (10, '2024-03-22', 89.99, 1, 1004, 'David Brown', 'david@example.com', 'Denver', 'CO', 110, 'Desk Lamp', 'Furniture', 89.99),
      
      -- April sales
      (11, '2024-04-08', 399.99, 1, 1007, 'Grace Wilson', 'grace@example.com', 'Phoenix', 'AZ', 111, 'Tablet Pro', 'Electronics', 399.99),
      (12, '2024-04-18', 74.97, 3, 1005, 'Emma Davis', 'emma@example.com', 'Boston', 'MA', 104, 'USB Cable', 'Accessories', 24.99),
      
      -- May sales
      (13, '2024-05-10', 299.99, 1, 1002, 'Bob Smith', 'bob@example.com', 'Austin', 'TX', 103, 'Monitor 27inch', 'Electronics', 299.99),
      (14, '2024-05-20', 799.99, 1, 1008, 'Henry Taylor', 'henry@example.com', 'Seattle', 'WA', 112, 'Printer Laser', 'Electronics', 799.99),
      
      -- June sales - Customer 1001 moved to Portland
      (15, '2024-06-10', 129.99, 1, 1001, 'Alice Johnson', 'alice@example.com', 'Portland', 'OR', 101, 'Laptop Stand', 'Accessories', 129.99),
      (16, '2024-06-15', 149.99, 1, 1006, 'Frank Miller', 'frank@example.com', 'Miami', 'FL', 106, 'Keyboard Mechanical', 'Accessories', 149.99),
      (17, '2024-06-25', 599.99, 1, 1001, 'Alice Johnson', 'alice@example.com', 'Portland', 'OR', 109, 'Standing Desk', 'Furniture', 599.99),
      
      -- July sales
      (18, '2024-07-08', 199.99, 1, 1003, 'Carol Williams', 'carol@example.com', 'Chicago', 'IL', 108, 'Desk Chair', 'Furniture', 199.99),
      (19, '2024-07-18', 1599.99, 1, 1007, 'Grace Wilson', 'grace@example.com', 'Phoenix', 'AZ', 105, 'Laptop Pro', 'Electronics', 1599.99),
      
      -- August sales
      (20, '2024-08-05', 299.99, 1, 1004, 'David Brown', 'david@example.com', 'Denver', 'CO', 103, 'Monitor 27inch', 'Electronics', 299.99),
      (21, '2024-08-22', 179.98, 2, 1008, 'Henry Taylor', 'henry@example.com', 'Seattle', 'WA', 110, 'Desk Lamp', 'Furniture', 89.99),
      
      -- September sales
      (22, '2024-09-10', 49.99, 1, 1005, 'Emma Davis', 'emma@example.com', 'Boston', 'MA', 102, 'Wireless Mouse', 'Accessories', 49.99),
      (23, '2024-09-20', 399.99, 1, 1002, 'Bob Smith', 'bob@example.com', 'Austin', 'TX', 111, 'Tablet Pro', 'Electronics', 399.99),
      
      -- October sales
      (24, '2024-10-05', 799.99, 1, 1006, 'Frank Miller', 'frank@example.com', 'Miami', 'FL', 112, 'Printer Laser', 'Electronics', 799.99),
      (25, '2024-10-18', 79.99, 1, 1007, 'Grace Wilson', 'grace@example.com', 'Phoenix', 'AZ', 107, 'Webcam HD', 'Electronics', 79.99),
      
      -- November sales - Customer 1001 email changed
      (26, '2024-11-08', 299.99, 1, 1001, 'Alice Johnson', 'alice.j.new@example.com', 'Portland', 'OR', 103, 'Monitor 27inch', 'Electronics', 299.99),
      (27, '2024-11-15', 199.99, 1, 1003, 'Carol Williams', 'carol@example.com', 'Chicago', 'IL', 108, 'Desk Chair', 'Furniture', 199.99),
      
      -- December sales
      (28, '2024-12-05', 1599.99, 1, 1004, 'David Brown', 'david@example.com', 'Denver', 'CO', 105, 'Laptop Pro', 'Electronics', 1599.99),
      (29, '2024-12-15', 599.99, 1, 1008, 'Henry Taylor', 'henry@example.com', 'Seattle', 'WA', 109, 'Standing Desk', 'Furniture', 599.99),
      (30, '2024-12-28', 129.99, 2, 1001, 'Alice Johnson', 'alice.j.new@example.com', 'Portland', 'OR', 101, 'Laptop Stand', 'Accessories', 129.99)
  )
)

SELECT * FROM source_data
