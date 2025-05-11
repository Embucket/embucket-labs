CREATE TABLE employee_table (
    employee_ID INTEGER,
    last_name VARCHAR,
    first_name VARCHAR,
    department_ID INTEGER
    );

CREATE TABLE department_table (
    department_ID INTEGER,
    department_name VARCHAR
    );

INSERT INTO employee_table (employee_ID, last_name, first_name, department_ID) VALUES
    (101, 'Montgomery', 'Pat', 1),
    (102, 'Levine', 'Terry', 2),
    (103, 'Comstock', 'Dana', 2);

INSERT INTO department_table (department_ID, department_name) VALUES
    (1, 'Engineering'),
    (2, 'Customer Support'),
    (3, 'Finance');

CREATE TABLE ftable1 (retail_price FLOAT, wholesale_cost FLOAT, description VARCHAR);

INSERT INTO ftable1 (retail_price, wholesale_cost, description) 
  VALUES (14.00, 6.00, 'bling');

CREATE TABLE t1 (col1 INTEGER);
CREATE TABLE t2 (col1 INTEGER);

INSERT INTO t1 (col1) VALUES 
   (2),
   (3),
   (4);
INSERT INTO t2 (col1) VALUES 
   (1),
   (2),
   (2),
   (3);

CREATE TABLE departments (department_id INTEGER, name VARCHAR);
CREATE TABLE employees (employee_ID INTEGER, last_name VARCHAR, 
                        department_ID INTEGER);

INSERT INTO departments (department_ID, name) VALUES 
    (1, 'Engineering'), 
    (2, 'Support');
INSERT INTO employees (employee_ID, last_name, department_ID) VALUES 
    (101, 'Richards', 1),
    (102, 'Paulson',  1),
    (103, 'Johnson',  2);

CREATE OR REPLACE TABLE quarterly_sales(
  empid INT,
  amount INT,
  quarter TEXT)
  AS SELECT * FROM VALUES
    (1, 10000, '2023_Q1'),
    (1, 400, '2023_Q1'),
    (2, 4500, '2023_Q1'),
    (2, 35000, '2023_Q1'),
    (1, 5000, '2023_Q2'),
    (1, 3000, '2023_Q2'),
    (2, 200, '2023_Q2'),
    (2, 90500, '2023_Q2'),
    (1, 6000, '2023_Q3'),
    (1, 5000, '2023_Q3'),
    (2, 2500, '2023_Q3'),
    (2, 9500, '2023_Q3'),
    (3, 2700, '2023_Q3'),
    (1, 8000, '2023_Q4'),
    (1, 10000, '2023_Q4'),
    (2, 800, '2023_Q4'),
    (2, 4500, '2023_Q4'),
    (3, 2700, '2023_Q4'),
    (3, 16000, '2023_Q4'),
    (3, 10200, '2023_Q4');

CREATE OR REPLACE TABLE monthly_sales(
  empid INT,
  dept TEXT,
  jan INT,
  feb INT,
  mar INT,
  apr INT);

INSERT INTO monthly_sales VALUES
  (1, 'electronics', 100, 200, 300, 100),
  (2, 'clothes', 100, 300, 150, 200),
  (3, 'cars', 200, 400, 100, 50),
  (4, 'appliances', 100, NULL, 100, 50);

CREATE TABLE leased_bicycles (bicycle_id INTEGER, customer_id INTEGER);
CREATE TABLE returned_bicycles (bicycle_id INTEGER);

INSERT INTO leased_bicycles (bicycle_ID, customer_ID) VALUES
    (101, 1111),
    (102, 2222),
    (103, 3333),
    (104, 4444),
    (105, 5555);
INSERT INTO returned_bicycles (bicycle_ID) VALUES
    (102),
    (104);

CREATE TABLE target_table (ID INTEGER, description VARCHAR);

CREATE TABLE source_table (ID INTEGER, description VARCHAR);

INSERT INTO target_table (ID, description) VALUES
    (10, 'To be updated (this is the old value)')
    ;

INSERT INTO source_table (ID, description) VALUES
    (10, 'To be updated (this is the new value)')
    ;

CREATE TABLE sales (
  product_ID INTEGER,
  retail_price REAL,
  quantity INTEGER,
  city VARCHAR,
  state VARCHAR);

INSERT INTO sales (product_id, retail_price, quantity, city, state) VALUES
  (1, 2.00,  1, 'SF', 'CA'),
  (1, 2.00,  2, 'SJ', 'CA'),
  (2, 5.00,  4, 'SF', 'CA'),
  (2, 5.00,  8, 'SJ', 'CA'),
  (2, 5.00, 16, 'Miami', 'FL'),
  (2, 5.00, 32, 'Orlando', 'FL'),
  (2, 5.00, 64, 'SJ', 'PR');

CREATE TABLE products (
  product_ID INTEGER,
  wholesale_price REAL);
INSERT INTO products (product_ID, wholesale_price) VALUES (1, 1.00);
INSERT INTO products (product_ID, wholesale_price) VALUES (2, 2.00);

CREATE OR REPLACE TABLE d1 (
  id INTEGER,
  name VARCHAR
  );
INSERT INTO d1 (id, name) VALUES
  (1,'a'),
  (2,'b'),
  (4,'c');

CREATE OR REPLACE TABLE d2 (
  id INTEGER,
  value VARCHAR
  );
INSERT INTO d2 (id, value) VALUES
  (1,'xx'),
  (2,'yy'),
  (5,'zz');