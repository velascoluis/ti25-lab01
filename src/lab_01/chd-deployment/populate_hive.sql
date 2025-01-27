-- HIVE script to create and populate a demo table
-- Storage format: PARQUET
CREATE DATABASE IF NOT EXISTS ccf_db;

-- Use the database
USE ccf_db;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS loan_repayments;
DROP TABLE IF EXISTS loan_applications;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS loan_repayments_csv;
DROP TABLE IF EXISTS loan_applications_csv;
DROP TABLE IF EXISTS customers_csv;

-- Create customers table with correct columns
CREATE TABLE IF NOT EXISTS customers (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth STRING,
    email STRING,
    phone STRING,
    registration_date STRING,
    life_event STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET;

-- Create loan applications table with correct columns
CREATE TABLE IF NOT EXISTS loan_applications (
    application_id STRING,
    customer_id STRING,
    application_date STRING,
    product_type STRING,
    loan_amount DECIMAL(10,2),
    application_status STRING,
    application_channel STRING,
    marketing_cost DECIMAL(10,2),
    approval_date STRING,
    disbursement_date STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET;

-- Create loan repayments table with correct columns
CREATE TABLE IF NOT EXISTS loan_repayments (
    repayment_id STRING,
    loan_id STRING,
    repayment_date STRING,
    amount_due DECIMAL(10,2),
    amount_paid DECIMAL(10,2),
    payment_status STRING,
    days_past_due INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET;

-- Create temporary tables for CSV data
CREATE TABLE IF NOT EXISTS customers_csv (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth STRING,
    email STRING,
    phone STRING,
    registration_date STRING,
    life_event STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE TABLE IF NOT EXISTS loan_applications_csv (
    application_id STRING,
    customer_id STRING,
    application_date STRING,
    product_type STRING,
    loan_amount DECIMAL(10,2),
    application_status STRING,
    application_channel STRING,
    marketing_cost DECIMAL(10,2),
    approval_date STRING,
    disbursement_date STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS loan_repayments_csv (
    repayment_id STRING,
    loan_id STRING,
    repayment_date STRING,
    amount_due DECIMAL(10,2),
    amount_paid DECIMAL(10,2),
    payment_status STRING,
    days_past_due INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- Load CSV data into temporary tables
LOAD DATA LOCAL INPATH '/home/cloudera/customers.csv' OVERWRITE INTO TABLE customers_csv;
LOAD DATA LOCAL INPATH '/home/cloudera/loan_applications.csv' OVERWRITE INTO TABLE loan_applications_csv;
LOAD DATA LOCAL INPATH '/home/cloudera/loan_repayments.csv' OVERWRITE INTO TABLE loan_repayments_csv;

-- Insert data from CSV tables into Parquet tables
INSERT OVERWRITE TABLE customers
SELECT * FROM customers_csv;

INSERT OVERWRITE TABLE loan_applications
SELECT * FROM loan_applications_csv;

INSERT OVERWRITE TABLE loan_repayments
SELECT * FROM loan_repayments_csv;

-- Drop temporary CSV tables
DROP TABLE customers_csv;
DROP TABLE loan_applications_csv;
DROP TABLE loan_repayments_csv;       