# sql_query.py

# SQL queries stored as constants

# Drop tables queries
DROP_TABLES = [
    "DROP TABLE IF EXISTS fact_sales CASCADE",
    "DROP TABLE IF EXISTS dim_payment CASCADE",
    "DROP TABLE IF EXISTS dim_customer CASCADE",
    "DROP TABLE IF EXISTS dim_time CASCADE",
    "DROP TABLE IF EXISTS dim_item CASCADE",
    "DROP TABLE IF EXISTS dim_store CASCADE",
    "DROP TABLE IF EXISTS staging_sales CASCADE",
    "DROP TABLE IF EXISTS staging_payment CASCADE",
    "DROP TABLE IF EXISTS staging_customer CASCADE",
    "DROP TABLE IF EXISTS staging_time CASCADE",
    "DROP TABLE IF EXISTS staging_item CASCADE",
    "DROP TABLE IF EXISTS staging_store CASCADE"
]

# Create staging tables
CREATE_STAGING_SALES = '''CREATE TABLE staging_sales (
    payment_key VARCHAR,
    coustomer_key VARCHAR,
    time_key VARCHAR,
    item_key VARCHAR,
    store_key VARCHAR,
    quantity INTEGER,
    unit TEXT,
    unit_price NUMERIC(18,2),
    total_price NUMERIC(18,2)
)'''

CREATE_STAGING_PAYMENT = '''CREATE TABLE staging_payment (
    payment_key VARCHAR PRIMARY KEY,
    trans_type VARCHAR,
    bank_name VARCHAR
)'''

CREATE_STAGING_CUSTOMER = '''CREATE TABLE staging_customer (
    coustomer_key VARCHAR PRIMARY KEY,
    name TEXT,
    contact_no TEXT,
    nid TEXT
)'''

CREATE_STAGING_TIME = '''CREATE TABLE staging_time (
    time_key VARCHAR PRIMARY KEY,
    date TIMESTAMP,
    hour INTEGER,
    day INTEGER,
    week VARCHAR,
    month INTEGER,
    quarter VARCHAR,
    year INTEGER
)'''

CREATE_STAGING_ITEM = '''CREATE TABLE staging_item (
    item_key VARCHAR PRIMARY KEY,
    item_name VARCHAR,
    "desc" VARCHAR,
    unit_price NUMERIC(10,2),
    man_country VARCHAR,
    supplier VARCHAR,
    unit VARCHAR
)'''

CREATE_STAGING_STORE = '''CREATE TABLE staging_store (
    store_key VARCHAR PRIMARY KEY,
    division VARCHAR,
    district VARCHAR,
    upazila VARCHAR
)'''

# Create dimension tables
CREATE_DIM_PAYMENT = '''CREATE TABLE dim_payment (
    payment_key VARCHAR PRIMARY KEY,
    trans_type VARCHAR,
    bank_name VARCHAR
)'''

CREATE_DIM_CUSTOMER = '''CREATE TABLE dim_customer (
    coustomer_key VARCHAR PRIMARY KEY,
    name TEXT,
    contact_no TEXT,
    nid TEXT
)'''

CREATE_DIM_TIME = '''CREATE TABLE dim_time (
    time_key VARCHAR PRIMARY KEY,
    date TIMESTAMP,
    hour INTEGER,
    day INTEGER,
    week VARCHAR,
    month INTEGER,
    quarter VARCHAR,
    year INTEGER
)'''

CREATE_DIM_ITEM = '''CREATE TABLE dim_item (
    item_key VARCHAR PRIMARY KEY,
    item_name VARCHAR,
    "desc" VARCHAR,
    unit_price NUMERIC(10,2),
    man_country VARCHAR,
    supplier VARCHAR,
    unit VARCHAR
)'''

CREATE_DIM_STORE = '''CREATE TABLE dim_store (
    store_key VARCHAR PRIMARY KEY,
    division VARCHAR,
    district VARCHAR,
    upazila VARCHAR
)'''

# Create fact tables
CREATE_FACT_SALES = '''CREATE TABLE fact_sales (
    sales_id SERIAL PRIMARY KEY,
    payment_key VARCHAR REFERENCES dim_payment(payment_key),
    coustomer_key VARCHAR REFERENCES dim_customer(coustomer_key),
    time_key VARCHAR REFERENCES dim_time(time_key),
    item_key VARCHAR REFERENCES dim_item(item_key),
    store_key VARCHAR REFERENCES dim_store(store_key),
    quantity INTEGER,
    unit TEXT,
    unit_price NUMERIC(18,2),
    total_price NUMERIC(18,2)
)'''

# Lists of tables to be created by category
STAGING_TABLES = [
    CREATE_STAGING_SALES,
    CREATE_STAGING_PAYMENT,
    CREATE_STAGING_CUSTOMER,
    CREATE_STAGING_TIME,
    CREATE_STAGING_ITEM,
    CREATE_STAGING_STORE
]

DIMENSION_TABLES = [
    CREATE_DIM_PAYMENT,
    CREATE_DIM_CUSTOMER,
    CREATE_DIM_TIME,
    CREATE_DIM_ITEM,
    CREATE_DIM_STORE
]

FACT_TABLES = [CREATE_FACT_SALES]
