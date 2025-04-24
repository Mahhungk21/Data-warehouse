from sql.create_table import create_tables
from sql.insert_staging_table import insert_csv_to_staging
from sql.insert_table import etl_staging_to_main

def main():
    # Bước 1: Tạo các bảng (staging, dimension, fact)
    print("Creating tables...")
    create_tables()

    # Bước 2: Load dữ liệu CSV vào staging
    print("Loading data into staging tables...")
    insert_csv_to_staging('data/customer_dim.csv', 'staging_customer', 'customer_key')
    insert_csv_to_staging('data/item_dim.csv', 'staging_item', 'item_key')
    insert_csv_to_staging('data/time_dim.csv', 'staging_time', 'time_key')
    insert_csv_to_staging('data/store_dim.csv', 'staging_store', 'store_key')
    insert_csv_to_staging('data/fact_table.csv', 'staging_sales', 'payment_key')
    insert_csv_to_staging('data/Trans_dim.csv', 'staging_payment', 'payment_key')
    # Bước 3: Thực hiện ETL từ staging vào bảng chính
    print("Performing ETL from staging to main tables...")
    etl_staging_to_main()
    # transform_and_load_items() 

    print("Data Warehouse pipeline executed successfully!")

if __name__ == '__main__':
    main()
