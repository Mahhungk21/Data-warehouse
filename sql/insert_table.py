import psycopg2

def etl_staging_to_main():
    conn = psycopg2.connect(
        dbname="datawarehouse",
        user="postgres",
        password="Hung2809",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    try:
        # Load from staging_customer to dim_customer
        print("Inserting data into dim_customer...")
        cursor.execute('''
            INSERT INTO dim_customer (coustomer_key, name, contact_no, nid)
            SELECT DISTINCT coustomer_key, name, contact_no, nid
            FROM staging_customer
        ''')
        print("Inserted data into dim_customer")

        # Load from staging_item to dim_item
        print("Inserting data into dim_item...")
        cursor.execute('''
            INSERT INTO dim_item (item_key, item_name, "desc", unit_price, man_country, supplier, unit)
            SELECT DISTINCT item_key, item_name, "desc", unit_price, man_country, supplier, unit
            FROM staging_item
        ''')
        print("Inserted data into dim_item")

        # Load from staging_store to dim_store
        print("Inserting data into dim_store...")
        cursor.execute('''
            INSERT INTO dim_store (store_key, division, district, upazila)
            SELECT DISTINCT store_key, division, district, upazila
            FROM staging_store
        ''')
        print("Inserted data into dim_store")

        # Load from staging_time to dim_time
        print("Inserting data into dim_time...")
        cursor.execute('''
            INSERT INTO dim_time (time_key, date, hour, day, week, month, quarter, year)
            SELECT DISTINCT time_key, TO_TIMESTAMP(date, 'DD-MM-YYYY HH24:MI'), hour, day, week, month, quarter, year
            FROM staging_time
        ''')
        print("Inserted data into dim_time")

        # Load from staging_payment to dim_payment
        print("Inserting data into dim_payment...")
        cursor.execute('''
            INSERT INTO dim_payment (payment_key, trans_type, bank_name)
            SELECT DISTINCT payment_key, trans_type, bank_name
            FROM staging_payment
        ''')
        print("Inserted data into dim_payment")

        # Load from staging_sales to fact_sales
        print("Inserting data into fact_sales...")
        cursor.execute('''
            INSERT INTO fact_sales (payment_key, coustomer_key, time_key, item_key, store_key, quantity, unit, unit_price, total_price)
            SELECT payment_key, coustomer_key, time_key, item_key, store_key, quantity, unit, unit_price, total_price
            FROM staging_sales
        ''')
        print("Inserted data into fact_sales")

        # Drop staging tables
        print("Dropping staging tables...")
        staging_tables = ['staging_customer', 'staging_item', 'staging_store', 'staging_time', 'staging_payment', 'staging_sales']
        for table in staging_tables:
            cursor.execute(f'DROP TABLE IF EXISTS {table} CASCADE')
            print(f"Dropped table {table}")

        conn.commit()
    except Exception as e:
        print(f"An error occurred during ETL: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        print("ETL process completed.")