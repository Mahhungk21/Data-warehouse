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
            INSERT INTO dim_customer (customer_key, name, contact_no, nid)
            SELECT DISTINCT customer_key, name, contact_no, nid
            FROM staging_customer
        ''')
        print("Inserted data into dim_customer")

        # Load from staging_item to dim_item
        print("Inserting data into dim_item...")
        cursor.execute('''
            INSERT INTO dim_item (item_key, item_name, "desc", unit_price, man_country, supplier, unit, category, subcategory)
            SELECT 
                item_key,
                item_name,
                "desc",
                unit_price,
                man_country,
                supplier,
                unit,
                -- Transform "desc" into category
                CASE 
                    WHEN "desc" ILIKE 'a. %' THEN 'a. Beverage'
                    WHEN trim("desc") ILIKE 'Beverage%' THEN 'Beverage'
                    WHEN trim("desc") ILIKE 'Kitchen%' AND NOT "desc" ILIKE '%-%' THEN 'Kitchen'
                    WHEN trim("desc") ILIKE 'Coffee%' AND NOT "desc" ILIKE '%-%' THEN 'Coffee'
                    WHEN trim("desc") ILIKE 'Food%' THEN 'Food'
                    WHEN trim("desc") ILIKE 'Gum%' THEN 'Gum'
                    WHEN trim("desc") ILIKE 'Dishware%' THEN 'Dishware'
                    WHEN "desc" ILIKE 'Coffee K-Cups%' THEN 'Coffee'
                    ELSE trim("desc")
                END AS category,
                -- Transform "desc" into subcategory
                CASE 
                    WHEN "desc" ILIKE '%-%' THEN trim(split_part("desc", '-', 2))
                    WHEN trim("desc") ILIKE 'a. Beverage%' THEN trim(substr("desc", 12))
                    WHEN trim("desc") ILIKE 'Kitchen%' AND NOT "desc" ILIKE '%-%' THEN trim(substr("desc", 8))
                    WHEN trim("desc") ILIKE 'Coffee%' AND NOT "desc" ILIKE '%-%' THEN trim(substr("desc", 7))
                    WHEN "desc" ILIKE 'Beverage Water%' THEN 'Water'
                    WHEN trim("desc") ILIKE 'Food%' THEN 'Food Item'
                    WHEN "desc" ILIKE 'Coffee K-Cups%' THEN 'K-Cups'
                    ELSE NULL
                END AS subcategory
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
                INSERT INTO dim_time (time_key, date, time, hour, day, week, month, month_name, quarter, year)
                SELECT DISTINCT 
                    time_key, 
                    TO_TIMESTAMP(date, 'DD-MM-YYYY')::DATE AS date, 
                    TO_TIMESTAMP(date, 'DD-MM-YYYY HH24:MI')::TIME AS time, 
                    hour, 
                    day, 
                    week, 
                    month,
                    CASE 
                        WHEN month = 1 THEN 'Jan'
                        WHEN month = 2 THEN 'Feb'
                        WHEN month = 3 THEN 'Mar'
                        WHEN month = 4 THEN 'Apr'
                        WHEN month = 5 THEN 'May'
                        WHEN month = 6 THEN 'Jun'
                        WHEN month = 7 THEN 'Jul'
                        WHEN month = 8 THEN 'Aug'
                        WHEN month = 9 THEN 'Sep'
                        WHEN month = 10 THEN 'Oct'
                        WHEN month = 11 THEN 'Nov'
                        WHEN month = 12 THEN 'Dec'
                    END AS month_name,
                    quarter, 
                    year
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
            INSERT INTO fact_sales (payment_key, customer_key, time_key, item_key, store_key, quantity, unit, unit_price, total_price)
            SELECT payment_key, customer_key, time_key, item_key, store_key, quantity, unit, unit_price, total_price
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
        