import psycopg2
import pandas as pd
from sqlalchemy import create_engine

DATABASE_URI = 'postgresql+psycopg2://postgres:Hung2809@localhost:5432/datawarehouse'
engine = create_engine(DATABASE_URI)

def insert_csv_to_staging(csv_path, table_name):
    try:
        # Specify encoding to handle non-UTF-8 files
        df = pd.read_csv(csv_path, encoding='ISO-8859-1')  # Change encoding if needed
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Inserted {csv_path} into {table_name}")
    except UnicodeDecodeError as e:
        print(f"Error reading {csv_path}: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    insert_csv_to_staging('data/customer_dim.csv', 'staging_customer')
    insert_csv_to_staging('data/item_dim.csv', 'staging_item')
    insert_csv_to_staging('data/time_dim.csv', 'staging_time')
    insert_csv_to_staging('data/store_dim.csv', 'staging_store')
    insert_csv_to_staging('data/fact_table.csv', 'staging_sales')
    insert_csv_to_staging('data/Trans_dim.csv', 'staging_payment')