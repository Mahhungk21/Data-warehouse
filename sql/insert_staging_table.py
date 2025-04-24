import psycopg2
import pandas as pd
from sqlalchemy import create_engine

DATABASE_URI = 'postgresql+psycopg2://postgres:Hung2809@localhost:5432/datawarehouse'
engine = create_engine(DATABASE_URI)

def insert_csv_to_staging(csv_path, table_name, key_col):
    try:
        # Specify encoding to handle non-UTF-8 files
        df = pd.read_csv(csv_path, encoding='ISO-8859-1')  # Change encoding if needed
        
        # Rename column 'coustomer' to 'customer' if it exists
        if 'coustomer_key' in df.columns:
            df.rename(columns={'coustomer_key': 'customer_key'}, inplace=True)
        if table_name == 'staging_time':
        # Chỉ định dayfirst=True để pandas hiểu day-month-year
            df['date'] = pd.to_datetime(df['date'], dayfirst=True, format='%d-%m-%Y %H:%M')
            
        existing = pd.read_sql(f"SELECT {key_col} FROM {table_name}", engine)
        new_df = df[~df[key_col].isin(existing[key_col])]  # chỉ lấy những dòng chưa có key
        
        if not new_df.empty:
            new_df.to_sql(table_name, engine, if_exists='append', index=False)
            print(f"Inserted new data into {table_name}")
        else:
            print("No new data to insert.")
    except UnicodeDecodeError as e:
        print(f"Error reading {csv_path}: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    insert_csv_to_staging('data/customer_dim.csv', 'staging_customer', 'customer_key')
    insert_csv_to_staging('data/item_dim.csv', 'staging_item', 'item_key')
    insert_csv_to_staging('data/time_dim.csv', 'staging_time', 'time_key')
    insert_csv_to_staging('data/store_dim.csv', 'staging_store', 'store_key')
    insert_csv_to_staging('data/fact_table.csv', 'staging_sales', 'payment_key')
    insert_csv_to_staging('data/Trans_dim.csv', 'staging_payment', 'payment_key')