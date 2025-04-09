import psycopg2
from sql.sql_query import DROP_TABLES, STAGING_TABLES, DIMENSION_TABLES, FACT_TABLES

def create_tables():
    conn = psycopg2.connect(
        dbname="datawarehouse",
        user="postgres",
        password="Hung2809",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    # Drop existing tables to ensure a clean state
    for query in DROP_TABLES:
        cursor.execute(query)

    # Create staging tables
    for query in STAGING_TABLES:
        cursor.execute(query)

    # Create dimension tables
    for query in DIMENSION_TABLES:
        cursor.execute(query)

    # Create fact tables
    for query in FACT_TABLES:
        cursor.execute(query)

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    create_tables()
    print("All tables created successfully.")