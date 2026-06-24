import logging
import yaml
from etl.extract import extract_from_mssql
from etl.transform import transform_data
from etl.load import load_to_postgres
import pyodbc
import psycopg2

# Setup logging
logging.basicConfig(filename="logs/etl.log", level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Load config
with open("config/settings.yaml", "r") as f:
    config = yaml.safe_load(f)

def run_etl():
    try:
        logging.info("Starting ETL process...")
        rows = extract_from_mssql(config["mssql"]["query"], config["mssql"]["conn_str"])
        logging.info(f"Extracted {len(rows)} rows from MSSQL")

        transformed = transform_data(rows)
        logging.info(f"Transformed {len(transformed)} rows")

        #print(transformed)
        load_to_postgres(transformed, config["postgres"], 'config.PINCODE'
                         #config["postgres"]["target_table"]
                         )
        logging.info("Data successfully loaded into PostgreSQL")

    except Exception as e:
        logging.error(f"ETL failed: {e}")
        raise


def test_sql_connection():
    try:
        conn_str = config["mssql"]["conn_str"]
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT SYSTEM_USER, DB_NAME()")
        row = cursor.fetchone()
        logging.info(f"Connected successfully as: {row[0]} to database: {row[1]}")
        print(f"Connected successfully as: {row[0]} to database: {row[1]}")
        cursor.close()
        conn.close()
    except Exception as e:
        print("Connection failed:", e)




def test_postgres_connection():
    try:
        conn = psycopg2.connect(**config["postgres"])
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"Connected successfully! PostgreSQL version: {version[0]}")
        cursor.close()
        conn.close()
    except Exception as e:
        print("Connection failed:", e)



if __name__ == "__main__":
    run_etl()
    #test_sql_connection()
    #test_postgres_connection()

