import logging
import yaml
import pyodbc
import psycopg2
import threading

logging.basicConfig(filename="logs/etl.log", level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Load config
with open("config/settings.yaml", "r") as f:
    config = yaml.safe_load(f)

# --- Extract ---
def extract_from_mssql(query, conn_str):
    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]
    return rows, col_names

# --- Table-specific transforms ---
def transform_table1(rows, col_names):
    return [
        (row[0], row[1].strip().upper(), row[2])
        for row in rows if row[1]
    ]

def transform_table2(rows, col_names):
    return [
        (row[0], row[1].lower(), row[2], row[3])
        for row in rows if row[1]
    ]

TRANSFORM_MAP = {
    "transform_table1": transform_table1,
    "transform_table2": transform_table2,
}

# --- Load with batching ---
def load_to_postgres(data, col_names, pg_conf, target_table, batch_size=1000):
    placeholders = ", ".join(["%s"] * len(col_names))
    columns = ", ".join(col_names)
    insert_sql = f"INSERT INTO {target_table} ({columns}) VALUES ({placeholders})"

    with psycopg2.connect(
        host=pg_conf["host"],
        dbname=pg_conf["dbname"],
        user=pg_conf["user"],
        password=pg_conf["password"],
        port=pg_conf["port"]
    ) as conn:
        with conn.cursor() as cursor:
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]
                cursor.executemany(insert_sql, batch)
                conn.commit()
                logging.info(f"Inserted batch {i//batch_size+1} into {target_table}")

# --- Worker for each table ---
def etl_worker(mapping):
    try:
        logging.info(f"Starting ETL for {mapping['target_table']}...")
        rows, col_names = extract_from_mssql(mapping["source_query"], config["mssql"]["conn_str"])
        logging.info(f"Extracted {len(rows)} rows from MSSQL")

        transform_func = TRANSFORM_MAP[mapping["transform"]]
        transformed = transform_func(rows, col_names)
        logging.info(f"Transformed {len(transformed)} rows")

        load_to_postgres(transformed, col_names, config["postgres"], mapping["target_table"])
        logging.info(f"Loaded data into {mapping['target_table']} successfully")
    except Exception as e:
        logging.error(f"ETL failed for {mapping['target_table']}: {e}")

# --- Run all tables in parallel ---
def run_etl_multithreaded():
    threads = []
    for mapping in config["tables"]:
        t = threading.Thread(target=etl_worker, args=(mapping,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

if __name__ == "__main__":
    run_etl_multithreaded()
