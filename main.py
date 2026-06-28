import json
import datetime
import decimal
import logging
import os
import threading
import yaml
from confluent_kafka.admin import AdminClient
from etl.extract import extract_from_mssql, extract_from_mssql_Cols
from etl.transform import transform_LoginHistory_Kafka, transform_Pincode, transform_Pincode_Kafka, transform_STATE_Code, transform_STATE_Code_Kafka, transform_data
from etl.load import load_to_postgres, load_to_postgres_many, load_to_postgres_many_Kafka, load_to_postgres_many_Kafka_Upset
from confluent_kafka import Consumer, Producer
import pyodbc
import psycopg2


# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

# Setup logging
logging.basicConfig(
    filename="logs/etl.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load config File
with open("config/settings.yaml", "r") as f:
    config = yaml.safe_load(f)

#Normal ETL For Single Table
#-----------------------------------------------------------------------------------------
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

#-----------------------------------------------------------------------------------------

# Connection Testing
#-----------------------------------------------------------------------------------------
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
#-----------------------------------------------------------------------------------------


#-----------------------------------------------------------------------------------------
# Multi Threading RUN (For Multiple Table)
# --- Run all tables in parallel ---
def run_etl_multithreaded():
    logging.info(f"Thread Running ETL for all tables...")
    #print(config["tables"])
    threads = []
    for mapping in config["tables"]:
        t = threading.Thread(target=etl_worker, args=(mapping,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()


TRANSFORM_MAP = {
    "transform_Pincode": transform_Pincode,
    "transform_STATE_Code": transform_STATE_Code,
}

# --- Worker for each table ---
def etl_worker(mapping):
    try:
        logging.info(f"Starting ETL for {mapping['target_table']}...")
        rows, col_names = extract_from_mssql_Cols(mapping["source_query"], config["mssql"]["conn_str"])
        logging.info(f"Extracted {len(rows)} rows from MSSQL")

        transform_func = TRANSFORM_MAP[mapping["transform"]]
        transformed = transform_func(rows)
        logging.info(f"Transformed {len(transformed)} rows")

        load_to_postgres_many(transformed, col_names, config["postgres"], mapping["target_table"])
        logging.info(f"Loaded data into {mapping['target_table']} successfully")
    except Exception as e:
        logging.error(f"ETL failed for {mapping['target_table']}: {e}")
#-----------------------------------------------------------------------------------------


# Runing ETL using Kafka
#-----------------------------------------------------------------------------------------
# Kafka ETL

TRANSFORM_MAP_Kafka = {
    "transform_Pincode": transform_Pincode_Kafka,
    "transform_STATE_Code": transform_STATE_Code_Kafka,
    "transform_LoginHistory": transform_LoginHistory_Kafka,
}

consumer = Consumer({
    'bootstrap.servers': config["kafka"]["brokers"],
    'group.id': 'etl-transform',
    'auto.offset.reset': 'earliest'
})

producer = Producer({
    'bootstrap.servers': config["kafka"]["brokers"],
    'queue.buffering.max.messages': 200000,
    'queue.buffering.max.kbytes': 1048576,  # 1 GB
})


def extract_and_publish_worker(mapping):
    topic = f"raw_mssql_data_{mapping['topic']}"
    rows, col_names = extract_from_mssql_Cols(mapping["source_query"], config["mssql"]["conn_str"])
    for row in rows:
        row_dict = {col: json_serializer(val) for col, val in zip(col_names, row)}
        producer.produce(
            topic,
            key=str(row[0]),
            value=json.dumps(row_dict)
            #,callback=delivery_report
        )
        producer.poll(0)  # prevent buffer overflow
    producer.flush()
    logging.info(f"Published {len(rows)} rows to {topic}")


def json_serializer(obj):
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    return str(obj)

def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def reset_producer(producer):
    # Clear any pending messages
    producer.flush(0)
    logging.info("Producer queue cleared before new batch.")


#Multi Threading Using Kafka Topices  
def run_kafka_etl_multitable_parallel():
    # For Temporary Flush
    reset_producer(producer)
    
    threads = []
    for mapping in config["tables"]:

        if mapping['skip']:
            logging.info(f"Skipping ETL for {mapping['topic']} as per configuration.")
            continue
        # Each mapping gets its own end-to-end pipeline
        t = threading.Thread(target=etl_pipeline_worker, args=(mapping,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

def etl_pipeline_worker(mapping):
    logging.info(f"Starting parallel ETL for {mapping['topic']}")

    mode = mapping.get("mode", "poll")  # default to normal polling

    # 1. Extract & Publish
    if mode == "poll":
        extract_and_publish_worker(mapping)

    # 2. Transform (batch mode)
    transform_worker_once(mapping)

    # 3. Load (with batching)
     # 3. Load (with batching)
    if mode == "poll":
        load_worker_once(mapping, batch_size=20000)
    elif mode == "cdc":
        cdc_worker(mapping, batch_size=20000)
    

    logging.info(f"Completed parallel ETL for {mapping['topic']}")

     # 4. Delete Kafka topics for this table
    #safe_delete_topics(
    #    [f"raw_mssql_data_{mapping['topic']}", f"transformed_data_{mapping['topic']}"],
    #    bootstrap_servers=config["kafka"]["brokers"]
    #)
    
    #logging.info(f"Deleted topics for {mapping['topic']}")

def transform_worker_once(mapping):
    consumer = Consumer({
        'bootstrap.servers': config["kafka"]["brokers"],
        'group.id': f"etl-transform-{mapping['topic']}",
        'auto.offset.reset': 'earliest'
    })
    raw_topic = f"raw_mssql_data_{mapping['topic']}"
    transformed_topic = f"transformed_data_{mapping['topic']}"
    consumer.subscribe([raw_topic])

    logging.info(f"Starting transform stage for {mapping['topic']}")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            # stop when no more messages
            break
        if msg.error():
            logging.error(msg.error())
            continue

        # Kafka message → dict
        row = json.loads(msg.value().decode("utf-8"))

        # Apply transform function (dict‑based)
        transform_func = TRANSFORM_MAP_Kafka[mapping["transform"]]
        transformed_rows = transform_func([row])

        # Publish each transformed row
        for tr in transformed_rows:
            producer.produce(
                transformed_topic,
                value=json.dumps(tr, default=json_serializer)
            )
            producer.poll(0)

    producer.flush()
    consumer.close()
    logging.info(f"Completed transform stage for {mapping['topic']}")

def load_worker_once(mapping, batch_size=10000):
    consumer = Consumer({
        'bootstrap.servers': config["kafka"]["brokers"],
        'group.id': f"etl-load-{mapping['topic']}",
        'auto.offset.reset': 'earliest'
    })
    transformed_topic = f"transformed_data_{mapping['topic']}"
    consumer.subscribe([transformed_topic])

    target_mapping = mapping['column_mapping']
    print(f"Target Mapping: {target_mapping}")

    buffer = []
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            if buffer:
                load_to_postgres_many_Kafka(buffer, list(buffer[0].keys()), config["postgres"], 
                                            mapping["target_table"],target_mapping)
                logging.info(f"Loaded final {len(buffer)} rows into {mapping['target_table']}")
            break
        if msg.error():
            logging.error(msg.error())
            continue

        row = json.loads(msg.value().decode("utf-8"))
        buffer.append(row)

        if len(buffer) >= batch_size:
            load_to_postgres_many_Kafka(buffer, list(buffer[0].keys()), config["postgres"], 
                                        mapping["target_table"],target_mapping)
            logging.info(f"Loaded {len(buffer)} rows into {mapping['target_table']}")
            buffer.clear()

    consumer.close()

def safe_delete_topics(prefixes, bootstrap_servers="localhost:9092"):
    try:
        admin = AdminClient({'bootstrap.servers': bootstrap_servers})
        metadata = admin.list_topics(timeout=10)
        topics = list(metadata.topics.keys())
        to_delete = [t for t in topics if any(t.startswith(p) for p in prefixes)]
        if not to_delete:
            logging.info("No topics matched for deletion.")
            return
        futures = admin.delete_topics(to_delete, operation_timeout=30)
        for topic, f in futures.items():
            try:
                f.result()
                logging.info(f"Deleted topic: {topic}")
            except Exception as e:
                logging.error(f"Failed to delete topic {topic}: {e}")
    except Exception as e:
        logging.error(f"Broker not available, skipping topic deletion: {e}")




#CDC Mapping

def cdc_worker(mapping, batch_size=10000):
    """
    CDC worker for a single table mapping.
    Consumes Debezium CDC events from Kafka and applies changes into Postgres.
    """
    raw_topic = f"raw_mssql_data_{mapping['topic']}"
    consumer = Consumer({
        'bootstrap.servers': config["kafka"]["brokers"],
        'group.id': f"cdc-transform-{mapping['topic']}",
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([raw_topic])

    logging.info(f"CDC worker started for {mapping['topic']}")

    buffer = []
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            # flush remaining buffer
            if buffer:
                load_to_postgres_many_Kafka(
                    buffer,
                    list(buffer[0].keys()),
                    config["postgres"],
                    mapping["target_table"],
                    mapping["column_mapping"]
                )
                logging.info(f"CDC loaded final {len(buffer)} rows into {mapping['target_table']}")
            break
        if msg.error():
            logging.error(msg.error())
            continue

        event = json.loads(msg.value().decode("utf-8"))
        op = event.get("op")

        if op in ("c", "u"):  # insert or update
            row = event["after"]
            buffer.append(row)
        elif op == "d":       # delete
            row = event["before"]
            handle_delete(row, mapping)

        if len(buffer) >= batch_size:
            load_to_postgres_many_Kafka(
                buffer,
                list(buffer[0].keys()),
                config["postgres"],
                mapping["target_table"],
                mapping["column_mapping"]
            )
            logging.info(f"CDC loaded {len(buffer)} rows into {mapping['target_table']}")
            buffer.clear()

    consumer.close()
    logging.info(f"CDC worker finished for {mapping['topic']}")


def handle_delete(row, mapping):
    """
    Delete handler for CDC events.
    Requires 'primary_key' defined in mapping.
    """
    conn = psycopg2.connect(**config["postgres"])
    cursor = conn.cursor()
    pk = mapping["primary_key"]  # must be set in YAML config
    target_pk = mapping["column_mapping"][pk]

    cursor.execute(
        f"DELETE FROM {mapping['target_table']} WHERE {target_pk} = %s",
        (row[pk],)
    )
    conn.commit()
    cursor.close()
    conn.close()
    logging.info(f"Deleted row from {mapping['target_table']} with {pk}={row[pk]}")





# -------------------------------------------------------------------

#-----------------------------------------------------------------------------------------

# Orchestration
if __name__ == "__main__":
    #run_etl()
    #run_etl_multithreaded()
    #test_sql_connection()
    #test_postgres_connection()
    run_kafka_etl_multitable_parallel()
    
