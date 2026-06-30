import io
import logging

import psycopg2
from psycopg2.extras import execute_values

def load_to_postgres(data, conn_params, target_table):
    """Load transformed data into PostgreSQL."""

    #print(conn_params)
    #print(target_table)
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    #print(conn)
    #print(cursor)
    insert_query = f"""
        INSERT INTO {target_table} (officename
      ,pincode
      ,officeType
      ,Deliverystatus
      ,divisionname
      ,regionname
      ,circlename
      ,Taluk
      ,Districtname
      ,statename
      ,STATE_CD)
        VALUES %s
            ;
    """
    execute_values(cursor, insert_query, data)
    conn.commit()
    cursor.close()
    conn.close()


def load_to_postgres_many(data, col_names, conn_params, target_table, batch_size=10000):
    """Load transformed data into PostgreSQL in batches."""
    placeholders = ", ".join(["%s"] * len(col_names))
    columns = ", ".join(col_names)
    insert_sql = f"INSERT INTO {target_table} ({columns}) VALUES ({placeholders})"

    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cursor:
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]
                cursor.executemany(insert_sql, batch)
                conn.commit()
                logging.info(f"Inserted batch {i//batch_size+1} into {target_table}")


def load_to_postgres_many_Kafka(rows, src_col_names, postgres_config, target_table,mapping):
    conn = psycopg2.connect(**postgres_config)
    cursor = conn.cursor()

    col_names = list(mapping.values())
    cols = ", ".join(col_names)
    placeholders = ", ".join(["%s"] * len(col_names))
    insert_sql = f"INSERT INTO {target_table} ({cols}) VALUES %s"

    # Convert dicts to tuples
    values = [tuple(row[col] for col in src_col_names) for row in rows]

    execute_values(cursor, insert_sql, values)
    conn.commit()
    cursor.close()
    conn.close()


def load_to_postgres_many_Kafka_Upset(rows, src_col_names, postgres_config, mapping):
    conn = psycopg2.connect(**postgres_config)
    cursor = conn.cursor()

    #print(src_col_names)

    #print("Mapping received for UPSERT:", mapping)
    
    target_table = mapping["target_table"]
    #print("Target table:", target_table)

    # Target column names from mapping, aligned with source
    col_names = [mapping["column_mapping"][src] for src in src_col_names]
    cols = ", ".join(col_names)

    #print("Target columns:", cols)

    # Primary key must be defined in mapping
    pk = mapping.get("primary_key")
    if not pk:
        raise ValueError(f"No primary_key defined for {target_table}")
    target_pk = mapping["column_mapping"][pk]

    #print("Target primary key:", target_pk)

    # Build ON CONFLICT clause (update all non-PK columns)
    update_clause = ", ".join([f"{col}=EXCLUDED.{col}" for col in col_names if col != target_pk])

    insert_sql = f"""
        INSERT INTO {target_table} ({cols})
        VALUES ({", ".join(["%s"] * len(col_names))})
        ON CONFLICT ({target_pk}) DO UPDATE SET {update_clause};
    """

    #print("Insert SQL:", insert_sql)

    # Convert dicts to tuples using source keys
    values = [tuple(row[src] for src in src_col_names) for row in rows]

    try:
        cursor.executemany(insert_sql, values)
        conn.commit()
        print(f"Upserted {len(rows)} rows into {target_table}")
    except Exception as e:
        conn.rollback()
        print(f"Error upserting rows into {target_table}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
        print(f"Connection closed for {target_table}")
