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
