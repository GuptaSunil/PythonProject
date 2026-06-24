import pyodbc

def extract_from_mssql(query, conn_str):
    """Extract data from MSSQL Server."""
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


def extract_from_mssql_Cols(query, conn_str):
    """Extract data from MSSQL Server."""
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    col_names = [desc[0] for desc in cursor.description]
    cursor.close()
    conn.close()
    return rows, col_names
