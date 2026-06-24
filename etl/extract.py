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
