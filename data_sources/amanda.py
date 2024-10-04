import oracledb as cx_Oracle
import os

# AMANDA RR DB Credentials
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
SERVICE_NAME = os.getenv("SERVICE_NAME")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASS")


def get_conn():
    """
    Get connected to the AMANDA Read replica database

    Returns
    -------
    cx_Oracle Connection Object

    """
    dsn_tns = cx_Oracle.makedsn(HOST, PORT, service_name=SERVICE_NAME)
    return cx_Oracle.connect(user=USER, password=PASSWORD, dsn=dsn_tns)


def row_factory(cursor):
    """
    Define cursor row handler which returns each row as a dict
    h/t https://stackoverflow.com/questions/35045879/cx-oracle-how-can-i-receive-each-row-as-a-dictionary

    Parameters
    ----------
    cursor : cx_Oracle Cursor object

    Returns
    -------
    function: the rowfactory.

    """
    return lambda *args: dict(zip([d[0] for d in cursor.description], args))


def get_amanda_data(query):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.rowfactory = row_factory(cursor)
    rows = cursor.fetchall()
    conn.close()
    return rows
