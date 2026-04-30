import os
import mysql.connector


def get_connection(with_database=True):
    host = os.getenv('DB_HOST', 'localhost')
    port = int(os.getenv('DB_PORT', '3306'))
    name = os.getenv('DB_NAME', 'irisdb')
    user = os.getenv('DB_USER', 'root')
    password = os.getenv('DB_PASSWORD', '')

    params = dict(host=host, port=port, user=user, password=password)
    if with_database:
        params['database'] = name
    return mysql.connector.connect(**params)


def ensure_database():
    name = os.getenv('DB_NAME', 'irisdb')
    conn = get_connection(with_database=False)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{name}`")
    cursor.close()
    conn.close()
