import os
import mysql.connector

DB_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_NAME = os.environ.get("DB_NAME")
DB_PORT = os.environ.get("DB_PORT")

print("DEBUG: Trying to connect with:", DB_HOST, DB_USER, DB_NAME, DB_PORT)

try:
    db = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=int(DB_PORT)
    )
    print("SUCCESS: Connection was successful!")
    db.close()
except Exception as e:
    print(f"FAILURE: The connection failed. Error: {e}")
