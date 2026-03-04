import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

MYSQL_CONFIG = {
    'host': os.getenv("MYSQL_HOST"),
    'port': int(os.getenv("MYSQL_PORT", 3306)),
    'user': os.getenv("MYSQL_USER"),
    'password': os.getenv("MYSQL_PASSWORD"),
    'database': os.getenv("MYSQL_DATABASE")
}

conn = mysql.connector.connect(**MYSQL_CONFIG)
cursor = conn.cursor()

cursor.execute("UPDATE apioutput SET status = 'pushed'")
conn.commit()

print(f"Reset complete: {cursor.rowcount} rows updated")

cursor.close()
conn.close()
