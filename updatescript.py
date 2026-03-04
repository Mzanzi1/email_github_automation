# update_emailcontent_from_github.py
import mysql.connector
import os
from dotenv import load_dotenv
import logging
import re

load_dotenv()

MYSQL_CONFIG = {
    'host': os.getenv("MYSQL_HOST"),
    'port': int(os.getenv("MYSQL_PORT")),
    'user': os.getenv("MYSQL_USER"),
    'password': os.getenv("MYSQL_PASSWORD"),
    'database': os.getenv("MYSQL_DATABASE")
}

def connect_mysql():
    return mysql.connector.connect(**MYSQL_CONFIG)

def clean_subject(subject):
    """Remove all RE:, FW:, (numbers), etc., from start of subject"""
    pattern = r'^((RE:|FW:|Fw:|\(\d+\)|\s)+)+'
    return re.sub(pattern, '', subject, flags=re.IGNORECASE).strip()

def main():
    logging.basicConfig(
        filename='update_emailcontent.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    db = connect_mysql()
    cursor = db.cursor(dictionary=True)

    # 1️⃣ Load github_projects info
    cursor.execute("SELECT email_title, country, operator, category, sub FROM github_projects")
    github_rows = cursor.fetchall()
    logging.info(f"Fetched {len(github_rows)} github_projects entries")

    # 2️⃣ Load emailcontent info
    cursor.execute("SELECT UID, subject FROM emailcontent")
    emails = cursor.fetchall()
    logging.info(f"Fetched {len(emails)} emailcontent entries")

    updated_count = 0

    for email in emails:
        email_sub_clean = clean_subject(email['subject']).lower()
        match = None

        for row in github_rows:
            if row['email_title'] and row['email_title'].lower() in email_sub_clean:
                match = row
                break

        if match:
            try:
                cursor.execute("""
                    UPDATE emailcontent
                    SET country = %s,
                        operator = %s,
                        category = %s,
                        sub = %s
                    WHERE UID = %s
                """, (
                    match.get('country'),
                    match.get('operator'),
                    match.get('category'),
                    match.get('sub'),
                    email['UID']
                ))
                updated_count += 1
            except Exception as e:
                logging.error(f"Failed to update UID {email['UID']}: {e}")

    db.commit()
    logging.info(f"✅ Updated {updated_count} emailcontent rows based on github_projects")

    cursor.close()
    db.close()

if __name__ == "__main__":
    main()
