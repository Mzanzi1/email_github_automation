# emaildownloader.py
# downloading email headers
# Scheduled Interval
# FIXED VERSION - All critical issues addressed

import poplib
import email
import mysql.connector
from dotenv import load_dotenv
import os
from tqdm import tqdm
import logging
from datetime import datetime, timezone, timedelta
import time
import sys

# Load environment variables
load_dotenv()

# Timezone setup
DUBAI_TZ = timezone(timedelta(hours=4))


# ============================================================================
# ENVIRONMENT VARIABLE VALIDATION
# ============================================================================
def validate_environment():
    """
    Check all required environment variables exist before starting.
    Like checking you have all ingredients before cooking.
    """
    required_vars = [
        "POP3_SERVER", "POP3_PORT", "EMAIL_USER", "EMAIL_PASS",
        "MYSQL_HOST", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE"
    ]

    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise RuntimeError(
            f"❌ Missing required environment variables: {', '.join(missing)}\n"
            f"Please check your .env file!"
        )


# Validate BEFORE loading anything else
validate_environment()

# Load configuration with defaults
POP3_SERVER = os.getenv("POP3_SERVER")
POP3_PORT = int(os.getenv("POP3_PORT", 995))  # Default to 995 if somehow missing
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")

MYSQL_CONFIG = {
    'host': os.getenv("MYSQL_HOST"),
    'user': os.getenv("MYSQL_USER"),
    'password': os.getenv("MYSQL_PASSWORD"),
    'database': os.getenv("MYSQL_DATABASE"),
    'port': int(os.getenv("MYSQL_PORT", 3306))
}

# ============================================================================
# LOGGING SETUP - DO THIS ONCE, NOT IN A LOOP
# ============================================================================
logging.basicConfig(
    filename='email_download.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Also log to console so you can see what's happening
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(console_handler)


# ============================================================================
# CONNECTION FUNCTIONS
# ============================================================================
def connect_pop3():
    """
    Connect to POP3 server with retry logic.
    Tries 3 times before giving up.
    """
    for attempt in range(3):
        try:
            pop_conn = poplib.POP3_SSL(POP3_SERVER, POP3_PORT, timeout=30)
            pop_conn.user(EMAIL_USER)
            pop_conn.pass_(EMAIL_PASS)
            logging.info(f"✅ POP3 connection successful")
            return pop_conn
        except Exception as e:
            logging.warning(f"POP3 connection failed (attempt {attempt + 1}/3): {e}")
            if attempt < 2:  # Don't sleep on the last attempt
                time.sleep(2)  # Wait 2 seconds before retry

    logging.error("❌ Failed to connect to POP3 after 3 attempts")
    return None


def connect_mysql():
    """
    Connect to MySQL database with error handling.
    Returns None if connection fails instead of crashing.
    """
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        logging.info(f"✅ MySQL connection successful")
        return conn
    except mysql.connector.Error as e:
        logging.error(f"❌ MySQL connection failed: {e}")
        return None


# ============================================================================
# MAIN EMAIL DOWNLOAD FUNCTION
# ============================================================================
def download_headers():
    """
    Download new email headers from POP3 server and store in MySQL.
    Uses proper resource cleanup to prevent connection leaks.
    """
    # Connect to POP3
    pop_conn = connect_pop3()
    if not pop_conn:
        logging.error("Cannot proceed without POP3 connection")
        return

    # Connect to MySQL
    db = connect_mysql()
    if not db:
        logging.error("Cannot proceed without MySQL connection")
        try:
            pop_conn.quit()
        except:
            pass
        return

    cursor = None

    try:
        cursor = db.cursor()

        # Get existing UIDs from database
        cursor.execute("SELECT UID FROM emailheaders")
        existing_uids = set(row[0] for row in cursor.fetchall())
        logging.info(f"📊 Found {len(existing_uids)} existing UIDs in database")

        # Get UIDL list from server
        try:
            messages = pop_conn.uidl()[1]
        except Exception as e:
            logging.error(f"❌ Failed to fetch UIDL list: {e}")
            return  # The finally block will clean up connections

        # Build UID map (UID -> POP3 message index)
        uid_map = {}
        for i, line in enumerate(messages):
            parts = line.decode('utf-8', errors='replace').split()
            if len(parts) >= 2:
                uid_map[parts[1]] = i + 1  # POP3 uses 1-based indexing

        # Find new UIDs
        new_uids = [uid for uid in uid_map if uid not in existing_uids]

        if not new_uids:
            logging.info("✅ No new UIDs found — database is up to date")
            return

        logging.info(f"📥 Found {len(new_uids)} new emails to download")

        # Download headers for new UIDs
        headers_batch = []
        failed_count = 0

        for uid in tqdm(new_uids, desc="Downloading new headers", disable=False):
            index = uid_map[uid]
            try:
                # Get header only (top 0 lines of body)
                response, lines, octets = pop_conn.top(index, 0)

                # Decode with error handling for weird encodings
                msg_content = b'\r\n'.join(lines).decode('utf-8', errors='replace')
                msg = email.message_from_string(msg_content)

                # Extract header fields
                subject = msg.get("Subject", "No Subject")
                from_address = msg.get("From", "Unknown")
                raw_date = msg.get("Date", "")

                # Parse email date with error handling
                email_date = None
                if raw_date:
                    try:
                        email_date = email.utils.parsedate_to_datetime(raw_date)
                        if email_date:
                            # Handle timezone
                            if email_date.tzinfo is None:
                                email_date = email_date.replace(tzinfo=DUBAI_TZ)
                            else:
                                email_date = email_date.astimezone(DUBAI_TZ)
                    except Exception as e:
                        logging.warning(f"Failed to parse date for UID {uid}: {e}")
                        email_date = None

                headers_batch.append((uid, subject, from_address, email_date))

            except Exception as e:
                logging.warning(f"Failed to process message UID {uid}: {e}")
                failed_count += 1
                continue

        # Insert into database
        if headers_batch:
            try:
                cursor.executemany("""
                    INSERT INTO emailheaders (UID, subject, fromaddress, email_date)
                    VALUES (%s, %s, %s, %s)
                """, headers_batch)
                db.commit()
                logging.info(f"✅ Successfully inserted {len(headers_batch)} new headers into database")
            except mysql.connector.Error as e:
                logging.error(f"❌ Database insert failed: {e}")
                db.rollback()

        if failed_count > 0:
            logging.warning(f"⚠️  Failed to process {failed_count} emails")

    except Exception as e:
        logging.error(f"❌ Unexpected error in download_headers: {e}", exc_info=True)

    finally:
        # ALWAYS clean up resources, no matter what happened
        # Like you're 5: Always put your toys back in the toy box when you're done

        if cursor:
            try:
                cursor.close()
            except Exception as e:
                logging.warning(f"Failed to close cursor: {e}")

        if db:
            try:
                db.close()
            except Exception as e:
                logging.warning(f"Failed to close database: {e}")

        if pop_conn:
            try:
                pop_conn.quit()
            except Exception as e:
                logging.warning(f"POP3 quit failed: {e}")

        logging.info("🧹 All connections closed")


# ============================================================================
# SCHEDULER LOOP
# ============================================================================
if __name__ == "__main__":
    # Configuration
    INTERVAL_MINUTES = 5  # Change this to your desired frequency

    logging.info("=" * 80)
    logging.info("🚀 Email Header Downloader Started")
    logging.info(f"📧 POP3 Server: {POP3_SERVER}:{POP3_PORT}")
    logging.info(f"📧 Email Account: {EMAIL_USER}")
    logging.info(f"🗄️  MySQL Database: {MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}")
    logging.info(f"⏱️  Check Interval: {INTERVAL_MINUTES} minutes")
    logging.info("=" * 80)

    run_count = 0

    while True:
        run_count += 1
        logging.info(f"\n{'=' * 80}")
        logging.info(f"🔁 Starting email header check (Run #{run_count})")
        logging.info(f"{'=' * 80}")

        try:
            download_headers()
        except Exception as e:
            logging.error(f"🚨 Critical error during email header download: {e}", exc_info=True)

        logging.info(f"⏸️  Sleeping for {INTERVAL_MINUTES} minutes...")
        logging.info(f"{'=' * 80}\n")

        time.sleep(INTERVAL_MINUTES * 60)
