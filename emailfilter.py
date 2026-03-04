# emailfilter.py
# downloading filtered emails
# Scheduled Interval
# FIXED VERSION - All critical issues addressed

import poplib
import email
import mysql.connector
import os
import logging
import time
import sys
from dotenv import load_dotenv
from tqdm import tqdm
from datetime import datetime, timezone, timedelta
from emailutils import decode_email_body

# Load environment variables
load_dotenv()

# Timezone setup
DUBAI_TZ = timezone(timedelta(hours=4))  # UTC+4

# Constants
FILTER_CACHE_MINUTES = 60  # How long to cache filters before refreshing

# ============================================================================
# ENVIRONMENT VARIABLE VALIDATION
# ============================================================================
def validate_environment():
    """Check all required environment variables exist before starting."""
    required_vars = [
        "POP3_SERVER", "POP3_PORT", "EMAIL_USER", "EMAIL_PASS",
        "MYSQL_HOST", "MYSQL_PORT", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE"
    ]

    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise RuntimeError(
            f"❌ Missing required environment variables: {', '.join(missing)}\n"
            f"Please check your .env file!"
        )


# Validate BEFORE loading config
validate_environment()

# Load configuration with defaults
POP3_SERVER = os.getenv("POP3_SERVER")
POP3_PORT = int(os.getenv("POP3_PORT", 995))
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")

MYSQL_CONFIG = {
    'host': os.getenv("MYSQL_HOST"),
    'port': int(os.getenv("MYSQL_PORT", 3306)),
    'user': os.getenv("MYSQL_USER"),
    'password': os.getenv("MYSQL_PASSWORD"),
    'database': os.getenv("MYSQL_DATABASE")
}

# ============================================================================
# LOGGING SETUP
# ============================================================================
logging.basicConfig(
    filename='email_filter.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Also log to console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(console_handler)

# ============================================================================
# GLOBAL CACHE FOR FILTERS
# ============================================================================
_filter_cache = {
    'filters': None,
    'last_updated': None
}

# ============================================================================
# CONNECTION FUNCTIONS
# ============================================================================
def connect_pop3():
    """
    Connect to POP3 server with retry logic and exponential backoff.
    """
    for attempt in range(3):
        try:
            conn = poplib.POP3_SSL(POP3_SERVER, POP3_PORT, timeout=90)
            conn.user(EMAIL_USER)
            conn.pass_(EMAIL_PASS)
            logging.info(f"✅ POP3 connection successful")
            return conn
        except Exception as e:
            wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
            logging.warning(f"POP3 attempt {attempt + 1}/3 failed: {e}")
            if attempt < 2:
                logging.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)

    logging.error("❌ Failed to connect to POP3 after 3 attempts")
    return None


def connect_mysql():
    """
    Connect to MySQL database with error handling.
    """
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        logging.info(f"✅ MySQL connection successful")
        return conn
    except mysql.connector.Error as e:
        logging.error(f"❌ MySQL connection failed: {e}")
        return None


# ============================================================================
# FILTER MANAGEMENT WITH CACHING
# ============================================================================
def fetch_subject_filters(cursor, force_refresh=False):
    """
    Fetch email subject filters from database with caching.

    Like you're 5: Instead of asking your mom "what's for dinner?" every 5 minutes,
    you remember what she said for an hour. Only ask again if you forgot or if
    it's been a while.

    Args:
        cursor: MySQL cursor
        force_refresh: Force reload from database even if cache is fresh

    Returns:
        List of filter dictionaries
    """
    global _filter_cache

    now = datetime.now()
    cache_age = None

    if _filter_cache['last_updated']:
        cache_age = (now - _filter_cache['last_updated']).total_seconds() / 60

    # Use cache if it's fresh and we're not forcing a refresh
    if not force_refresh and _filter_cache['filters'] and cache_age and cache_age < FILTER_CACHE_MINUTES:
        logging.info(f"📋 Using cached filters (age: {cache_age:.1f} minutes)")
        return _filter_cache['filters']

    # Fetch fresh filters from database
    try:
        cursor.execute("SELECT email_title, country, operator, category, sub FROM github_projects")
        results = cursor.fetchall()

        filters = []
        for row in results:
            filters.append({
                "email_title": row[0].lower() if row[0] else "",
                "country": row[1],
                "operator": row[2],
                "category": row[3],
                "sub": row[4]
            })

        # Update cache
        _filter_cache['filters'] = filters
        _filter_cache['last_updated'] = now

        logging.info(f"📋 Loaded {len(filters)} filters from database")
        return filters

    except mysql.connector.Error as e:
        logging.error(f"❌ Failed to fetch filters: {e}")
        # Return cached filters if available, even if stale
        if _filter_cache['filters']:
            logging.warning("⚠️  Using stale cached filters due to database error")
            return _filter_cache['filters']
        return []


def match_subject(filters, subject):
    """
    Match email subject against filters.

    Returns the matched filter dict, or None if no match.
    """
    subject_lower = subject.lower()
    for f in filters:
        if f["email_title"] and f["email_title"] in subject_lower:
            return f
    return None

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
def is_uid_downloaded(cursor, uid):
    """Check if UID already exists in database."""
    try:
        cursor.execute("SELECT 1 FROM emailcontent WHERE UID = %s LIMIT 1", (uid,))
        return cursor.fetchone() is not None
    except mysql.connector.Error as e:
        logging.error(f"Database error checking UID {uid}: {e}")
        return False

def get_latest_uid(cursor):
    """
    Get the most recently processed UID from database.

    Returns None if no emails have been processed yet.
    """
    try:
        # Order by email_date since there's no auto-increment id column
        cursor.execute("SELECT UID FROM emailcontent ORDER BY email_date DESC LIMIT 1")
        row = cursor.fetchone()
        return row[0] if row else None
    except mysql.connector.Error as e:
        logging.error(f"Failed to get latest UID: {e}")
        return None

# ============================================================================
# MAIN FILTER & DOWNLOAD LOGIC
# ============================================================================
def filter_and_download():
    """
    Download and filter emails from POP3 server.

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

        # Fetch filters (with caching)
        subject_filters = fetch_subject_filters(cursor)

        if not subject_filters:
            logging.warning("⚠️  No filters found - will skip all emails")

        # Get latest UID we've processed
        latest_uid = get_latest_uid(cursor)
        if latest_uid:
            logging.info(f"📌 Latest processed UID: {latest_uid}")
        else:
            logging.info("📌 No previous UIDs found - will process all matching emails")

        # Get UIDL list from server
        try:
            messages = pop_conn.uidl()[1]
            logging.info(f"📬 Found {len(messages)} total messages on server")
        except Exception as e:
            logging.error(f"❌ Failed to fetch UIDL list: {e}")
            return

        # Counters
        downloaded_count = 0
        skipped_count = 0
        error_count = 0

        # Process each message
        for entry in tqdm(messages, desc="Filtering emails"):
            try:
                # Parse UID list entry
                parts = entry.decode('utf-8', errors='replace').split()
                if len(parts) < 2:
                    logging.warning(f"Invalid UIDL entry: {entry}")
                    error_count += 1
                    continue

                index = int(parts[0])
                uid = parts[1]

                # Validate UID format
                if not uid or len(uid) > 255:  # Basic sanity check
                    logging.warning(f"UID {uid} looks invalid, skipping")
                    skipped_count += 1
                    continue

                # Skip if already downloaded
                if is_uid_downloaded(cursor, uid):
                    skipped_count += 1
                    continue

                # Skip if we've seen this UID before (string comparison is fine here)
                # Note: This is a simple check. For numeric UIDs, you might want numeric comparison
                if latest_uid and uid <= latest_uid:
                    skipped_count += 1
                    continue

                # Retrieve email with retry logic
                raw_bytes = None
                for attempt in range(3):
                    try:
                        response, lines, octets = pop_conn.retr(index)
                        raw_bytes = b"\r\n".join(lines)
                        break
                    except Exception as e:
                        wait_time = 2 ** attempt  # Exponential backoff
                        logging.warning(f"Attempt {attempt + 1}/3 failed for UID {uid}: {e}")
                        if attempt < 2:
                            time.sleep(wait_time)

                if not raw_bytes:
                    logging.error(f"❌ Failed to retrieve UID {uid} after 3 attempts")
                    error_count += 1
                    continue

                # Decode raw content for processing (not stored in database)
                rawcontent = raw_bytes.decode('utf-8', errors="replace")

                # Parse message
                msg = email.message_from_bytes(raw_bytes)
                subject = msg.get("Subject", "(No Subject)")

                # Check if subject matches any filter
                matched = match_subject(subject_filters, subject)
                if not matched:
                    skipped_count += 1
                    continue

                # Parse email date
                date_header = msg.get("Date", "")
                email_date = None

                if date_header:
                    try:
                        email_date = email.utils.parsedate_to_datetime(date_header)
                        if email_date:
                            # Handle timezone
                            if email_date.tzinfo is None:
                                email_date = email_date.replace(tzinfo=DUBAI_TZ)
                            else:
                                email_date = email_date.astimezone(DUBAI_TZ)
                    except Exception as e:
                        logging.warning(f"Date parse error for UID {uid}: {e}")

                # Use current time if date parsing failed
                if not email_date:
                    email_date = datetime.now(DUBAI_TZ)

                # Decode email body with error handling
                try:
                    plaintext = decode_email_body(rawcontent)
                except Exception as e:
                    logging.error(f"Failed to decode body for UID {uid}: {e}")
                    plaintext = "Failed to decode email body"

                # Insert into database (rawcontent NOT stored - saves disk space)
                try:
                    cursor.execute("""
                        INSERT INTO emailcontent
                        (UID, plaintext, subject, email_date, status,
                         country, operator, category, sub)
                        VALUES (%s, %s, %s, %s, 'pending', %s, %s, %s, %s)
                    """, (
                        uid,
                        plaintext,
                        subject.strip(),
                        email_date,
                        matched.get("country"),
                        matched.get("operator"),
                        matched.get("category"),
                        matched.get("sub")
                    ))
                    db.commit()
                    downloaded_count += 1
                    logging.info(f"✅ Downloaded UID {uid}: {subject[:50]}")

                except mysql.connector.Error as db_error:
                    logging.error(f"❌ DB insert failed for UID {uid}: {db_error}")
                    db.rollback()
                    error_count += 1

            except Exception as e:
                logging.error(f"❌ Error processing email entry: {e}", exc_info=True)
                error_count += 1
                continue

        # Log summary
        logging.info(f"{'=' * 80}")
        logging.info(f"✅ Filtering complete")
        logging.info(f"   📥 Downloaded: {downloaded_count}")
        logging.info(f"   ⏭️  Skipped: {skipped_count}")
        logging.info(f"   ❌ Errors: {error_count}")
        logging.info(f"{'=' * 80}")

    except Exception as e:
        logging.error(f"❌ Critical error in filter_and_download: {e}", exc_info=True)

    finally:
        # ALWAYS clean up resources
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
    logging.info("🚀 Email Filter & Downloader Started")
    logging.info(f"📧 POP3 Server: {POP3_SERVER}:{POP3_PORT}")
    logging.info(f"📧 Email Account: {EMAIL_USER}")
    logging.info(f"🗄️  MySQL Database: {MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}")
    logging.info(f"⏱️  Check Interval: {INTERVAL_MINUTES} minutes")
    logging.info(f"💾 Filter Cache Duration: {FILTER_CACHE_MINUTES} minutes")
    logging.info("=" * 80)

    run_count = 0

    while True:
        run_count += 1
        logging.info(f"\n{'=' * 80}")
        logging.info(f"🔁 Starting email filtering cycle (Run #{run_count})")
        logging.info(f"{'=' * 80}")

        try:
            filter_and_download()
        except Exception as e:
            logging.error(f"🚨 Critical error during scheduled cycle: {e}", exc_info=True)

        logging.info(f"⏸️  Sleeping for {INTERVAL_MINUTES} minutes...")
        logging.info(f"{'=' * 80}\n")

        time.sleep(INTERVAL_MINUTES * 60)
