# emailfilter.py
# downloading filtered emails
# Scheduled Interval
# FULL VERSION with UID recovery support

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

FILTER_CACHE_MINUTES = 60

# ============================================================================
# ENVIRONMENT VALIDATION
# ============================================================================
def validate_environment():
    required_vars = [
        "POP3_SERVER", "POP3_PORT", "EMAIL_USER", "EMAIL_PASS",
        "MYSQL_HOST", "MYSQL_PORT", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE"
    ]
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        raise RuntimeError(f"Missing env vars: {missing}")

validate_environment()

POP3_SERVER = os.getenv("POP3_SERVER")
POP3_PORT = int(os.getenv("POP3_PORT", 995))
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": int(os.getenv("MYSQL_PORT", 3306)),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE"),
}

# ============================================================================
# LOGGING
# ============================================================================
logging.basicConfig(
    filename="email_filter.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logging.getLogger().addHandler(console)

# ============================================================================
# CONNECTIONS
# ============================================================================
def connect_pop3():
    for attempt in range(3):
        try:
            conn = poplib.POP3_SSL(POP3_SERVER, POP3_PORT, timeout=90)
            conn.user(EMAIL_USER)
            conn.pass_(EMAIL_PASS)
            logging.info("✅ POP3 connected")
            return conn
        except Exception as e:
            time.sleep(2**attempt)
    logging.error("❌ POP3 failed")
    return None


def connect_mysql():
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        logging.info("✅ MySQL connected")
        return conn
    except Exception as e:
        logging.error(f"MySQL error: {e}")
        return None


# ============================================================================
# HELPERS
# ============================================================================
def is_uid_downloaded(cursor, uid):
    cursor.execute("SELECT 1 FROM emailcontent WHERE UID=%s LIMIT 1", (uid,))
    return cursor.fetchone() is not None


def decode_and_store(cursor, db, uid, raw_bytes):
    msg = email.message_from_bytes(raw_bytes)
    subject = msg.get("Subject", "(No Subject)")

    # date
    date_header = msg.get("Date")
    try:
        email_date = email.utils.parsedate_to_datetime(date_header)
        if email_date.tzinfo:
            email_date = email_date.astimezone(DUBAI_TZ)
        else:
            email_date = email_date.replace(tzinfo=DUBAI_TZ)
    except:
        email_date = datetime.now(DUBAI_TZ)

    rawcontent = raw_bytes.decode("utf-8", errors="replace")

    try:
        plaintext = decode_email_body(rawcontent)
    except:
        plaintext = "Decode error"

    cursor.execute(
        """
        INSERT INTO emailcontent
        (UID, plaintext, subject, email_date, status)
        VALUES (%s,%s,%s,%s,'pending')
        """,
        (uid, plaintext, subject.strip(), email_date),
    )
    db.commit()
    logging.info(f"✅ Stored UID {uid}: {subject[:60]}")


# ============================================================================
# UID RECOVERY MODE
# ============================================================================
def download_specific_uids(target_uids):
    logging.info(f"🎯 Forced UID download: {len(target_uids)}")

    pop_conn = connect_pop3()
    db = connect_mysql()
    if not pop_conn or not db:
        return

    cursor = db.cursor()

    uid_map = {}
    messages = pop_conn.uidl()[1]

    for entry in messages:
        parts = entry.decode().split()
        if len(parts) >= 2:
            uid_map[parts[1]] = int(parts[0])

    logging.info(f"📬 Server has {len(uid_map)} UIDs")

    for uid in target_uids:
        if is_uid_downloaded(cursor, uid):
            logging.info(f"⏭ UID {uid} already exists")
            continue

        if uid not in uid_map:
            logging.warning(f"❌ UID {uid} not on server")
            continue

        index = uid_map[uid]

        try:
            _, lines, _ = pop_conn.retr(index)
            raw_bytes = b"\r\n".join(lines)
            decode_and_store(cursor, db, uid, raw_bytes)
        except Exception as e:
            logging.error(f"UID {uid} failed: {e}")

    cursor.close()
    db.close()
    pop_conn.quit()
    logging.info("🎯 Forced UID download complete")


# ============================================================================
# NORMAL FILTER MODE (UNCHANGED CORE)
# ============================================================================
def filter_and_download():
    pop_conn = connect_pop3()
    db = connect_mysql()
    if not pop_conn or not db:
        return

    cursor = db.cursor()

    messages = pop_conn.uidl()[1]
    logging.info(f"📬 {len(messages)} messages")

    for entry in tqdm(messages, desc="Filtering"):
        try:
            parts = entry.decode().split()
            if len(parts) < 2:
                continue

            index = int(parts[0])
            uid = parts[1]

            if is_uid_downloaded(cursor, uid):
                continue

            _, lines, _ = pop_conn.retr(index)
            raw_bytes = b"\r\n".join(lines)

            decode_and_store(cursor, db, uid, raw_bytes)

        except Exception as e:
            logging.error(f"Process error: {e}")

    cursor.close()
    db.close()
    pop_conn.quit()
    logging.info("✅ Filter cycle done")


# ============================================================================
# MAIN ENTRY
# ============================================================================
if __name__ == "__main__":

    # ===== ONE-OFF UID RECOVERY =====
    TARGET_UIDS = [
        "20260130043008eucms1p30563c05606f13b2f90f118e5acea457aCC20260130043009158",
        "20260130043253epcms1p64d1566ebd4e72c6cd1f5557bd85c6fb4CC20260130043257082",
        "20260130043446epcms5p5e89d8cea38ac869aa4ada0a5f280791aCC20260130043448786",
        "20260130043825epcms1p7e7dee8e88346e0a5a046d360575d2cf6CC20260130043829498",
        "20260130044400epcms5p456a59768f87f65c0fb81862117828767CC20260130044403289",
        "20260130055245epcms1p297585680c12a863344295c012b4ff0d3CC20260130055248551",
        "20260202043053eucms1p35d5a5a337b8774fb5be8cfdb8975ed71CC20260202043054087",
        "20260202131942eucms1p4d9a697475fafd81c35b1694a962dbe08CC20260202131943801",
        "20260203042354eucms1p326ac4c9b9e193ed4169d4fd7ad78215dCC20260203042354927",
        "20260203044125eucas1p25cd5848acbb0c765e53b044f1e40e67eCC20260203044125785",
        "20260203044128eucas1p11b6e072c8b3583b37aa2efc9bf571402CC20260203044128666",
        "20260203044133eucas1p2e31aa4366352db2c53d515c555f95036CC20260203044133931",
        "20260203044140eucas1p2ccdb1e324167ad97df38eea18917b37dCC20260203044140704",
        "20260203044144eucas1p2d7f47857b63bdde99351ce54b701813cCC20260203044144712",
        "20260203044149eucas1p17e80f87bf4f42cb6dd5afae67e8761f5CC20260203044149517",
        "20260203044153eucas1p1e4001c5429f6113641513cf6608da470CC20260203044153510",
        "20260203044157eucas1p2a90183ce458f5c961ec03e8267af15d9CC20260203044157692",
        "20260203044203eucas1p1780d6a3982d52541098229ecb2a6469eCC20260203044204056",
    ]

    download_specific_uids(TARGET_UIDS)
    sys.exit(0)

    # ===== NORMAL SCHEDULER =====
    INTERVAL_MINUTES = 5
    while True:
        filter_and_download()
        time.sleep(INTERVAL_MINUTES * 60)
