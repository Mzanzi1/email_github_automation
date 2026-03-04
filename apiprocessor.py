# apiprocessor.py
# Processes emails, sends to Samsung API with correct payload format, saves summaries
# Scheduled Interval
# FIXED VERSION - All critical issues addressed

import mysql.connector
import requests
import os
import logging
import json
import sys
from dotenv import load_dotenv
from tqdm import tqdm
from time import sleep
from datetime import datetime, timezone, timedelta
import time

# Load environment variables
load_dotenv()

# Timezone setup
DUBAI_TZ = timezone(timedelta(hours=4))

# Configuration
MAX_RETRIES = 3
RETRY_DELAY = 60  # Initial delay in seconds
BATCH_SIZE = 100  # Process emails in batches to avoid memory issues
ENABLE_DEBUG_PAYLOADS = False  # Set to True only for debugging


# ============================================================================
# ENVIRONMENT VARIABLE VALIDATION
# ============================================================================
def validate_environment():
    """Check all required environment variables exist before starting."""
    required_vars = [
        "MYSQL_HOST", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE",
        "API_URL", "API_KEY"
    ]

    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise RuntimeError(
            f"❌ Missing required environment variables: {', '.join(missing)}\n"
            f"Please check your .env file!"
        )


# Validate BEFORE loading config
validate_environment()

# Load configuration
MYSQL_CONFIG = {
    'host': os.getenv("MYSQL_HOST"),
    'port': int(os.getenv("MYSQL_PORT", 3306)),
    'user': os.getenv("MYSQL_USER"),
    'password': os.getenv("MYSQL_PASSWORD"),
    'database': os.getenv("MYSQL_DATABASE")
}

API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")

HEADERS = {
    "Content-Type": "application/json",
    "x-api-key": API_KEY
}

# ============================================================================
# LOGGING SETUP - DO THIS ONCE, APPEND MODE
# ============================================================================
logging.basicConfig(
    filename='api_process.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Also log to console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(console_handler)


# ============================================================================
# PROMPT TEMPLATE
# ============================================================================
def build_prompt(uid, subject, formatted_date, email_text):
    """
    Build the API prompt from template.

    Separated from main code for better maintainability.
    """
    return f"""You are given an email thread. Use only its contents to create a clear summary.

Summarize the email content into the specified format:
UID: {uid}
Email Subject: {subject}
Email Date: {formatted_date}
<details>
---
**1. Latest Status**
- **Date**: {formatted_date}
- **Content**: [Provide a summary of the latest updates.]

---
**2. To-Do List**
- **Assignee**: [Specify responsible person(s).]
- **Task Details**: [Describe required actions and include deadlines if specified.]

**3. History**
Provide a reverse chronological list of updates and key points.

---
**※ Summary Criteria**:
- The latest status should reflect key points from the most recent email.
- To-do lists should infer urgent tasks.
- History must be organized in reverse chronological order.

<details>
---
<summary>:globe_with_meridians: Summary in Korean</summary>

**1. 최신 현황**
- **날짜**: {formatted_date}
- **내용**: [Provide a summary of the latest updates in Korean.]

---
**2. 해야 할 일**
- **담당자**: [Specify responsible person(s) in Korean.]
- **작업 내용**: [Describe required actions and include deadlines in Korean.]

---
**3. 이력**
Provide a reverse chronological list of updates and key points in Korean.

---
**※ 요약 기준**:
- 최신 현황은 가장 간개 이면의 해외 기술 노드 사항을 반영.
- 해야 할 일은 시기한 작업으로 유추.
- 이력은 주요 요청 및 문제 제기 순으로 역순 정리.
</details>
----- EMAIL START -----
{email_text.strip()}
----- EMAIL END -----"""


# ============================================================================
# CONNECTION FUNCTIONS
# ============================================================================
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
# API CALL LOGIC
# ============================================================================
def save_debug_payload(uid, payload):
    """
    Save payload to debug file (only if ENABLE_DEBUG_PAYLOADS is True).

    Like you're 5: Save your homework to a folder so you can look at it later
    if something goes wrong. But only save it if we're in "debug mode".
    """
    if not ENABLE_DEBUG_PAYLOADS:
        return

    try:
        os.makedirs("debug_payloads", exist_ok=True)
        with open(f"debug_payloads/debug_payload_{uid}.json", "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        logging.info(f"💾 Saved debug payload for UID {uid}")
    except Exception as e:
        logging.warning(f"Failed to save debug payload for UID {uid}: {e}")


def call_api_with_retry(email_text, uid, subject, email_date):
    """
    Call Samsung API with retry logic and exponential backoff.

    Returns the API response text, or None if all retries fail.
    """
    # Format date
    if email_date:
        if email_date.tzinfo is None:
            email_date = email_date.replace(tzinfo=DUBAI_TZ)
        else:
            email_date = email_date.astimezone(DUBAI_TZ)
        formatted_date = email_date.strftime("%Y-%m-%d %H:%M")
    else:
        formatted_date = "Unknown"

    # Build prompt
    prompt = build_prompt(uid, subject, formatted_date, email_text)

    # Build payload
    payload = {
        "input_type": "text",
        "output_type": "text",
        "component_inputs": {
            "gauss2_chat_37b-Iz7v9": {
                "input_value": email_text.strip(),
                "json_mode": False,
                "parameters": json.dumps({
                    "temperature": 0.3,
                    "top_p": 0.96,
                    "extra_body": {
                        "repetition_penalty": 1.03
                    }
                }),
                "stream": False,
                "system_message": prompt
            }
        }
    }

    # Save debug payload if enabled
    save_debug_payload(uid, payload)

    # Retry loop with exponential backoff
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(API_URL, json=payload, headers=HEADERS, timeout=120)

            # Handle rate limiting
            if response.status_code == 429:
                # Exponential backoff: 60s, 120s, 240s
                wait_time = RETRY_DELAY * (2 ** (attempt - 1))
                logging.warning(
                    f"⏸️  Rate limit hit for UID {uid} (attempt {attempt}/{MAX_RETRIES}). "
                    f"Waiting {wait_time} seconds..."
                )
                sleep(wait_time)
                continue

            # Handle other HTTP errors
            elif response.status_code != 200:
                logging.error(
                    f"❌ API error for UID {uid}: Status {response.status_code}, "
                    f"Response: {response.text[:200]}"
                )
                return None

            # Parse response
            response_data = response.json()

            if "outputs" in response_data and len(response_data["outputs"]) > 0:
                outputs = response_data["outputs"][0]
                text_output = next(
                    (o for o in outputs["outputs"] if o.get("component_display_name") == "Text Output"),
                    None
                )
                if text_output and "results" in text_output:
                    result_text = text_output["results"]["text"]["text"]
                    logging.info(f"✅ Successfully processed UID {uid}")
                    return result_text

            # Response structure doesn't match expected format
            logging.warning(f"⚠️  Unexpected API response structure for UID {uid}")
            logging.debug(f"Response: {json.dumps(response_data, indent=2)[:500]}")
            return None

        except requests.exceptions.Timeout:
            logging.warning(f"⏱️  Timeout for UID {uid} (attempt {attempt}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES:
                sleep(RETRY_DELAY)
        except requests.exceptions.RequestException as e:
            logging.warning(f"🌐 Network error for UID {uid} (attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                sleep(RETRY_DELAY)
        except Exception as e:
            logging.error(f"❌ Unexpected error for UID {uid}: {e}", exc_info=True)
            return None

    logging.error(f"❌ UID {uid} failed after {MAX_RETRIES} retries")
    return None


# ============================================================================
# MAIN EMAIL PROCESSING LOGIC
# ============================================================================
def process_emails():
    """
    Process pending emails in batches.

    Uses proper resource cleanup and batch processing to avoid memory issues.
    """
    db = connect_mysql()
    if not db:
        logging.error("Cannot proceed without MySQL connection")
        return

    cursor = None

    try:
        cursor = db.cursor()

        # Get already-processed UIDs in ONE query (not in a loop)
        cursor.execute("SELECT UID FROM apioutput")
        processed_uids = set(row[0] for row in cursor.fetchall())
        logging.info(f"📋 Found {len(processed_uids)} already-processed UIDs")

        # Get count of pending emails
        cursor.execute("SELECT COUNT(*) FROM emailcontent WHERE status = 'pending'")
        total_pending = cursor.fetchone()[0]
        logging.info(f"📬 Found {total_pending} pending emails to process")

        if total_pending == 0:
            logging.info("✅ No pending emails to process")
            return

        # Process in batches to avoid memory issues
        offset = 0
        total_processed = 0
        total_errors = 0
        total_skipped = 0

        while offset < total_pending:
            # Fetch batch
            cursor.execute("""
                SELECT UID, plaintext, subject, email_date 
                FROM emailcontent 
                WHERE status = 'pending' 
                ORDER BY email_date ASC 
                LIMIT %s OFFSET %s
            """, (BATCH_SIZE, offset))

            rows = cursor.fetchall()
            if not rows:
                break

            logging.info(f"\n{'=' * 80}")
            logging.info(f"📦 Processing batch {offset + 1} to {offset + len(rows)} of {total_pending}")
            logging.info(f"{'=' * 80}")

            # Process each email in the batch
            for uid, plain, subject, email_date in tqdm(rows, desc=f"Batch {offset // BATCH_SIZE + 1}"):
                # Skip if already processed
                if uid in processed_uids:
                    logging.info(f"⏭️  UID {uid} already processed, skipping...")
                    total_skipped += 1
                    continue

                # Skip if no plaintext
                if not plain or not plain.strip():
                    logging.warning(f"⚠️  UID {uid} has no plaintext — marking as error")
                    cursor.execute("UPDATE emailcontent SET status = 'error' WHERE UID = %s", (uid,))
                    total_errors += 1
                    continue

                # Call API
                summary = call_api_with_retry(plain, uid, subject, email_date)

                if not summary:
                    # API call failed
                    cursor.execute("UPDATE emailcontent SET status = 'error' WHERE UID = %s", (uid,))
                    total_errors += 1
                    continue

                # Save to database
                try:
                    # Use INSERT ... ON DUPLICATE KEY UPDATE to handle duplicates gracefully
                    cursor.execute("""
                        INSERT INTO apioutput (UID, apiresponse_text, status)
                        VALUES (%s, %s, 'processed')
                        ON DUPLICATE KEY UPDATE 
                            apiresponse_text = VALUES(apiresponse_text),
                            status = VALUES(status)
                    """, (uid, summary))

                    cursor.execute("UPDATE emailcontent SET status = 'processed' WHERE UID = %s", (uid,))
                    total_processed += 1

                    # Add to processed set
                    processed_uids.add(uid)

                except mysql.connector.Error as e:
                    logging.error(f"❌ Database error saving UID {uid}: {e}")
                    total_errors += 1

            # Commit the entire batch at once (not after every email)
            try:
                db.commit()
                logging.info(f"💾 Batch committed successfully")
            except mysql.connector.Error as e:
                logging.error(f"❌ Failed to commit batch: {e}")
                db.rollback()

            offset += BATCH_SIZE

        # Final summary
        logging.info(f"\n{'=' * 80}")
        logging.info(f"✅ Email processing complete")
        logging.info(f"   📊 Processed: {total_processed}")
        logging.info(f"   ⏭️  Skipped: {total_skipped}")
        logging.info(f"   ❌ Errors: {total_errors}")
        logging.info(f"{'=' * 80}\n")

    except Exception as e:
        logging.error(f"❌ Critical error in process_emails: {e}", exc_info=True)
        if db:
            db.rollback()

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

        logging.info("🧹 All connections closed")


# ============================================================================
# SCHEDULER LOOP
# ============================================================================
if __name__ == "__main__":
    # Configuration
    INTERVAL_MINUTES = 5  # Change this to your desired frequency

    logging.info("=" * 80)
    logging.info("🚀 API Email Processor Started")
    logging.info(f"🗄️ MySQL Database: {MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}")
    logging.info(f"🌐 API URL: {API_URL}")
    logging.info(f"⏱️ Check Interval: {INTERVAL_MINUTES} minutes")
    logging.info(f"📦 Batch Size: {BATCH_SIZE} emails")
    logging.info(f"🔄 Max Retries: {MAX_RETRIES}")
    logging.info(f"💾 Debug Payloads: {'Enabled' if ENABLE_DEBUG_PAYLOADS else 'Disabled'}")
    logging.info("=" * 80)

    run_count = 0

    while True:
        run_count += 1
        logging.info(f"\n{'=' * 80}")
        logging.info(f"🔄 Starting API summary cycle (Run #{run_count})")
        logging.info(f"{'=' * 80}")

        try:
            process_emails()
        except Exception as e:
            logging.error(f"🚨 Critical error in API cycle: {e}", exc_info=True)

        logging.info(f"⏸️ Sleeping for {INTERVAL_MINUTES} minutes...")
        logging.info(f"{'=' * 80}\n")

        time.sleep(INTERVAL_MINUTES * 60)
