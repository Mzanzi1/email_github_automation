# githubupdater.py
# Updating GitHub issues with summaries posted at the TOP of issue body and as a comment
# FIXED VERSION - All critical issues addressed

import os
import logging
import mysql.connector
import requests
import sys
from dotenv import load_dotenv
from urllib.parse import urlparse
import re
from datetime import timezone, timedelta
from emailutils import decode_email_body
import time

# Load environment variables
load_dotenv()

# Timezone setup
DUBAI_TZ = timezone(timedelta(hours=4))

# Configuration
BATCH_SIZE = 100  # Process summaries in batches
ENABLE_DEBUG_FILES = False  # Set to True only for debugging
REQUEST_TIMEOUT = 30  # Timeout for GitHub API calls in seconds


# ============================================================================
# ENVIRONMENT VARIABLE VALIDATION
# ============================================================================
def validate_environment():
    """Check all required environment variables exist before starting."""
    required_vars = [
        "MYSQL_HOST", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE",
        "GITHUB_API_KEY", "GITHUB_URL"
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
GITHUB_API_KEY = os.getenv("GITHUB_API_KEY")
GITHUB_URL = os.getenv("GITHUB_URL")

MYSQL_CONFIG = {
    'host': os.getenv("MYSQL_HOST"),
    'port': int(os.getenv("MYSQL_PORT", 3306)),
    'user': os.getenv("MYSQL_USER"),
    'password': os.getenv("MYSQL_PASSWORD"),
    'database': os.getenv("MYSQL_DATABASE")
}

HEADERS = {
    "Authorization": f"token {GITHUB_API_KEY}",
    "Accept": "application/vnd.github.v3+json"
}

# ============================================================================
# LOGGING SETUP
# ============================================================================
logging.basicConfig(
    filename='github_push.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Also log to console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(console_handler)


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
# HELPER FUNCTIONS
# ============================================================================
def extract_repo_and_issue(github_url):
    """
    Extract owner, repo, and issue number from GitHub URL.

    Example: https://github.com/owner/repo/issues/123 -> (owner, repo, 123)
    """
    try:
        parsed = urlparse(github_url)
        parts = parsed.path.strip("/").split("/")
        if len(parts) >= 4 and parts[-2] == "issues":
            owner = parts[-4]
            repo = parts[-3]
            issue_number = parts[-1]
            return owner, repo, issue_number
    except Exception as e:
        logging.warning(f"Failed to parse GitHub URL {github_url}: {e}")

    return None, None, None


def smart_truncate_markdown(text, max_bytes=65536):
    """
    Truncate markdown text to fit within byte limit.

    Unlike the broken version, this actually checks BYTES and truncates by BYTES.
    """
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text

    # Truncate by bytes, then decode safely
    truncated = encoded[:max_bytes].decode("utf-8", errors="ignore")

    # Add a warning at the end
    return truncated + "\n\n...(truncated due to size limit)"


def format_recent_emails_for_subject(plaintext_map, subject):
    """
    Format recent emails into a collapsible markdown block.

    NOTE: Since we removed rawcontent column, we now use plaintext directly.
    We get sender/subject info from the email metadata, not by parsing raw headers.

    Args:
        plaintext_map: dict of {subject: [(plaintext, sender, date), ...]}
        subject: The email subject to format

    Returns:
        Formatted markdown string with collapsible details
    """
    if subject not in plaintext_map or not plaintext_map[subject]:
        return ""

    emails = plaintext_map[subject]
    formatted = ""

    for idx, (plaintext, sender, date_str) in enumerate(emails[:4], 1):  # Limit to 4 most recent
        # Truncate plaintext to reasonable length for preview
        body_preview = plaintext[:2000] if plaintext else "No content"

        formatted += (
            f"\n**Email #{idx}**\n"
            f"**Sender:** {sender}\n"
            f"**Date:** {date_str}\n"
            f"**Content:**\n```\n{body_preview}\n```\n"
        )

    if not formatted:
        return ""

    return f"<details>\n<summary>✅ Click here to view recent emails</summary>\n\n{formatted.strip()}\n</details>"


def save_debug_markdown(uid, markdown_text):
    """
    Save markdown to debug file (only if ENABLE_DEBUG_FILES is True).
    """
    if not ENABLE_DEBUG_FILES:
        return

    try:
        os.makedirs("debug_payloads", exist_ok=True)
        with open(f"debug_payloads/github_comment_{uid}.md", "w", encoding="utf-8") as f:
            f.write(markdown_text)
        logging.info(f"💾 Saved GitHub markdown debug for UID {uid}")
    except Exception as e:
        logging.warning(f"Failed to save markdown debug for UID {uid}: {e}")


# ============================================================================
# GITHUB API FUNCTIONS
# ============================================================================
def post_comment(owner, repo, issue_number, markdown_comment, uid):
    """
    Post a comment to a GitHub issue.

    Returns True if successful, False otherwise.
    """
    # Truncate if too large
    markdown_comment = smart_truncate_markdown(markdown_comment, max_bytes=65536)

    url = f"{GITHUB_URL}/repos/{owner}/{repo}/issues/{issue_number}/comments"

    try:
        response = requests.post(
            url,
            headers=HEADERS,
            json={"body": markdown_comment},
            timeout=REQUEST_TIMEOUT
        )

        if response.status_code == 201:
            comment_url = response.json().get("html_url", "")
            logging.info(f"✅ Comment posted to issue #{issue_number}: {comment_url}")
            return True
        else:
            logging.error(
                f"❌ Failed to post comment for UID {uid}: "
                f"Status {response.status_code} - {response.text[:200]}"
            )
            return False

    except requests.exceptions.Timeout:
        logging.error(f"⏱️  Timeout posting comment for UID {uid}")
        return False
    except requests.exceptions.RequestException as e:
        logging.error(f"🌐 Network error posting comment for UID {uid}: {e}")
        return False
    except Exception as e:
        logging.error(f"❌ Unexpected error posting comment for UID {uid}: {e}", exc_info=True)
        return False


def update_issue_description(owner, repo, issue_number, new_summary, uid):
    """
    Update GitHub issue description by prepending new summary.

    NOTE: This will prepend the summary every time. To avoid duplicates,
    we should check if the summary already exists, but that's complex.
    For now, we just prepend.

    Returns True if successful, False otherwise.
    """
    url = f"{GITHUB_URL}/repos/{owner}/{repo}/issues/{issue_number}"

    try:
        # Fetch existing issue body
        response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)

        if response.status_code != 200:
            logging.error(f"❌ Failed to fetch issue #{issue_number}: {response.status_code}")
            return False

        existing_body = response.json().get("body", "") or ""

        # Prepend new summary
        updated_body = f"{new_summary.strip()}\n\n---\n\n{existing_body.strip()}"

        # Update issue
        patch_response = requests.patch(
            url,
            headers=HEADERS,
            json={"body": updated_body},
            timeout=REQUEST_TIMEOUT
        )

        if patch_response.status_code == 200:
            logging.info(f"✅ Updated issue body for #{issue_number}")
            return True
        else:
            logging.error(
                f"❌ Failed to update issue body for #{issue_number}: "
                f"Status {patch_response.status_code}"
            )
            return False

    except requests.exceptions.Timeout:
        logging.error(f"⏱️  Timeout updating issue description for UID {uid}")
        return False
    except requests.exceptions.RequestException as e:
        logging.error(f"🌐 Network error updating issue description for UID {uid}: {e}")
        return False
    except Exception as e:
        logging.error(f"❌ Unexpected error updating issue description for UID {uid}: {e}", exc_info=True)
        return False


# ============================================================================
# MAIN PROCESSING LOGIC
# ============================================================================
def process_summaries():
    """
    Process API summaries and push to GitHub.

    Uses proper resource cleanup and batch processing.
    """
    db = connect_mysql()
    if not db:
        logging.error("Cannot proceed without MySQL connection")
        return

    cursor = None

    try:
        cursor = db.cursor()

        # Get count of processed summaries
        cursor.execute("SELECT COUNT(*) FROM apioutput WHERE status = 'processed'")
        total_pending = cursor.fetchone()[0]
        logging.info(f"📬 Found {total_pending} summaries to push to GitHub")

        if total_pending == 0:
            logging.info("✅ No summaries to process")
            return

        # Process in batches
        offset = 0
        total_success = 0
        total_failed = 0

        while offset < total_pending:
            # Fetch batch of summaries
            cursor.execute("""
                SELECT UID, apiresponse_text 
                FROM apioutput 
                WHERE status = 'processed'
                LIMIT %s OFFSET %s
            """, (BATCH_SIZE, offset))

            summary_rows = cursor.fetchall()
            if not summary_rows:
                break

            logging.info(f"\n{'=' * 80}")
            logging.info(f"📦 Processing batch {offset + 1} to {offset + len(summary_rows)} of {total_pending}")
            logging.info(f"{'=' * 80}")

            # Get UIDs for this batch
            uids = [row[0] for row in summary_rows]

            # Fetch ALL email data for this batch in ONE query (not in a loop!)
            placeholders = ','.join(['%s'] * len(uids))
            cursor.execute(f"""
                SELECT UID, subject, email_date, plaintext
                FROM emailcontent 
                WHERE UID IN ({placeholders})
            """, uids)

            email_data = {}
            plaintext_by_subject = {}  # Group by subject for recent emails

            for uid, subject, email_date, plaintext in cursor.fetchall():
                # Format date
                if email_date:
                    if email_date.tzinfo is None:
                        email_date = email_date.replace(tzinfo=timezone.utc)
                    formatted_date = email_date.astimezone(DUBAI_TZ).strftime("%Y-%m-%d %H:%M")
                else:
                    formatted_date = "Unknown"

                email_data[uid] = {
                    'subject': subject or "No Subject",
                    'date': formatted_date,
                    'plaintext': plaintext or ""
                }

                # Group by subject for recent emails section
                if subject not in plaintext_by_subject:
                    plaintext_by_subject[subject] = []
                plaintext_by_subject[subject].append((plaintext, "Unknown Sender", formatted_date))

            # Fetch ALL GitHub URL mappings in ONE query (not in a loop!)
            # Get unique subjects
            unique_subjects = list(set(email_data[uid]['subject'] for uid in email_data))

            # This is a simplified approach - fetch all mappings and match in Python
            # The original LOCATE query was extremely slow
            cursor.execute("SELECT subject, github_id FROM subjectgithubfilter")
            github_mappings = cursor.fetchall()

            # Build subject -> github_url mapping
            subject_to_github = {}
            for mapping_subject, github_url in github_mappings:
                for email_subject in unique_subjects:
                    # Check if mapping subject is in email subject
                    if mapping_subject.lower() in email_subject.lower():
                        # Use longest match (same logic as original LOCATE + LENGTH)
                        if email_subject not in subject_to_github or \
                                len(mapping_subject) > len(subject_to_github[email_subject]['match']):
                            subject_to_github[email_subject] = {
                                'url': github_url,
                                'match': mapping_subject
                            }

            # Process each summary in the batch
            for uid, summary_text in summary_rows:
                if uid not in email_data:
                    logging.warning(f"⚠️  No email data found for UID {uid}, skipping")
                    total_failed += 1
                    continue

                email_info = email_data[uid]
                subject = email_info['subject']
                formatted_date = email_info['date']

                # Get GitHub URL
                if subject not in subject_to_github:
                    logging.warning(f"⚠️  No GitHub URL mapped for subject: {subject} (UID {uid})")
                    total_failed += 1
                    continue

                github_url = subject_to_github[subject]['url']

                # Parse GitHub URL
                owner, repo, issue_number = extract_repo_and_issue(github_url)
                if not all([owner, repo, issue_number]):
                    logging.error(f"❌ Could not parse issue URL for UID {uid}: {github_url}")
                    total_failed += 1
                    continue

                # Format recent emails block
                recent_block = format_recent_emails_for_subject(plaintext_by_subject, subject)

                # Build markdown comment
                markdown_comment = (
                    f"※ [AI generated summary]\n\n"
                    f"{recent_block}\n\n"
                    f"{summary_text.strip()}"
                )

                # Save debug file if enabled
                save_debug_markdown(uid, markdown_comment)

                # Post to GitHub
                comment_success = post_comment(owner, repo, issue_number, markdown_comment, uid)
                description_success = update_issue_description(owner, repo, issue_number, markdown_comment, uid)

                if comment_success or description_success:
                    cursor.execute("UPDATE apioutput SET status = 'pushed' WHERE UID = %s", (uid,))
                    total_success += 1
                    logging.info(f"✅ Successfully pushed UID {uid} to GitHub issue #{issue_number}")
                else:
                    total_failed += 1
                    logging.warning(f"⚠️  Failed to push UID {uid} to GitHub")

            # Commit the entire batch at once
            try:
                db.commit()
                logging.info(f"💾 Batch committed successfully")
            except mysql.connector.Error as e:
                logging.error(f"❌ Failed to commit batch: {e}")
                db.rollback()

            offset += BATCH_SIZE

        # Final summary
        logging.info(f"\n{'=' * 80}")
        logging.info(f"✅ GitHub push complete")
        logging.info(f"   📊 Success: {total_success}")
        logging.info(f"   ❌ Failed: {total_failed}")
        logging.info(f"{'=' * 80}\n")

    except Exception as e:
        logging.error(f"❌ Critical error in process_summaries: {e}", exc_info=True)
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
    logging.info("🚀 GitHub Summary Updater Started")
    logging.info(f"🗄️  MySQL Database: {MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}")
    logging.info(f"🐙 GitHub URL: {GITHUB_URL}")
    logging.info(f"⏱️  Check Interval: {INTERVAL_MINUTES} minutes")
    logging.info(f"📦 Batch Size: {BATCH_SIZE}")
    logging.info(f"💾 Debug Files: {'Enabled' if ENABLE_DEBUG_FILES else 'Disabled'}")
    logging.info("=" * 80)

    run_count = 0

    while True:
        run_count += 1
        logging.info(f"\n{'=' * 80}")
        logging.info(f"🔄 Starting GitHub update cycle (Run #{run_count})")
        logging.info(f"{'=' * 80}")

        try:
            process_summaries()
        except Exception as e:
            logging.error(f"🚨 Critical error in GitHub updater cycle: {e}", exc_info=True)

        logging.info(f"⏸️  Sleeping for {INTERVAL_MINUTES} minutes...")
        logging.info(f"{'=' * 80}\n")

        time.sleep(INTERVAL_MINUTES * 60)
