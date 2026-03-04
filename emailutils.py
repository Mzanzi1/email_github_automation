# emailutils.py
# FIXED VERSION - Proper email body decoding without double-decoding
import email
import re
from bs4 import BeautifulSoup

# Configuration
MAX_BODY_LENGTH = 4000  # Maximum characters to return (adjust based on your DB column size)


def smart_truncate(text, max_length=MAX_BODY_LENGTH):
    """
    Truncate text intelligently at word boundaries.

    Like you're 5: Instead of cutting a sentence in half like "The cat is very fu..."
    we cut at spaces like "The cat is very..."
    """
    if len(text) <= max_length:
        return text

    truncated = text[:max_length]
    last_space = truncated.rfind(' ')

    # Only truncate at space if it's reasonably close to the limit (80%)
    if last_space > max_length * 0.8:
        return truncated[:last_space] + "..."

    return truncated + "..."


def decode_email_body(rawcontent: str) -> str:
    """
    Extract and decode the body of an email from raw content.

    Prefers plaintext over HTML. Falls back gracefully if parsing fails.

    Args:
        rawcontent: Raw email content as a string

    Returns:
        Decoded email body (truncated to MAX_BODY_LENGTH characters)
    """
    try:
        msg = email.message_from_string(rawcontent)
        plaintext = ""
        htmltext = ""

        if msg.is_multipart():
            # Email has multiple parts (text, html, attachments, etc.)
            for part in msg.walk():
                ctype = part.get_content_type()

                # Skip non-text parts
                if not ctype.startswith('text/'):
                    continue

                # Get the payload
                # decode=True handles quoted-printable and base64 automatically
                payload = part.get_payload(decode=True)

                if not payload:
                    continue

                # Get charset, default to utf-8 if not specified
                charset = part.get_content_charset() or "utf-8"

                try:
                    # Decode bytes to string
                    # errors="replace" means: if you can't decode a byte, replace it with �
                    # This prevents crashes on weird encodings
                    decoded = payload.decode(charset, errors="replace")

                    # Grab the first text/plain part we find
                    if ctype == "text/plain" and not plaintext:
                        plaintext = decoded

                    # Grab the first text/html part we find
                    elif ctype == "text/html" and not htmltext:
                        htmltext = decoded

                except (UnicodeDecodeError, LookupError, AttributeError):
                    # LookupError: unknown encoding
                    # AttributeError: payload was None somehow
                    continue

        else:
            # Simple email with just one part
            payload = msg.get_payload(decode=True)

            if payload:
                charset = msg.get_content_charset() or "utf-8"
                try:
                    plaintext = payload.decode(charset, errors="replace")
                except (UnicodeDecodeError, LookupError):
                    plaintext = payload.decode("utf-8", errors="replace")

        # Return the best content we found, in order of preference

        # 1. Prefer plaintext if we found it
        if plaintext.strip():
            return smart_truncate(plaintext.strip())

        # 2. Fall back to HTML, but strip tags first
        elif htmltext.strip():
            try:
                soup = BeautifulSoup(htmltext, "html.parser")
                text = soup.get_text(separator="\n", strip=True)
                if text.strip():
                    return smart_truncate(text)
            except Exception:
                # BeautifulSoup parsing failed, just return message
                pass

        # 3. If we got nothing, return a clear message
        return "No readable body found."

    except (UnicodeDecodeError, LookupError, ValueError) as e:
        # Only catch specific exceptions related to decoding
        return f"Failed to decode email body: {e}"
    except Exception as e:
        # Catch-all for truly unexpected errors
        return f"Unexpected error decoding email: {e}"
