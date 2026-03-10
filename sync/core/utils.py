"""
Utilities for value extraction, text cleaning, and price formatting.
Patterns adapted from tools/10. Script for Product All v8.py and .tmp/convert_to_python.py.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any


# ── Mojibake replacements (common encoding artifacts) ────────────────────────
MOJIBAKE_MAP = {
    "\u00e2\u0080\u0099": "'",   # '
    "\u00e2\u0080\u009c": '"',   # "
    "\u00e2\u0080\u009d": '"',   # "
    "\u00e2\u0080\u0093": "-",   # –
    "\u00e2\u0080\u0094": "-",   # —
    "\u00c3\u00a9": "e",         # é
    "\u00c3\u00a8": "e",         # è
    "\u00c3\u00bc": "u",         # ü
    "\u00c3\u00b1": "n",         # ñ
}

# Zero-width and invisible characters to strip
INVISIBLE_PATTERN = re.compile(
    r"[\u200b\u200c\u200d\u2060\ufeff\u00ad\u034f\u180e]"
)


def extract_value(value: Any) -> Any:
    """
    Extract a usable value from Airtable's complex field types.

    Handles:
    - None / empty → None
    - Lists (lookups/linked records): single-element → unwrap, multi → join with ", "
    - Dicts (AI fields): extract 'value' key
    - Strings, numbers, bools: pass through
    """
    if value is None:
        return None

    if isinstance(value, list):
        if len(value) == 0:
            return None
        if len(value) == 1:
            return extract_value(value[0])
        # Multi-element list: join as strings
        parts = [str(extract_value(item)) for item in value if item is not None]
        return ", ".join(parts) if parts else None

    if isinstance(value, dict):
        # AI-generated fields have {"value": "...", "state": "generated"}
        if "value" in value:
            return value["value"]
        # Fallback: return string representation
        return str(value)

    return value


def clean_mojibake(text: str) -> str:
    """Replace common mojibake patterns with correct characters."""
    for bad, good in MOJIBAKE_MAP.items():
        text = text.replace(bad, good)
    return text


def clean_invisible(text: str) -> str:
    """Remove zero-width and invisible characters, normalize whitespace."""
    text = INVISIBLE_PATTERN.sub("", text)
    text = unicodedata.normalize("NFC", text)
    # Collapse multiple spaces
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clean(value: Any) -> str | None:
    """
    Full cleaning pipeline: extract → mojibake fix → invisible chars → strip.
    Returns None if the result is empty.
    """
    extracted = extract_value(value)
    if extracted is None:
        return None

    text = str(extracted).strip()
    if not text:
        return None

    text = clean_mojibake(text)
    text = clean_invisible(text)

    # Escape tabs and newlines (from v8 script pattern)
    text = text.replace("\t", " ").replace("\n", " ").replace("\r", " ")

    return text if text else None


def format_price(value: Any) -> float | None:
    """
    Extract a numeric value and format to 2 decimal places.
    Returns a float (not string) for JSON serialization to Priority.
    Returns None if the value is empty or not numeric.
    """
    extracted = extract_value(value)
    if extracted is None:
        return None

    text = str(extracted).strip()
    if not text:
        return None

    # Remove currency symbols and commas
    text = re.sub(r"[$,]", "", text)

    try:
        return round(float(text), 2)
    except (ValueError, TypeError):
        return None


def to_int(value: Any) -> int | None:
    """
    Extract an integer value.
    Returns None if the value is empty or not numeric.
    """
    extracted = extract_value(value)
    if extracted is None:
        return None

    text = str(extracted).strip()
    if not text:
        return None

    try:
        return int(float(text))
    except (ValueError, TypeError):
        return None


def to_float(value: Any) -> float | None:
    """
    Extract a numeric value as a float.
    Used for Airtable number fields that receive string values from Priority.
    Returns None if the value is empty or not numeric.
    """
    extracted = extract_value(value)
    if extracted is None:
        return None

    text = str(extracted).strip()
    if not text:
        return None

    try:
        return float(text)
    except (ValueError, TypeError):
        return None


def day_to_priority_int(day: str) -> int:
    """
    Convert a day name to Priority's integer WEEKDAY value.
    Priority uses: 1=Sunday, 2=Monday, …, 7=Saturday.

    Used for CUSTWEEKDAY_SUBFORM — Priority stores and returns WEEKDAY
    as an integer, so we must send integers for both writes AND comparisons
    (critical for GET+compare optimization to avoid false positives).

    Examples:
        "Monday" → 2, "Wednesday" → 4, "Friday" → 6
        "mon" → 2 (case-insensitive)
        "Mon" → 2 (abbreviations work too)
    """
    DAY_MAP = {
        "sunday": 1,    "sun": 1,
        "monday": 2,    "mon": 2,
        "tuesday": 3,   "tue": 3,
        "wednesday": 4, "wed": 4,
        "thursday": 5,  "thu": 5,
        "friday": 6,    "fri": 6,
        "saturday": 7,  "sat": 7,
    }
    key = day.strip().lower()
    if key in DAY_MAP:
        return DAY_MAP[key]
    # Try to parse as integer already (e.g., "5" → 5)
    try:
        return int(day.strip())
    except (ValueError, TypeError):
        raise ValueError(f"Unknown day: {day!r}")


def format_time_24h(time_str: str) -> str:
    """
    Convert 12-hour AM/PM time to 24-hour format.
    Used for CUSTWEEKDAY_SUBFORM Deliver After / Deliver Before.

    Examples:
        "2:00 PM" → "14:00"
        "12:00 AM" → "00:00"
        "12:00 PM" → "12:00"
        "9:30 AM" → "09:30"

    Returns original string if format doesn't match.
    """
    import re as _re

    match = _re.match(r"(\d{1,2}):(\d{2})\s*(AM|PM)", time_str.strip(), _re.IGNORECASE)
    if not match:
        return time_str.strip()

    hour = int(match.group(1))
    minute = match.group(2)
    period = match.group(3).upper()

    if period == "AM":
        if hour == 12:
            hour = 0
    else:  # PM
        if hour != 12:
            hour += 12

    return f"{hour:02d}:{minute}"


def format_date_mmddyyyy(value: Any) -> str | None:
    """
    Format a date string to MM/DD/YYYY.
    Used for date fields in price lists and special prices.

    Tries parsing: YYYY-MM-DD, MM/DD/YYYY, MM/DD/YY
    Returns None if value is empty.
    Returns original string if parsing fails.
    """
    from datetime import datetime as _dt

    extracted = extract_value(value)
    if extracted is None:
        return None
    text = str(extracted).strip()
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            parsed = _dt.strptime(text[:10], fmt)
            return parsed.strftime("%m/%d/%Y")
        except ValueError:
            continue

    return text  # Return as-is if no format matched


def to_priority_date(value: Any) -> str | None:
    """
    Convert a date string to Priority's ISO 8601 format (YYYY-MM-DDT00:00:00Z).

    Uses 'Z' suffix to match Priority's response format exactly, so that
    string comparison during GET+compare optimization works correctly.

    Handles: MM/DD/YY, MM/DD/YYYY, YYYY-MM-DD
    Returns None if value is empty.
    """
    from datetime import datetime as _dt

    extracted = extract_value(value)
    if extracted is None:
        return None
    text = str(extracted).strip()
    if not text:
        return None

    for fmt in ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            parsed = _dt.strptime(text[:10], fmt)
            return parsed.strftime("%Y-%m-%dT00:00:00Z")
        except ValueError:
            continue

    return text  # Return as-is if no format matched


def priority_yn(value: Any) -> str | None:
    """
    Convert Priority Y/N values to Airtable Yes/No.

    Priority stores boolean-like fields as "Y" / "N".
    Airtable uses "Yes" / "No".
    Non-Y/N values pass through unchanged (safe as a universal transform).

    Returns None if the value is empty.
    """
    v = clean(value)
    if v is None:
        return None
    if v == "Y":
        return "Yes"
    if v == "N":
        return "No"
    return v  # pass through other values (e.g. already "Yes"/"No")


def reverse_yn(value: Any) -> str | None:
    """
    Convert Airtable Yes/No values to Priority Y/N.

    Airtable singleSelect fields use "Yes" / "No".
    Priority stores boolean-like fields as "Y" / "N".
    Non-Yes/No values pass through unchanged.

    Returns None if the value is empty.
    """
    v = clean(value)
    if v is None:
        return None
    if v == "Yes":
        return "Y"
    if v == "No":
        return "N"
    return v  # pass through other values (e.g. already "Y"/"N")


def strip_html(text: str) -> str:
    """
    Strip HTML tags and normalize whitespace for text comparison.

    Priority wraps TEXT sub-form fields in HTML styling after PATCH
    (e.g., <style>...</style><p><p dir=ltr>actual text<br></p></p>).
    This function strips all that to get the plain text for comparison,
    so GET+compare optimization doesn't trigger false positives.
    """
    import re as _re

    result = _re.sub(r"<style[^>]*>.*?</style>", "", text, flags=_re.DOTALL)  # Remove <style> blocks
    result = _re.sub(r"<[^>]+>", "", result)         # Remove HTML tags
    result = result.replace("&nbsp;", " ")           # Normalize HTML spaces
    result = result.replace("&amp;", "&")            # Decode ampersands
    result = result.replace("&lt;", "<")             # Decode less-than
    result = result.replace("&gt;", ">")             # Decode greater-than
    result = _re.sub(r"\s+", " ", result).strip()    # Collapse whitespace
    return result


def values_equal(a: Any, b: Any) -> bool:
    """Type-aware comparison for GET+compare optimization.

    Handles the key mismatch: Python float(5.0) vs JSON int(5).
    If either side is numeric, compare as floats with epsilon tolerance.
    Otherwise compare as stripped strings.
    """
    EPSILON = 0.001
    if a is None and b is None:
        return True
    if a is None or b is None:
        a_str = "" if a is None else str(a).strip()
        b_str = "" if b is None else str(b).strip()
        return a_str == b_str
    if isinstance(a, (int, float)) or isinstance(b, (int, float)):
        try:
            return abs(float(a) - float(b)) < EPSILON
        except (ValueError, TypeError):
            pass
    return str(a).strip() == str(b or "").strip()
