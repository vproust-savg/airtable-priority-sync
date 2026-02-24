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
