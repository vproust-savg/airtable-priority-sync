"""
Dual logging setup: clean console output + detailed log file.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from sync.core.config import LA_TIMEZONE, LOG_DIR


def setup_logging(log_dir: str = LOG_DIR) -> Path:
    """
    Configure logging with two handlers:
    - Console: WARNING+ only (we use print() for clean progress output)
    - File: DEBUG level with full detail

    Returns:
        Path to the log file.
    """
    # Ensure log directory exists
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    # Timestamped log filename in LA time
    now_la = datetime.now(timezone.utc).astimezone(LA_TIMEZONE)
    filename = f"sync_{now_la.strftime('%Y-%m-%d_%H-%M-%S')}.log"
    log_file = log_path / filename

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # Clear existing handlers
    root_logger.handlers.clear()

    # ── File handler: DEBUG level, detailed ──────────────────────────
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root_logger.addHandler(file_handler)

    # ── Console handler: WARNING+ only (progress is via print) ───────
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(
        logging.Formatter("  ⚠ %(message)s")
    )
    root_logger.addHandler(console_handler)

    return log_file


# ── Console output helpers ───────────────────────────────────────────────────

def print_banner(direction: str = "Airtable → Priority") -> None:
    """Print the sync start banner."""
    now_la = datetime.now(timezone.utc).astimezone(LA_TIMEZONE)
    print()
    print("=" * 52)
    print(f"  {direction}")
    print(f"  {now_la.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print("=" * 52)
    print()


def print_section(text: str) -> None:
    """Print a section header."""
    print(f"{text}")


def print_detail(text: str) -> None:
    """Print an indented detail line."""
    print(f"  {text}")


def print_record_line(
    index: int,
    total: int,
    key: str,
    action: str,
    detail: str = "",
) -> None:
    """Print a single record sync line."""
    idx_width = len(str(total))
    idx_str = str(index).rjust(idx_width)
    action_str = action.ljust(6)
    line = f"  [{idx_str}/{total}] {key:<12} {action_str}"
    if detail:
        line += f"  {detail}"
    print(line)


def print_conflict_line(
    key: str,
    field: str,
    source_value: object,
    target_value: object,
    resolution: str,
) -> None:
    """Print a conflict warning line."""
    src = repr(source_value) if source_value is not None else "None"
    tgt = repr(target_value) if target_value is not None else "None"
    # Truncate long values for readability
    if len(src) > 40:
        src = src[:37] + "..."
    if len(tgt) > 40:
        tgt = tgt[:37] + "..."
    print(f"    ⚠ CONFLICT  {key}  {field}: source={src}  target={tgt}  → {resolution}")


def print_summary(
    created: int,
    updated: int,
    skipped: int,
    errors: int,
    duration: str,
    conflicts: int = 0,
) -> None:
    """Print the final sync summary."""
    print()
    print("=" * 52)
    total = created + updated + skipped + errors
    line = (
        f"  COMPLETE  |  "
        f"Created: {created}  "
        f"Updated: {updated}  "
        f"Skipped: {skipped}  "
        f"Errors: {errors}"
    )
    if conflicts > 0:
        line += f"  Conflicts: {conflicts}"
    print(line)
    print(f"  Total: {total}  |  Duration: {duration}")
    print("=" * 52)
    print()
