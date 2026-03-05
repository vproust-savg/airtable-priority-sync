"""Unit tests for sync/core/utils.py transform functions."""

from __future__ import annotations

import pytest

from sync.core.utils import (
    abbreviate_day,
    clean,
    extract_value,
    format_date_mmddyyyy,
    format_price,
    format_time_24h,
    priority_yn,
    to_float,
    to_int,
)


# ── extract_value ──────────────────────────────────────────────────────────

class TestExtractValue:
    def test_none(self):
        assert extract_value(None) is None

    def test_empty_list(self):
        assert extract_value([]) is None

    def test_single_element_list(self):
        assert extract_value(["hello"]) == "hello"

    def test_multi_element_list(self):
        assert extract_value(["a", "b", "c"]) == "a, b, c"

    def test_dict_with_value(self):
        assert extract_value({"value": "test", "state": "generated"}) == "test"

    def test_string_passthrough(self):
        assert extract_value("hello") == "hello"

    def test_number_passthrough(self):
        assert extract_value(42) == 42

    def test_nested_list(self):
        assert extract_value([["nested"]]) == "nested"

    def test_list_with_none(self):
        assert extract_value([None]) is None


# ── clean ──────────────────────────────────────────────────────────────────

class TestClean:
    def test_none(self):
        assert clean(None) is None

    def test_empty_string(self):
        assert clean("") is None

    def test_whitespace_only(self):
        assert clean("   ") is None

    def test_normal_string(self):
        assert clean("Hello World") == "Hello World"

    def test_strips_whitespace(self):
        assert clean("  trimmed  ") == "trimmed"

    def test_list_input(self):
        assert clean(["from list"]) == "from list"

    def test_replaces_tabs_newlines(self):
        assert clean("line1\tline2\nline3") == "line1 line2 line3"

    def test_collapses_spaces(self):
        assert clean("too   many   spaces") == "too many spaces"


# ── format_price ───────────────────────────────────────────────────────────

class TestFormatPrice:
    def test_none(self):
        assert format_price(None) is None

    def test_empty_string(self):
        assert format_price("") is None

    def test_integer_string(self):
        assert format_price("100") == 100.0

    def test_float_string(self):
        assert format_price("99.99") == 99.99

    def test_with_dollar_sign(self):
        assert format_price("$49.99") == 49.99

    def test_with_commas(self):
        assert format_price("1,234.56") == 1234.56

    def test_rounds_to_2_decimals(self):
        assert format_price("10.999") == 11.0

    def test_non_numeric(self):
        assert format_price("not a number") is None

    def test_from_list(self):
        assert format_price([12.50]) == 12.5


# ── to_int ─────────────────────────────────────────────────────────────────

class TestToInt:
    def test_none(self):
        assert to_int(None) is None

    def test_empty_string(self):
        assert to_int("") is None

    def test_integer_string(self):
        assert to_int("42") == 42

    def test_float_string(self):
        assert to_int("3.7") == 3

    def test_non_numeric(self):
        assert to_int("abc") is None


# ── to_float ───────────────────────────────────────────────────────────────

class TestToFloat:
    def test_none(self):
        assert to_float(None) is None

    def test_empty_string(self):
        assert to_float("") is None

    def test_float_string(self):
        assert to_float("3.14") == 3.14

    def test_integer_string(self):
        assert to_float("100") == 100.0

    def test_non_numeric(self):
        assert to_float("abc") is None


# ── priority_yn ────────────────────────────────────────────────────────────

class TestPriorityYN:
    def test_none(self):
        assert priority_yn(None) is None

    def test_y(self):
        assert priority_yn("Y") == "Yes"

    def test_n(self):
        assert priority_yn("N") == "No"

    def test_yes_passthrough(self):
        assert priority_yn("Yes") == "Yes"

    def test_no_passthrough(self):
        assert priority_yn("No") == "No"

    def test_other_passthrough(self):
        assert priority_yn("Maybe") == "Maybe"


# ── abbreviate_day ─────────────────────────────────────────────────────────

class TestAbbreviateDay:
    def test_monday(self):
        assert abbreviate_day("Monday") == "Mon"

    def test_case_insensitive(self):
        assert abbreviate_day("wednesday") == "Wed"

    def test_already_abbreviated(self):
        assert abbreviate_day("Fri") == "Fri"


# ── format_time_24h ───────────────────────────────────────────────────────

class TestFormatTime24h:
    def test_pm(self):
        assert format_time_24h("2:00 PM") == "14:00"

    def test_am(self):
        assert format_time_24h("9:30 AM") == "09:30"

    def test_midnight(self):
        assert format_time_24h("12:00 AM") == "00:00"

    def test_noon(self):
        assert format_time_24h("12:00 PM") == "12:00"


# ── format_date_mmddyyyy ──────────────────────────────────────────────────

class TestFormatDateMMDDYYYY:
    def test_none(self):
        assert format_date_mmddyyyy(None) is None

    def test_iso_format(self):
        assert format_date_mmddyyyy("2026-03-05") == "03/05/2026"

    def test_us_format(self):
        assert format_date_mmddyyyy("03/05/2026") == "03/05/2026"
