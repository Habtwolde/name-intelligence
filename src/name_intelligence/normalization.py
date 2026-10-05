"""Name-safe normalization.

The stored normalized form deliberately preserves diacritics and culturally
meaningful punctuation.  A second, lossy key is used only for search and
candidate blocking.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from typing import Any


_SPACE_RE = re.compile(r"\s+")
_SEARCH_PUNCT_RE = re.compile(r"[^\w\s'\-]", flags=re.UNICODE)


def normalize_name(value: Any) -> str:
    """Return a stable display-preserving normalized name."""
    if value is None:
        return ""
    text = unicodedata.normalize("NFKC", str(value))
    text = text.replace("\u00a0", " ").replace("’", "'").replace("`", "'")
    text = _SPACE_RE.sub(" ", text).strip()
    return text.upper()


def search_key(value: Any) -> str:
    """Return an accent-folded key for lookup, never for display."""
    normalized = normalize_name(value)
    decomposed = unicodedata.normalize("NFKD", normalized)
    without_marks = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    cleaned = _SEARCH_PUNCT_RE.sub("", without_marks.casefold())
    return _SPACE_RE.sub(" ", cleaned).strip()


def stable_name_hash(value: Any) -> str:
    """Create a deterministic cache key without exposing the raw name."""
    return hashlib.sha256(normalize_name(value).encode("utf-8")).hexdigest()


def looks_like_name(value: Any) -> bool:
    """Conservative row-level check used by column profiling."""
    text = normalize_name(value)
    if not text or len(text) > 120:
        return False
    if "@" in text or "://" in text:
        return False
    letters = sum(ch.isalpha() for ch in text)
    digits = sum(ch.isdigit() for ch in text)
    if letters == 0 or digits > 0:
        return False
    allowed = sum(ch.isalpha() or ch.isspace() or ch in "-'" for ch in text)
    return allowed / max(len(text), 1) >= 0.90

