"""Header-agnostic name-column detection for CSV samples."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import re
from typing import Iterable

import pandas as pd

from .normalization import looks_like_name, normalize_name


_NEGATIVE_HEADER = re.compile(
    r"(^|_)(id|key|code|date|time|status|state|amount|price|phone|email|zip|count|flag)(_|$)",
    re.IGNORECASE,
)
_POSITIVE_HEADER = re.compile(
    r"(^|_)(name|nm|surname|srnm|given|first|last|family|forename)(_|$)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ColumnProfile:
    column: str
    score: float
    selected: bool
    non_null_ratio: float
    text_ratio: float
    name_like_ratio: float
    uniqueness_ratio: float
    average_length: float
    reason: str

    def as_dict(self) -> dict:
        return asdict(self)


def _safe_ratio(num: float, den: float) -> float:
    return float(num) / float(den) if den else 0.0


def profile_column(series: pd.Series, threshold: float = 0.56) -> ColumnProfile:
    total = len(series)
    values = series.dropna()
    non_null_ratio = _safe_ratio(len(values), total)
    if values.empty:
        return ColumnProfile(str(series.name), 0.0, False, non_null_ratio, 0, 0, 0, 0, "empty")

    raw = values.astype(str).map(str.strip)
    raw = raw[raw != ""]
    if raw.empty:
        return ColumnProfile(str(series.name), 0.0, False, non_null_ratio, 0, 0, 0, 0, "blank")

    normalized = raw.map(normalize_name)
    text_ratio = normalized.map(lambda x: any(ch.isalpha() for ch in x)).mean()
    name_like_ratio = raw.map(looks_like_name).mean()
    uniqueness_ratio = normalized.nunique(dropna=True) / max(len(normalized), 1)
    average_length = normalized.map(len).mean()

    header = str(series.name)
    header_bonus = 0.08 if _POSITIVE_HEADER.search(header) else 0.0
    header_penalty = 0.45 if _NEGATIVE_HEADER.search(header) else 0.0
    cardinality_penalty = 0.25 if normalized.nunique() <= min(8, max(2, len(normalized) // 20)) else 0.0
    length_score = 1.0 if 2 <= average_length <= 45 else 0.3

    score = (
        0.12 * non_null_ratio
        + 0.18 * text_ratio
        + 0.42 * name_like_ratio
        + 0.18 * min(1.0, uniqueness_ratio * 1.5)
        + 0.10 * length_score
        + header_bonus
        - header_penalty
        - cardinality_penalty
    )
    score = round(max(0.0, min(1.0, float(score))), 4)

    reasons = []
    if name_like_ratio >= 0.8:
        reasons.append("mostly alphabetic name-shaped values")
    if uniqueness_ratio >= 0.25:
        reasons.append("useful value diversity")
    if header_bonus:
        reasons.append("header is supportive but not required")
    if header_penalty:
        reasons.append("header suggests a non-name field")
    if cardinality_penalty:
        reasons.append("very low cardinality")

    return ColumnProfile(
        column=header,
        score=score,
        selected=score >= threshold,
        non_null_ratio=round(non_null_ratio, 4),
        text_ratio=round(float(text_ratio), 4),
        name_like_ratio=round(float(name_like_ratio), 4),
        uniqueness_ratio=round(float(uniqueness_ratio), 4),
        average_length=round(float(average_length), 2),
        reason="; ".join(reasons) or "insufficient name evidence",
    )


def detect_name_columns(
    frame: pd.DataFrame,
    threshold: float = 0.56,
    sample_size: int = 5_000,
) -> list[ColumnProfile]:
    """Profile every column and rank likely name columns.

    Headers influence only a small part of the score.  The decision primarily
    comes from the actual values, allowing generic or unfamiliar headers.
    """
    if frame.empty:
        return []
    sample = frame.head(sample_size)
    profiles = [profile_column(sample[column], threshold) for column in sample.columns]
    return sorted(profiles, key=lambda item: (-item.score, item.column))


def selected_columns(profiles: Iterable[ColumnProfile]) -> list[str]:
    return [profile.column for profile in profiles if profile.selected]

