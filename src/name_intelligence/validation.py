"""Validation and conservative cleanup of model responses."""

from __future__ import annotations

from collections import Counter
from typing import Any

from .normalization import normalize_name
from .prompting import RELATIONSHIP_TYPES


def validate_analysis(item: dict[str, Any], expected_name: str) -> tuple[dict[str, Any], list[str]]:
    issues: list[str] = []
    cleaned = dict(item or {})
    cleaned["input_name"] = expected_name
    if cleaned.get("name_form") not in {"given_name", "surname", "either", "unknown"}:
        cleaned["name_form"] = "unknown"
        issues.append("missing or invalid name form")
    cleaned["primary_name_tradition"] = str(cleaned.get("primary_name_tradition") or "Unknown")
    cleaned["ambiguity_note"] = str(
        cleaned.get("ambiguity_note")
        or "A written name alone cannot establish a person's culture or identity."
    )
    cleaned["summary"] = str(cleaned.get("summary") or "Insufficient evidence for a reliable analysis.")
    cleaned["meaning_and_etymology"] = str(cleaned.get("meaning_and_etymology") or "")
    cleaned["cultural_usage"] = str(cleaned.get("cultural_usage") or "")
    cleaned["pronunciation_note"] = str(cleaned.get("pronunciation_note") or "")

    confidence = cleaned.get("confidence", 0)
    try:
        confidence = max(0.0, min(1.0, float(confidence)))
    except (TypeError, ValueError):
        confidence = 0.0
        issues.append("invalid confidence")
    cleaned["confidence"] = confidence

    relationships = cleaned.get("relationships") or []
    accepted: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    counts: Counter[str] = Counter()
    for relationship in relationships:
        if not isinstance(relationship, dict):
            issues.append("non-object relationship removed")
            continue
        kind = relationship.get("relationship_type")
        candidate = str(relationship.get("name", "")).strip()
        if kind not in RELATIONSHIP_TYPES or not candidate:
            issues.append("invalid relationship removed")
            continue
        key = (kind, normalize_name(candidate))
        if key in seen or counts[kind] >= 5:
            continue
        seen.add(key)
        counts[kind] += 1
        try:
            rel_confidence = max(0.0, min(1.0, float(relationship.get("confidence", 0))))
        except (TypeError, ValueError):
            rel_confidence = 0.0
        accepted.append(
            {
                "name": candidate,
                "relationship_type": kind,
                "cultural_context": str(relationship.get("cultural_context", "Unknown")),
                "why": str(relationship.get("why", "Insufficient explanation")),
                "confidence": rel_confidence,
            }
        )

    cleaned["relationships"] = accepted
    raw_coverage = cleaned.get("coverage_notes")
    raw_coverage = raw_coverage if isinstance(raw_coverage, dict) else {}
    cleaned["coverage_notes"] = {
        kind: str(raw_coverage.get(kind) or "") for kind in sorted(RELATIONSHIP_TYPES)
    }
    cleaned["other_possible_traditions"] = list(dict.fromkeys(cleaned.get("other_possible_traditions") or []))[:4]
    cleaned["review_required"] = bool(cleaned.get("review_required", False))
    if confidence < 0.70 or issues:
        cleaned["review_required"] = True
    cleaned["validation_issues"] = issues
    return cleaned, issues
