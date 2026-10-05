"""Strict, compact prompt and response schema for name-family analysis."""

from __future__ import annotations

import json
from typing import Sequence


RELATIONSHIP_TYPES = {
    "orthographic_variant",
    "transliteration_variant",
    "phonetic_variant",
    "cultural_cognate",
    "nickname",
}


def response_schema() -> dict:
    relation = {
        "type": "object",
        "additionalProperties": False,
        "required": ["name", "relationship_type", "cultural_context", "why", "confidence"],
        "properties": {
            "name": {"type": "string"},
            "relationship_type": {"type": "string", "enum": sorted(RELATIONSHIP_TYPES)},
            "cultural_context": {"type": "string"},
            "why": {"type": "string"},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        },
    }
    item = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "input_name",
            "name_form",
            "primary_name_tradition",
            "other_possible_traditions",
            "confidence",
            "ambiguity_note",
            "relationships",
            "summary",
            "review_required",
        ],
        "properties": {
            "input_name": {"type": "string"},
            "name_form": {"type": "string", "enum": ["given_name", "surname", "either", "unknown"]},
            "primary_name_tradition": {"type": "string"},
            "other_possible_traditions": {"type": "array", "maxItems": 4, "items": {"type": "string"}},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "ambiguity_note": {"type": "string"},
            "relationships": {"type": "array", "maxItems": 25, "items": relation},
            "summary": {"type": "string"},
            "review_required": {"type": "boolean"},
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["items"],
        "properties": {"items": {"type": "array", "items": item}},
    }


SYSTEM_PROMPT = """You are a cautious onomastics assistant. Analyze the linguistic and naming traditions associated with written names; never infer a person's actual ethnicity, nationality, religion, or identity. A spelling can belong to several traditions. Distinguish orthographic variants, transliteration variants, near-phonetic variants, cultural cognates, and nicknames. A cognate shares an etymological root and need not sound the same. Return at most five entries for each relationship type. Do not invent a nickname for a surname. Keep every reason factual and under 28 words. If evidence is weak, use unknown, lower confidence, return fewer relationships, and set review_required true."""


def build_user_prompt(names: Sequence[str], optional_context: str = "") -> str:
    if not names:
        raise ValueError("At least one name is required")
    if len(names) > 25:
        raise ValueError("A prompt batch may contain at most 25 names")
    payload = {
        "names": list(names),
        "optional_context": optional_context.strip(),
        "instructions": [
            "Return exactly one item for every input name in the same order.",
            "Do not exceed five relationships of any one relationship_type.",
            "Explain why each relationship applies in its specific cultural context.",
            "Preserve the input spelling in input_name.",
        ],
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
