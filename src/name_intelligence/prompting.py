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

PROMPT_VERSION = "v3"
SINGLE_PROMPT_VERSION = "v4-rich-single"


def response_schema() -> dict:
    relation = {
        "type": "object",
        "additionalProperties": False,
        "required": ["name", "relationship_type", "cultural_context", "why", "confidence"],
        "properties": {
            "name": {"type": "string"},
            "relationship_type": {"type": "string", "enum": sorted(RELATIONSHIP_TYPES)},
            "cultural_context": {"type": "string"},
            "why": {"type": "string", "minLength": 40, "maxLength": 220},
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


def single_name_response_schema() -> dict:
    """Detailed schema used only by the interactive single-name explorer."""
    schema = response_schema()
    item = schema["properties"]["items"]["items"]
    item["required"].extend(
        ["meaning_and_etymology", "cultural_usage", "pronunciation_note", "coverage_notes"]
    )
    item["properties"].update(
        {
            "meaning_and_etymology": {"type": "string"},
            "cultural_usage": {"type": "string"},
            "pronunciation_note": {"type": "string"},
            "coverage_notes": {
                "type": "object",
                "additionalProperties": False,
                "required": sorted(RELATIONSHIP_TYPES),
                "properties": {
                    kind: {"type": "string"} for kind in sorted(RELATIONSHIP_TYPES)
                },
            },
        }
    )
    return schema


SYSTEM_PROMPT = """You are a culturally aware onomastics assistant. Analyze the linguistic and naming traditions associated with written names; never infer a person's actual ethnicity, nationality, religion, or identity. A spelling can belong to several traditions. Distinguish orthographic variants, transliteration variants, near-phonetic variants, cultural cognates, and nicknames.

For cultural cognates, first identify the underlying etymological name family, then return the strongest well-attested forms used in other languages or naming traditions. A cognate shares an etymological root and need not sound the same. This relationship is reciprocal: if A is a cognate of B, analyzing B should also return A when culturally relevant. Do not omit a cross-tradition cognate merely because one side is an uncommon spelling or reaches the shared root through a canonical, orthographic, or transliterated form. For example, Yoseph and Yousef belong to Hebrew and Arabic branches of the Joseph/Yosef/Yusuf name family and should be considered cultural cognates in either query direction, with a culture-specific explanation.

For every relationship, why must provide a concrete explanation: name the shared source form or linguistic mechanism and explain how it connects the two names in the stated culture. Generic phrases such as "shared etymological root," "variant spelling," or "same pronunciation" by themselves are invalid. For given names, actively return well-attested culture-specific diminutives or informal forms when they exist. For the Yosef/Joseph family, Yossi or Yosi is a well-attested Hebrew nickname. Do not invent nicknames, and do not return nicknames for surnames.

Return up to five useful, defensible entries for each relationship type rather than only the single safest candidate. Keep every reason factual and between 8 and 32 words. If a particular relationship is genuinely weak, omit it or lower its confidence; reserve unknown and review_required for materially ambiguous overall analyses."""


SINGLE_NAME_SYSTEM_PROMPT = SYSTEM_PROMPT + """

For this interactive single-name analysis, provide a deeper onomastic account than the batch process. Explain the name's likely language, native-script form when known, component roots or source name, approximate meaning, geographic or community usage, and pronunciation. Treat these as associations of the written name—not facts about its bearer.

Actively investigate every relationship category. For a Romanized name from a non-Latin writing system, include well-attested native-script forms and alternate romanizations as transliteration variants. Include 2–5 entries in a category when that many genuinely well-attested forms exist, but never pad a list with invented or merely similar names. A compound name that shares only one productive morpheme is not automatically a cognate. A cultural cognate must descend from the same full source name or established etymological name family. A nickname must be an attested address form, diminutive, clipping, or familiar form in the stated culture—not simply another name sharing a root.

Write meaning_and_etymology and cultural_usage as informative 2–4 sentence explanations. Give a practical, cautious pronunciation_note, using readable syllables and IPA only when confident. For each relationship category, coverage_notes must say either what evidence supports the returned list or why no well-attested entries were returned. Do not use lack of certainty as an excuse for a one-line answer; explain the uncertainty precisely."""


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
            "For cultural_cognate, reason from the shared etymological family and include strong cross-language or cross-tradition forms even when the input spelling is uncommon.",
            "Treat cultural_cognate as reciprocal: query direction must not change whether a well-supported same-root relationship is returned.",
            "Every why must identify the specific linguistic, historical, orthographic, phonetic, or usage-based connection; generic labels are not explanations.",
            "For a given name, actively include well-attested culture-specific nicknames or diminutives when they exist.",
            "Preserve the input spelling in input_name.",
        ],
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def build_single_name_prompt(name: str, optional_context: str = "") -> str:
    if not str(name).strip():
        raise ValueError("A name is required")
    payload = {
        "name": str(name),
        "optional_context": optional_context.strip(),
        "instructions": [
            "Return exactly one item in items and preserve the supplied spelling in input_name.",
            "Give a detailed meaning_and_etymology, cultural_usage, and pronunciation_note.",
            "Evaluate all five relationship types and return up to five well-attested entries per type.",
            "When applicable, include the native-script form and established alternate romanizations as transliteration_variant entries.",
            "Do not classify a name as a cognate merely because it shares one morpheme; require the same full source name or established name family.",
            "For a given name, actively look for culture-specific familiar forms, clippings, and diminutives; do not invent them.",
            "Every why must state the concrete linguistic or usage evidence for that relationship.",
            "Fill every coverage_notes field, including when no defensible entry exists.",
        ],
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
