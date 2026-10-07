from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from name_intelligence.detection import detect_name_columns, selected_columns
from name_intelligence.normalization import normalize_name, search_key, stable_name_hash
from name_intelligence.prompting import PROMPT_VERSION, SYSTEM_PROMPT, build_user_prompt
from name_intelligence.validation import validate_analysis


class CoreTests(unittest.TestCase):
    def test_normalization_preserves_diacritics_and_compounds(self):
        self.assertEqual(normalize_name("  Díaz  "), "DÍAZ")
        self.assertEqual(normalize_name("De   Jesus Rivera"), "DE JESUS RIVERA")
        self.assertEqual(search_key("DÍAZ"), "diaz")
        self.assertEqual(stable_name_hash(" Díaz "), stable_name_hash("DÍAZ"))

    def test_generic_headers_detect_only_name_columns(self):
        frame = pd.read_csv(ROOT / "sample_names_test.csv")
        profiles = detect_name_columns(frame)
        self.assertEqual(set(selected_columns(profiles)), {"FIELD_A", "FIELD_B"})

    def test_validator_deduplicates_and_caps_each_relationship_type(self):
        relationships = [
            {
                "name": f"Variant {index}",
                "relationship_type": "phonetic_variant",
                "cultural_context": "Test",
                "why": "Test relationship",
                "confidence": 0.9,
            }
            for index in range(8)
        ]
        relationships.append(dict(relationships[0]))
        cleaned, _ = validate_analysis(
            {
                "confidence": 0.9,
                "relationships": relationships,
                "other_possible_traditions": [],
                "review_required": False,
            },
            "Example",
        )
        self.assertEqual(len(cleaned["relationships"]), 5)

    def test_prompt_is_json_and_batch_is_bounded(self):
        prompt = build_user_prompt(["MOHAMMAD", "DÍAZ"])
        self.assertEqual(json.loads(prompt)["names"], ["MOHAMMAD", "DÍAZ"])
        with self.assertRaises(ValueError):
            build_user_prompt([str(i) for i in range(26)])

    def test_cognate_prompt_requires_reciprocal_family_reasoning(self):
        self.assertEqual(PROMPT_VERSION, "v3")
        self.assertIn("This relationship is reciprocal", SYSTEM_PROMPT)
        self.assertIn("Yossi or Yosi", SYSTEM_PROMPT)
        self.assertIn("Generic phrases", SYSTEM_PROMPT)
        prompt = build_user_prompt(["YOSEPH"])
        self.assertIn("query direction must not change", prompt)
