from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class PackageTests(unittest.TestCase):
    def test_required_portable_files_exist(self):
        required = [
            "app.yaml",
            "requirements.txt",
            "sample_names_test.csv",
            "app/app.py",
            "notebooks/00_RUN_ME_INSTALL_AND_DEPLOY.py",
            "notebooks/01_BATCH_NAME_PIPELINE.py",
            "notebooks/02_RETRY_AND_VALIDATE.py",
            "notebooks/03_ACCEPTANCE_TESTS.py",
        ]
        self.assertFalse([path for path in required if not (ROOT / path).is_file()])

    def test_no_workspace_identity_is_hardcoded(self):
        text = "\n".join(
            path.read_text(encoding="utf-8")
            for path in ROOT.rglob("*.py")
            if "tests" not in path.parts
        )
        self.assertNotIn("adb-", text)
        self.assertNotIn("azuredatabricks.net", text)
        self.assertNotIn("cloud.databricks.com", text)
        self.assertNotIn("dapi", text)
