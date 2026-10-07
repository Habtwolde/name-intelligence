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

    def test_llama_batch_has_safe_output_reservation_and_split_fallback(self):
        pipeline = (ROOT / "notebooks/01_BATCH_NAME_PIPELINE.py").read_text(encoding="utf-8")
        installer = (ROOT / "notebooks/00_RUN_ME_INSTALL_AND_DEPLOY.py").read_text(encoding="utf-8")
        self.assertIn("min(8000", pipeline)
        self.assertIn("def call_endpoint_once", pipeline)
        self.assertIn("return call_endpoint(batch[:midpoint])", pipeline)
        self.assertIn("MAX_CONCURRENCY = 1", installer)

    def test_app_configuration_uses_attached_resource_values(self):
        manifest = (ROOT / "app.yaml").read_text(encoding="utf-8")
        installer = (ROOT / "notebooks/00_RUN_ME_INSTALL_AND_DEPLOY.py").read_text(encoding="utf-8")
        for resource_key in ["project-volume", "sql-warehouse", "serving-endpoint", "batch-job"]:
            self.assertIn(f"valueFrom: {resource_key}", manifest)
            self.assertIn(f'"name": "{resource_key}"', installer)
        self.assertIn('"resources": app_resources', installer)
