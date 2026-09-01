import importlib.util
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path


def load_app():
    root = tempfile.TemporaryDirectory()
    os.environ["PM_BASE"] = str(Path(root.name) / "opt")
    os.environ["PM_DATA"] = str(Path(root.name) / "data")
    httpx = types.ModuleType("httpx")
    psycopg = types.ModuleType("psycopg")
    rows = types.ModuleType("psycopg.rows")
    rows.dict_row = object()
    psycopg.rows = rows
    sys.modules.update({"httpx": httpx, "psycopg": psycopg, "psycopg.rows": rows})
    spec = importlib.util.spec_from_file_location("pricemon_app", Path(__file__).with_name("app.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module._test_tmp = root
    return module


class AppTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def test_part_for_model_is_rejected(self):
        self.assertTrue(self.app.is_part("Уплотнение UNOX 151853 для XF043"))

    def test_device_is_not_part(self):
        self.assertFalse(self.app.is_part("Печь конвекционная UNOX XF043"))

    def test_short_code_never_uses_substring_matching(self):
        self.assertTrue(self.app.code_matches("XF043", "XF043", "Печь XF043"))
        self.assertFalse(self.app.code_matches("XF043", "XF0431", "Печь XF0431"))
        self.assertFalse(self.app.code_matches("XF043", "OTHER", "Уплотнение для XF043"))

    def test_long_code_can_match_a_small_suffix(self):
        self.assertTrue(self.app.code_matches("DB1050A0", "DB1050", "Печь DB1050"))

    def test_run_marker_is_written_after_function_call(self):
        source = Path(__file__).with_name("app.py").read_text(encoding="utf-8")
        call_at = source.index("fn(*args)")
        marker_at = source.index("Path(success_mark).write_text", call_at)
        self.assertGreater(marker_at, call_at)

    def test_run_id_is_safe_for_file_names(self):
        self.assertEqual(self.app.run_id_from("2026-09-01T04:30:00+03:00"),
                         "2026-09-01T04-30-00-03-00")

    def test_failed_push_is_queued_for_the_next_run(self):
        source = self.app.PUB / "compare.csv"
        source.write_text("Бренд;Код модели\nUNOX;XF043\n", encoding="utf-8")
        self.app.os.environ["PM_PRODUCT_CENTER_URL"] = "https://example.invalid/snapshot"
        self.app.os.environ["PM_PRODUCT_CENTER_TOKEN"] = "test-token"
        original_post, original_sleep = getattr(self.app.httpx, "post", None), self.app.time.sleep
        self.app.httpx.post = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("offline"))
        self.app.time.sleep = lambda seconds: None
        try:
            result = self.app.push_snapshot("2026-09-01T04:30:00+03:00")
        finally:
            self.app.time.sleep = original_sleep
            if original_post is None:
                del self.app.httpx.post
            else:
                self.app.httpx.post = original_post
        self.assertFalse(result["ok"])
        self.assertEqual(result["attempts"], 5)
        self.assertTrue((self.app.PUSH_QUEUE / "2026-09-01T04-30-00-03-00.json").is_file())


if __name__ == "__main__":
    unittest.main()
