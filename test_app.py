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

    def policy(self):
        return {
            "version": 3, "market_multiplier": 1.075, "cost_floor_multiplier": 1.05,
            "rounding": {"mode": "ceil", "step": 100}, "min_platforms": 2,
            "eur_import_coefficient": 1.3,
            "derived_coefficients": {"carboma": 1.015, "unox": [
                {"max_dealer_price": 100000, "k": 1.059},
                {"max_dealer_price": 200000, "k": 1.037},
                {"max_dealer_price": 400000, "k": 1.042},
                {"max_dealer_price": None, "k": 0.944}]},
            "anomaly_cost_dealer_ratio": {"min": 0.625, "max": 1.6},
        }

    def test_market_price_uses_two_exact_platforms(self):
        result = self.app.calculate_price_item({"brand": "unox", "name": "Печь UNOX",
            "offers": [{"source": "a", "price": 50501}, {"source": "b", "price": 51423}]},
            self.policy(), {"rate_rub": 100})
        self.assertEqual(result["price"], 54300)
        self.assertEqual(result["price_source"], "monitor")

    def test_one_platform_falls_back_to_dealer(self):
        result = self.app.calculate_price_item({"brand": "carboma", "name": "Витрина Carboma",
            "offers": [{"source": "a", "price": 50000}], "dealer_price": 50000,
            "dealer_currency": "RUB"}, self.policy(), {"rate_rub": 100})
        self.assertEqual(result["price"], 54600)
        self.assertEqual(result["price_source"], "derived")

    def test_cost_floor_uses_eur_import_coefficient(self):
        result = self.app.calculate_price_item({"brand": "unox", "name": "Печь UNOX",
            "offers": [{"source": "a", "price": 100000}, {"source": "b", "price": 101000}],
            "dealer_price": 2000, "dealer_currency": "EUR", "cost": 1000,
            "cost_currency": "EUR"}, self.policy(), {"rate_rub": 100})
        self.assertEqual(result["price"], 136500)
        self.assertEqual(result["price_rule"], "cost_floor")

    def test_cost_anomaly_is_held(self):
        result = self.app.calculate_price_item({"brand": "unox", "name": "Печь UNOX",
            "dealer_price": 100000, "dealer_currency": "RUB", "cost": 200000,
            "cost_currency": "RUB"}, self.policy(), {"rate_rub": 100})
        self.assertEqual(result["reason"], "check_cost")
        self.assertFalse(result["publishable"])

    def test_excluded_bundle_has_no_price(self):
        result = self.app.calculate_price_item({"brand": "unox", "name": "Печь + зонт",
            "dealer_price": 100000}, self.policy(), {"rate_rub": 100})
        self.assertEqual(result["reason"], "excluded_item")

    def test_out_of_stock_item_has_no_price(self):
        result = self.app.calculate_price_item({"brand": "carboma", "name": "Витрина Carboma",
            "in_stock": False, "dealer_price": 100000}, self.policy(), {"rate_rub": 100})
        self.assertEqual(result["reason"], "no_stock")
        self.assertFalse(result["publishable"])

    def test_reference_index_uses_internal_code_and_model(self):
        self.app.write_json(self.app.REFERENCE / "aliases.json", {"items": [{
            "product_code": "12345", "model": "XEBC-06EU-E1RM", "dealer_price": 10,
        }]})
        index = self.app.reference_index("aliases.json")
        self.assertEqual(index["12345"]["dealer_price"], 10)
        self.assertEqual(index["XEBC06EUE1RM"]["dealer_price"], 10)

    def test_part_model_alias_does_not_shadow_device(self):
        self.app.write_json(self.app.REFERENCE / "parts.json", {"items": [{
            "product_code": "63843", "model": "XB893",
            "name": "Прокладка петли внутреннего стекла UNOX", "dealer_price": 10,
        }]})
        index = self.app.reference_index("parts.json")
        self.assertIn("63843", index)
        self.assertNotIn("XB893", index)


if __name__ == "__main__":
    unittest.main()
