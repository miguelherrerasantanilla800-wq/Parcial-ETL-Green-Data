"""
Unit and Integration Tests for Green Data ETL Pipeline
"""

import unittest
import sqlite3
import pandas as pd
from pathlib import Path
import sys

# Ensure project root in sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

import config
from src.extract.api_extractor import APIExtractor
from src.extract.excel_extractor import ExcelExtractor
from src.transform.bronze_to_silver import BronzeToSilverTransformer
from src.transform.silver_to_gold import SilverToGoldTransformer
from src.load.database_loader import DatabaseLoader

class TestGreenDataETL(unittest.TestCase):
    """
    Test suite for Bronze, Silver, and Gold ETL pipeline stages.
    """

    @classmethod
    def setUpClass(cls):
        cls.b2s = BronzeToSilverTransformer()
        cls.s2g = SilverToGoldTransformer()

    def test_01_spanish_text_repair(self):
        """Tests that corrupted Spanish encodings are properly repaired."""
        corrupt_samples = [
            ("1. Energa", "1. Energía"),
            ("5.D. Tratamiento y eliminacin aguas residuales industriales", "5.D. Tratamiento y eliminación aguas residuales industriales"),
            ("4.A.1.a. Tierras forestales (Consumo de lea)", "4.A.1.a. Tierras forestales (Consumo de leña)"),
            ("3.F.1.c. Maz", "3.F.1.c. Maíz"),
            ("3.B.4.a. Bfalos", "3.B.4.a. Búfalos")
        ]
        for bad, expected in corrupt_samples:
            repaired = self.b2s._repair_spanish_text(bad)
            self.assertEqual(repaired, expected, f"Failed on '{bad}' -> '{repaired}' != '{expected}'")

    def test_02_ipcc_hierarchy_parsing(self):
        """Tests hierarchical parsing of IPCC categories."""
        sample_clasif = "3.B.1.a.iv.1. Vacas de Alta Produccin"
        result = self.b2s._parse_ipcc_hierarchy(sample_clasif)
        
        self.assertEqual(result["codigo_ipcc"], "3.B.1.a.iv.1")
        self.assertEqual(result["sector_macro_id"], "3")
        self.assertEqual(result["sector_macro_nombre"], "Agricultura (AFOLU)")
        self.assertEqual(result["categoria_descripcion"], "Vacas de Alta Producción")
        self.assertGreaterEqual(result["nivel_jerarquia"], 5)

    def test_03_excel_extractor_integrity(self):
        """Tests that Excel extractor reads the file and produces checksums."""
        extractor = ExcelExtractor()
        df, meta = extractor.extract()
        
        self.assertIsNotNone(df)
        self.assertGreater(len(df), 10000)
        self.assertIn("file_sha256", meta)
        self.assertEqual(len(meta["file_sha256"]), 64)

    def test_04_api_extractor_and_fallback(self):
        """Tests that API extractor queries target endpoint and handles fallback."""
        extractor = APIExtractor()
        data = extractor.extract()
        
        self.assertIn("metadata", data)
        self.assertIsNotNone(data["resolved_station_raw"])
        resp = data["resolved_station_raw"].get("response", {})
        self.assertEqual(resp.get("status"), "ok")

    def test_05_silver_transformation_math_balance(self):
        """Tests that Silver transformation validates Net = Gross + Absorptions balance."""
        extractor = ExcelExtractor()
        df_bronze, _ = extractor.extract()
        df_wide, df_norm, df_sec = self.b2s.transform_excel_gei(df_bronze)
        
        self.assertTrue(df_wide["balance_valido"].all(), "Not all records satisfied Net = Gross + Absorptions balance!")
        self.assertGreater(len(df_norm), 10000)
        self.assertGreater(len(df_sec), 50)

    def test_06_gold_star_schema_and_warehouse(self):
        """Tests Gold layer models generation and SQLite database storage."""
        # 1. Extract
        api_extractor = APIExtractor()
        raw_api = api_extractor.extract()
        excel_extractor = ExcelExtractor()
        df_bronze, _ = excel_extractor.extract()

        # 2. Silver
        df_wide, df_norm, df_sec = self.b2s.transform_excel_gei(df_bronze)
        df_stat, df_meas = self.b2s.transform_api_air_quality(raw_api)

        # 3. Gold
        gold_models = self.s2g.build_gold_models(
            df_silver_gei_norm=df_norm,
            df_silver_sectores=df_sec,
            df_silver_stations=df_stat,
            df_silver_measurements=df_meas
        )

        self.assertIn("fact_emisiones_gei_historico", gold_models)
        self.assertIn("fact_calidad_aire_tiempo_real", gold_models)
        self.assertIn("mart_consolidado_ambiental", gold_models)
        self.assertIn("mart_kpis_resumen_ejecutivo", gold_models)

        # 4. Load
        loader = DatabaseLoader()
        loader.load_to_sqlite(gold_models)
        loader.generate_excel_report(gold_models)

        # 5. Validate SQLite
        self.assertTrue(config.DB_PATH.exists())
        conn = sqlite3.connect(config.DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM fact_emisiones_gei_historico;")
        count_gei = cur.fetchone()[0]
        self.assertGreater(count_gei, 10000)

        cur.execute("SELECT COUNT(*) FROM dim_sector_gei;")
        count_sec = cur.fetchone()[0]
        self.assertGreater(count_sec, 50)
        conn.close()

if __name__ == "__main__":
    unittest.main()
