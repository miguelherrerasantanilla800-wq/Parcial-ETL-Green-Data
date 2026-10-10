"""
Database Loader Module
Loads Gold dimensional models, fact tables, and data marts into SQLite Database (Green Data Warehouse)
and generates a multi-tab Executive Excel Report.
"""

import sqlite3
import logging
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd
import config

class DatabaseLoader:
    """
    Handles storage of the Gold layer into SQLite and Excel reports.
    """
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger("DatabaseLoader")
        self.db_path = config.DB_PATH
        self.excel_report_path = config.EXCEL_REPORT_PATH
        self.gold_dir = config.GOLD_DIR
        self.gold_dir.mkdir(parents=True, exist_ok=True)

    def load_to_sqlite(self, gold_models: Dict[str, pd.DataFrame]):
        """
        Loads all Gold models into the SQLite database with DDL indexes.
        """
        self.logger.info(f"Loading Gold models into SQLite Data Warehouse: {self.db_path.name}...")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # Enable Foreign Keys
            cursor.execute("PRAGMA foreign_keys = ON;")

            for table_name, df in gold_models.items():
                self.logger.info(f"Writing table '{table_name}' ({len(df):,} records)...")
                # Write dataframe to SQL table (replace if exists)
                df.to_sql(table_name, conn, if_exists="replace", index=False)

            # Create Indexes for Query Performance & Analytical Joins
            self.logger.info("Creating analytical indexes in SQLite warehouse...")
            indexes_sql = [
                "CREATE INDEX IF NOT EXISTS idx_fact_gei_anio ON fact_emisiones_gei_historico(anio);",
                "CREATE INDEX IF NOT EXISTS idx_fact_gei_sector ON fact_emisiones_gei_historico(sector_id);",
                "CREATE INDEX IF NOT EXISTS idx_fact_gei_gas ON fact_emisiones_gei_historico(gas_codigo);",
                "CREATE INDEX IF NOT EXISTS idx_fact_air_estacion ON fact_calidad_aire_tiempo_real(estacion_id);",
                "CREATE INDEX IF NOT EXISTS idx_fact_air_aqi ON fact_calidad_aire_tiempo_real(aqi);",
                "CREATE INDEX IF NOT EXISTS idx_dim_sector_macro ON dim_sector_gei(sector_macro_id);",
                "CREATE INDEX IF NOT EXISTS idx_mart_consolidado_anio ON mart_consolidado_ambiental(anio);"
            ]

            for idx_stmt in indexes_sql:
                cursor.execute(idx_stmt)

            conn.commit()
            self.logger.info("SQLite Data Warehouse load completed successfully.")
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error loading to SQLite warehouse: {e}")
            raise
        finally:
            conn.close()

    def generate_excel_report(self, gold_models: Dict[str, pd.DataFrame]):
        """
        Generates an executive Excel workbook containing structured reports, KPI cards, and data summaries.
        """
        self.logger.info(f"Generating Executive Excel Report: {self.excel_report_path.name}...")
        
        with pd.ExcelWriter(self.excel_report_path, engine="openpyxl") as writer:
            # Sheet 1: KPIs y Resumen Ejecutivo
            if "mart_kpis_resumen_ejecutivo" in gold_models:
                gold_models["mart_kpis_resumen_ejecutivo"].to_excel(
                    writer, sheet_name="KPIs Ejecutivos", index=False
                )

            # Sheet 2: Consolidado Ambiental por Sectores
            if "mart_consolidado_ambiental" in gold_models:
                gold_models["mart_consolidado_ambiental"].to_excel(
                    writer, sheet_name="Consolidado Sectores GEI", index=False
                )

            # Sheet 3: Calidad del Aire Tiempo Real
            if "fact_calidad_aire_tiempo_real" in gold_models and "dim_estacion_monitoreo" in gold_models:
                air_report = gold_models["fact_calidad_aire_tiempo_real"].merge(
                    gold_models["dim_estacion_monitoreo"], on="estacion_id", how="left"
                )
                air_report.to_excel(writer, sheet_name="Calidad Aire Tiempo Real", index=False)

            # Sheet 4: Muestra Fact Emisiones GEI Histórico
            if "fact_emisiones_gei_historico" in gold_models:
                # Top 10,000 facts for quick Excel navigation
                gold_models["fact_emisiones_gei_historico"].head(10000).to_excel(
                    writer, sheet_name="Emisiones GEI Fact Sample", index=False
                )

            # Sheet 5: Maestro de Gases y Contaminantes
            if "dim_tipo_gas_contaminante" in gold_models:
                gold_models["dim_tipo_gas_contaminante"].to_excel(
                    writer, sheet_name="Catálogo Gases y Contaminantes", index=False
                )

        self.logger.info(f"Executive Excel Report generated at: {self.excel_report_path}")
