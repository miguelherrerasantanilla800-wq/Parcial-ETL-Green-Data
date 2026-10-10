"""
Excel Extractor Module
Extracts historical Greenhouse Gas Inventory data from local Excel spreadsheet.
Saves raw data with audit metadata into Bronze layer.
"""

import hashlib
import datetime
import logging
from pathlib import Path
from typing import Tuple, Dict, Any, Optional
import pandas as pd
import config

class ExcelExtractor:
    """
    Handles extraction of Excel data sources into the Bronze layer.
    """
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger("ExcelExtractor")
        self.source_file = config.EXCEL_SOURCE_FILE
        self.bronze_dir = config.BRONZE_EXCEL_DIR
        self.bronze_dir.mkdir(parents=True, exist_ok=True)

    def _calculate_file_hash(self, file_path: Path) -> str:
        """Computes SHA-256 checksum for audit and data lineage."""
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(65536):
                sha256.update(chunk)
        return sha256.hexdigest()

    def extract(self) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Extracts Excel dataset, attaches audit metadata, and saves to Bronze storage.
        """
        self.logger.info(f"Starting extraction of Excel file: {self.source_file.name}")
        
        if not self.source_file.exists():
            raise FileNotFoundError(f"Source Excel file not found at: {self.source_file}")

        extraction_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        file_hash = self._calculate_file_hash(self.source_file)
        file_size_bytes = self.source_file.stat().st_size

        # Read the raw Excel sheet
        self.logger.info(f"Reading sheet 'Data' from Excel ({file_size_bytes / (1024*1024):.2f} MB)...")
        df_raw = pd.read_excel(self.source_file, sheet_name=0)

        # Audit metadata
        metadata = {
            "source_file": self.source_file.name,
            "source_path": str(self.source_file),
            "file_sha256": file_hash,
            "file_size_bytes": file_size_bytes,
            "extracted_at": extraction_timestamp,
            "raw_row_count": len(df_raw),
            "raw_column_count": len(df_raw.columns),
            "raw_columns": df_raw.columns.tolist()
        }

        # Add technical audit columns to bronze dataframe
        df_bronze = df_raw.copy()
        df_bronze["_ingested_at"] = extraction_timestamp
        df_bronze["_source_file"] = self.source_file.name
        df_bronze["_source_row_index"] = df_bronze.index

        # Persist to Bronze layer
        bronze_csv_path = self.bronze_dir / "raw_gei_inventory.csv"
        df_bronze.to_csv(bronze_csv_path, index=False, encoding="utf-8-sig")

        self.logger.info(
            f"Bronze Excel Extraction complete. {len(df_bronze):,} rows ingested into {bronze_csv_path}"
        )
        return df_bronze, metadata
