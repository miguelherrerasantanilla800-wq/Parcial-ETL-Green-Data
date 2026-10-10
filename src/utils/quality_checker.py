"""
Data Quality Checker Module
Performs automated audits and integrity validations across Bronze, Silver, and Gold layers.
"""

from typing import List, Dict, Any
import pandas as pd
import logging

class DataQualityChecker:
    """
    Automated data quality validation engine.
    """
    def __init__(self, logger: logging.Logger = None):
        self.logger = logger or logging.getLogger("DataQualityChecker")
        self.results: List[Dict[str, Any]] = []

    def check_non_empty(self, df: pd.DataFrame, dataset_name: str) -> bool:
        """Validates that a dataframe is not empty."""
        passed = df is not None and len(df) > 0
        self._record_check(
            layer_dataset=dataset_name,
            test_name="Non-Empty Dataset",
            passed=passed,
            details=f"Rows count: {len(df) if df is not None else 0}"
        )
        return passed

    def check_null_threshold(self, df: pd.DataFrame, dataset_name: str, max_null_ratio: float = 0.05, critical_cols: List[str] = None) -> bool:
        """Validates that null values do not exceed allowed thresholds in critical columns."""
        if df is None or len(df) == 0:
            self._record_check(dataset_name, "Null Threshold Check", False, "DataFrame is empty")
            return False

        cols_to_check = critical_cols if critical_cols else df.columns
        failed_cols = {}
        for col in cols_to_check:
            if col in df.columns:
                null_ratio = df[col].isnull().mean()
                if null_ratio > max_null_ratio:
                    failed_cols[col] = f"{null_ratio:.2%}"

        passed = len(failed_cols) == 0
        details = "All critical columns within threshold" if passed else f"Columns exceeding threshold: {failed_cols}"
        self._record_check(dataset_name, "Null Values Threshold", passed, details)
        return passed

    def check_unique_keys(self, df: pd.DataFrame, dataset_name: str, key_columns: List[str]) -> bool:
        """Validates that key columns have 0 duplicate entries (Primary Key constraint)."""
        if df is None or len(df) == 0:
            self._record_check(dataset_name, f"PK Uniqueness {key_columns}", False, "DataFrame is empty")
            return False

        missing_cols = [c for c in key_columns if c not in df.columns]
        if missing_cols:
            self._record_check(dataset_name, f"PK Uniqueness {key_columns}", False, f"Missing columns: {missing_cols}")
            return False

        dup_count = df.duplicated(subset=key_columns).sum()
        passed = dup_count == 0
        details = f"Duplicates found: {dup_count} on {key_columns}"
        self._record_check(dataset_name, f"PK Uniqueness {key_columns}", passed, details)
        return passed

    def check_range(self, df: pd.DataFrame, dataset_name: str, column: str, min_val: float = None, max_val: float = None) -> bool:
        """Validates that numeric column values fall within an expected valid domain."""
        if df is None or column not in df.columns or len(df) == 0:
            self._record_check(dataset_name, f"Range Check {column}", False, "Column missing or DF empty")
            return False

        series = df[column].dropna()
        violations = 0
        if min_val is not None:
            violations += (series < min_val).sum()
        if max_val is not None:
            violations += (series > max_val).sum()

        passed = violations == 0
        details = f"Valid range [{min_val}, {max_val}], violations: {violations}"
        self._record_check(dataset_name, f"Range Check [{column}]", passed, details)
        return passed

    def check_referential_integrity(self, fact_df: pd.DataFrame, dim_df: pd.DataFrame, fact_name: str, dim_name: str, key_col: str) -> bool:
        """Validates foreign key referential integrity between fact and dimension tables."""
        if fact_df is None or dim_df is None:
            self._record_check(f"{fact_name} -> {dim_name}", "Referential Integrity", False, "Missing table")
            return False

        fact_keys = set(fact_df[key_col].dropna().unique())
        dim_keys = set(dim_df[key_col].dropna().unique())
        orphans = fact_keys - dim_keys

        passed = len(orphans) == 0
        details = f"Foreign key '{key_col}' matches. Orphan keys count: {len(orphans)}"
        self._record_check(f"{fact_name} -> {dim_name}", f"FK Integrity [{key_col}]", passed, details)
        return passed

    def _record_check(self, layer_dataset: str, test_name: str, passed: bool, details: str):
        record = {
            "Dataset": layer_dataset,
            "Test": test_name,
            "Status": "PASSED" if passed else "FAILED",
            "Details": details
        }
        self.results.append(record)
        log_level = logging.INFO if passed else logging.WARNING
        self.logger.log(log_level, f"[DATA QUALITY] {layer_dataset} | {test_name}: {'PASSED' if passed else 'FAILED'} -> {details}")

    def get_summary_dataframe(self) -> pd.DataFrame:
        """Returns quality test results as a formatted DataFrame."""
        return pd.DataFrame(self.results)

    def print_summary(self):
        """Logs a formatted summary table of data quality results."""
        df_res = self.get_summary_dataframe()
        if len(df_res) == 0:
            self.logger.info("No data quality checks executed.")
            return

        total = len(df_res)
        passed = (df_res["Status"] == "PASSED").sum()
        failed = (df_res["Status"] == "FAILED").sum()
        
        self.logger.info("=" * 70)
        self.logger.info(f"DATA QUALITY AUDIT SUMMARY: {passed}/{total} PASSED ({failed} FAILED)")
        self.logger.info("=" * 70)
        for _, r in df_res.iterrows():
            mark = "[OK]" if r["Status"] == "PASSED" else "[FAIL]"
            self.logger.info(f" {mark:<6} [{r['Status']}] {r['Dataset']:<30} | {r['Test']:<25} | {r['Details']}")
        self.logger.info("=" * 70)
