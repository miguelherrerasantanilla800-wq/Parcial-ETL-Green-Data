"""
Bronze to Silver Transformation Module
Cleanses, conforms, repairs character encodings, extracts IPCC sector hierarchies,
normalizes gases into long-format facts, flattens API payloads, and validates data quality.
"""

import re
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd
import numpy as np
import config

class BronzeToSilverTransformer:
    """
    Transforms Bronze raw data into cleansed and conformed Silver data.
    """
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger("BronzeToSilverTransformer")
        self.silver_dir = config.SILVER_DIR
        self.silver_dir.mkdir(parents=True, exist_ok=True)

    def _repair_spanish_text(self, text: str) -> str:
        """Repairs common character encoding corruptions in Spanish strings."""
        if not isinstance(text, str):
            return ""
        
        rep_map = {
            "Energa": "Energía", "Energia": "Energía",
            "Gestin": "Gestión", "gestin": "gestión",
            "Plsticos": "Plásticos", "plsticos": "plásticos",
            "Biognicos": "Biogénicos", "biognicos": "biogénicos",
            "Fsiles": "Fósiles", "fsiles": "fósiles",
            "Slidos": "Sólidos", "slidos": "sólidos",
            "Maz": "Maíz", "maz": "maíz",
            "Azcar": "Azúcar", "azcar": "azúcar",
            "Lea": "Leña", "lea": "leña",
            "Bfalos": "Búfalos", "bfalos": "búfalos",
            "Limn": "Limón", "limn": "limón",
            "Maran": "Marañón", "maran": "marañón",
            "Orgnicos": "Orgánicos", "orgnicos": "orgánicos",
            "Inorgnicos": "Inorgánicos", "inorgnicos": "inorgánicos",
            "Atmosfrica": "Atmosférica", "atmosfrica": "atmosférica",
            "Lixiviacin": "Lixiviación", "lixiviacin": "lixiviación",
            "Agrcolas": "Agrícolas", "agrcolas": "agrícolas",
            "Mineralizacin": "Mineralización", "mineralizacin": "mineralización",
            "Inmovilizacin": "Inmovilización", "inmovilizacin": "inmovilización",
            "Prdida": "Pérdida", "prdida": "pérdida",
            "Materia orgnica": "Materia orgánica",
            "Domstico": "Doméstico", "domstico": "doméstico",
            "Domsticas": "Domésticas", "domsticas": "domésticas",
            "Eliminacin": "Eliminación", "eliminacin": "eliminación",
            "Incineracin": "Incineración", "incineracin": "incineración",
            "Produccin": "Producción", "produccin": "producción",
            "Conversin": "Conversión", "conversin": "conversión",
            "Degradacin": "Degradación", "degradacin": "degradación",
            "Caf": "Café", "caf": "café",
            "Lcteos": "Lácteos", "lcteos": "lácteos",
            "Cartn": "Cartón", "cartn": "cartón",
            "Anaerbico": "Anaeróbico", "anaerbico": "anaeróbico",
            "Qumicas": "Químicas", "qumicas": "químicas",
            "Mltiple": "Múltiple", "mltiple": "múltiple",
            "Aireacin": "Aireación", "aireacin": "aireación",
            "Sequa": "Sequía", "sequa": "sequía",
            "Aplicacin": "Aplicación", "aplicacin": "aplicación",
            "Refinacin": "Refinación", "refinacin": "refinación"
        }
        
        cleaned = text
        for corrupt, valid in rep_map.items():
            cleaned = cleaned.replace(corrupt, valid)
        
        # Replace any remaining question marks inside words
        cleaned = re.sub(r'(\w)(\w)', r'\1\2', cleaned)
        cleaned = cleaned.strip()
        return cleaned

    def _parse_ipcc_hierarchy(self, clasificacion: str) -> Dict[str, Any]:
        """
        Parses IPCC classification strings into structured hierarchical fields.
        Example: '3.B.1.a.iv.1. Vacas de Alta Producción' ->
          - codigo_ipcc: '3.B.1.a.iv.1'
          - sector_macro_id: '3'
          - sector_macro_nombre: 'Agricultura (AFOLU)'
          - categoria_descripcion: 'Vacas de Alta Producción'
          - nivel_jerarquia: 6
        """
        clasificacion = self._repair_spanish_text(clasificacion)
        
        if clasificacion.lower() == "nacional":
            return {
                "codigo_ipcc": "0",
                "sector_macro_id": "0",
                "sector_macro_nombre": "Total Nacional Consolidado",
                "categoria_descripcion": "Total Nacional Consolidado",
                "nivel_jerarquia": 0
            }

        # Regex to split IPCC Code prefix and text description
        match = re.match(r"^([0-9\(\)IVivA-Za-z\.\-]+)\s+(.*)$", clasificacion)
        if match:
            code = match.group(1).rstrip(".")
            desc = match.group(2).strip()
        else:
            code = clasificacion
            desc = clasificacion

        # Determine macro sector ID (1 to 5)
        first_char = code[0] if code else "0"
        if first_char in config.IPCC_MAIN_SECTORS:
            sector_macro_id = first_char
            sector_macro_nombre = config.IPCC_MAIN_SECTORS[first_char]
        elif code.startswith("4(") or code.startswith("4."):
            sector_macro_id = "4"
            sector_macro_nombre = config.IPCC_MAIN_SECTORS["4"]
        else:
            sector_macro_id = "OTRO"
            sector_macro_nombre = "Otros Sectores / Transversales"

        # Hierarchy level depth based on dot separators
        depth = len(code.split(".")) if "." in code else 1

        return {
            "codigo_ipcc": code,
            "sector_macro_id": sector_macro_id,
            "sector_macro_nombre": sector_macro_nombre,
            "categoria_descripcion": desc,
            "nivel_jerarquia": depth
        }

    def transform_excel_gei(self, df_bronze: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Transforms the Bronze GHG dataset into conformed Silver tables:
        1. Wide cleaned format
        2. Normalized long-format fact table for gases
        3. Sectors hierarchy dimension master
        """
        self.logger.info("Transforming Bronze GHG inventory data to Silver...")
        df = df_bronze.copy()

        # 1. Clean and standardize column names
        col_rename = {}
        for c in df.columns:
            cleaned_col = c.strip()
            if "A" in cleaned_col and "O" in cleaned_col and len(cleaned_col) <= 3:
                cleaned_col = "anio"
            elif cleaned_col.upper() == "CLASIFICACION":
                cleaned_col = "clasificacion_raw"
            elif cleaned_col == "Emisiones brutas":
                cleaned_col = "emisiones_brutas_kt"
            elif cleaned_col == "Absorciones":
                cleaned_col = "absorciones_kt"
            elif cleaned_col == "Emisiones netas":
                cleaned_col = "emisiones_netas_kt"
            col_rename[c] = cleaned_col

        df = df.rename(columns=col_rename)

        # Drop technical audit columns from computation set
        calc_cols = [c for c in df.columns if not c.startswith("_")]
        df = df[calc_cols]

        # 2. Repair text in classification
        df["clasificacion_limpia"] = df["clasificacion_raw"].apply(self._repair_spanish_text)
        
        # 3. Extract IPCC hierarchy
        hierarchy_data = df["clasificacion_limpia"].apply(self._parse_ipcc_hierarchy)
        hierarchy_df = pd.DataFrame(hierarchy_data.tolist(), index=df.index)
        
        df = pd.concat([df, hierarchy_df], axis=1)

        # Ensure correct data types
        df["anio"] = pd.to_numeric(df["anio"], errors="coerce").astype(int)
        
        # Numeric columns (gas emissions)
        num_cols = [c for c in df.columns if c not in [
            "anio", "clasificacion_raw", "clasificacion_limpia", "codigo_ipcc", 
            "sector_macro_id", "sector_macro_nombre", "categoria_descripcion", "nivel_jerarquia"
        ]]

        for col in num_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        # Mathematical verification: Net = Gross + Absorptions (since Absorptions are negative in inventory)
        df["balance_valido"] = np.isclose(
            df["emisiones_netas_kt"], 
            df["emisiones_brutas_kt"] + df["absorciones_kt"], 
            atol=1e-3
        )

        # Create unique Sector ID
        df["sector_id"] = df["codigo_ipcc"] + "_" + df["categoria_descripcion"].str.slice(0, 30).str.replace(r"[^\w]", "_", regex=True)

        # Generate Distinct Sectors Dimension
        dim_sectores = df[[
            "sector_id", "codigo_ipcc", "sector_macro_id", "sector_macro_nombre", 
            "categoria_descripcion", "clasificacion_limpia", "nivel_jerarquia"
        ]].drop_duplicates(subset=["sector_id"]).reset_index(drop=True)

        # 4. Unpivot gases to normalized long format
        self.logger.info("Unpivoting GHG gases into normalized dimensional facts...")
        gas_base_cols = [k for k in config.GAS_METADATA.keys() if k in df.columns]
        
        long_records = []
        for _, row in df.iterrows():
            anio = row["anio"]
            sector_id = row["sector_id"]
            em_bruta = row["emisiones_brutas_kt"]
            absorc = row["absorciones_kt"]
            em_neta = row["emisiones_netas_kt"]
            
            for gas in gas_base_cols:
                em_val = row[gas]
                eq_col = f"{gas}_eq"
                em_eq_val = row[eq_col] if eq_col in df.columns else em_val * config.GAS_METADATA[gas]["gwp"]
                
                # Only keep non-zero or significant emission facts
                if abs(em_val) > 1e-6 or abs(em_eq_val) > 1e-6:
                    long_records.append({
                        "anio": anio,
                        "sector_id": sector_id,
                        "gas_codigo": gas,
                        "emision_kt": round(float(em_val), 6),
                        "emision_co2eq_kt": round(float(em_eq_val), 6),
                        "emisiones_brutas_sector_kt": round(float(em_bruta), 6),
                        "absorciones_sector_kt": round(float(absorc), 6),
                        "emisiones_netas_sector_kt": round(float(em_neta), 6)
                    })

        df_gei_normalizado = pd.DataFrame(long_records)

        # Save to Silver Directory
        silver_wide_path = self.silver_dir / "silver_inventario_gei_ancho.csv"
        silver_norm_path = self.silver_dir / "silver_inventario_gei_normalizado.csv"
        silver_sec_path = self.silver_dir / "silver_jerarquia_sectores.csv"

        df.to_csv(silver_wide_path, index=False, encoding="utf-8-sig")
        df_gei_normalizado.to_csv(silver_norm_path, index=False, encoding="utf-8-sig")
        dim_sectores.to_csv(silver_sec_path, index=False, encoding="utf-8-sig")

        self.logger.info(
            f"Silver GHG transformation complete: {len(df):,} wide rows, {len(df_gei_normalizado):,} long facts, {len(dim_sectores)} sectors."
        )
        return df, df_gei_normalizado, dim_sectores

    def transform_api_air_quality(self, raw_api_data: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Transforms raw Bronze WAQI API payloads into conformed Silver Air Quality tables:
        1. Stations master dimension
        2. Real-time air quality measurements fact table
        """
        self.logger.info("Transforming Bronze WAQI API data to Silver Air Quality models...")
        
        stations_list = []
        measurements_list = []

        def process_station_payload(station_dict: Dict[str, Any], default_city: str = ""):
            if not station_dict or not isinstance(station_dict, dict):
                return
            
            resp = station_dict.get("response", {})
            if resp.get("status") != "ok":
                return
            
            data = resp.get("data", {})
            if not isinstance(data, dict):
                return

            idx = data.get("idx")
            if not idx:
                return

            # Station metadata
            city_info = data.get("city", {})
            station_name = city_info.get("name", f"Station {idx}")
            station_name = self._repair_spanish_text(station_name)
            geo = city_info.get("geo", [0.0, 0.0])
            lat = float(geo[0]) if len(geo) > 0 else 0.0
            lon = float(geo[1]) if len(geo) > 1 else 0.0
            
            # Country & City parsing
            parts = [p.strip() for p in station_name.split(",")]
            country = parts[-1] if len(parts) > 1 else "Unknown"
            city = parts[1] if len(parts) > 2 else (parts[0] if len(parts) > 1 else default_city)

            attributions = data.get("attributions", [])
            attribution_agency = attributions[0].get("name", "WAQI") if attributions else "WAQI"

            station_record = {
                "estacion_id": int(idx),
                "estacion_nombre": station_name,
                "ciudad": city,
                "pais": country,
                "latitud": lat,
                "longitud": lon,
                "agencia_monitoreo": attribution_agency,
                "url_estacion": city_info.get("url", "")
            }
            stations_list.append(station_record)

            # Air Quality Measurements & Pollutants
            aqi_val = data.get("aqi")
            try:
                aqi_float = float(aqi_val) if aqi_val is not None else np.nan
            except ValueError:
                aqi_float = np.nan

            # Determine AQI category
            categoria_aqi = "Desconocida"
            nivel_alerta = 0
            for cat in config.AQI_CATEGORIES:
                if cat["min"] <= aqi_float <= cat["max"]:
                    categoria_aqi = cat["categoria"]
                    nivel_alerta = cat["nivel_alerta"]
                    break

            time_info = data.get("time", {})
            iaqi = data.get("iaqi", {})

            # Extract individual pollutant metrics
            pm25 = iaqi.get("pm25", {}).get("v")
            pm10 = iaqi.get("pm10", {}).get("v")
            o3 = iaqi.get("o3", {}).get("v")
            no2 = iaqi.get("no2", {}).get("v")
            so2 = iaqi.get("so2", {}).get("v")
            co = iaqi.get("co", {}).get("v")
            temp = iaqi.get("t", {}).get("v")
            humidity = iaqi.get("h", {}).get("v")
            pressure = iaqi.get("p", {}).get("v")
            wind = iaqi.get("w", {}).get("v")

            measurement_record = {
                "estacion_id": int(idx),
                "timestamp_iso": time_info.get("iso", ""),
                "timestamp_local": time_info.get("s", ""),
                "timezone_offset": time_info.get("tz", ""),
                "aqi": aqi_float,
                "aqi_categoria": categoria_aqi,
                "nivel_alerta": nivel_alerta,
                "contaminante_dominante": data.get("dominentpol", "pm25"),
                "pm25": float(pm25) if pm25 is not None else np.nan,
                "pm10": float(pm10) if pm10 is not None else np.nan,
                "o3": float(o3) if o3 is not None else np.nan,
                "no2": float(no2) if no2 is not None else np.nan,
                "so2": float(so2) if so2 is not None else np.nan,
                "co": float(co) if co is not None else np.nan,
                "temperatura_c": float(temp) if temp is not None else np.nan,
                "humedad_relativa_pct": float(humidity) if humidity is not None else np.nan,
                "presion_hpa": float(pressure) if pressure is not None else np.nan,
                "velocidad_viento_ms": float(wind) if wind is not None else np.nan
            }
            measurements_list.append(measurement_record)

        # 1. Process target / resolved station
        if raw_api_data.get("resolved_station_raw"):
            process_station_payload(raw_api_data["resolved_station_raw"], default_city="Target Station")

        # 2. Process Colombia network stations
        for st in raw_api_data.get("colombia_stations_raw", []):
            process_station_payload(st, default_city=st.get("city_keyword", "Colombia"))

        # Deduplicate stations
        df_stations = pd.DataFrame(stations_list).drop_duplicates(subset=["estacion_id"]).reset_index(drop=True)
        df_measurements = pd.DataFrame(measurements_list).drop_duplicates(subset=["estacion_id", "timestamp_iso"]).reset_index(drop=True)

        # Save to Silver Storage
        silver_stat_path = self.silver_dir / "silver_estaciones_calidad_aire.csv"
        silver_meas_path = self.silver_dir / "silver_mediciones_calidad_aire.csv"

        df_stations.to_csv(silver_stat_path, index=False, encoding="utf-8-sig")
        df_measurements.to_csv(silver_meas_path, index=False, encoding="utf-8-sig")

        self.logger.info(
            f"Silver Air Quality transformation complete: {len(df_stations)} stations and {len(df_measurements)} measurement records."
        )
        return df_stations, df_measurements
