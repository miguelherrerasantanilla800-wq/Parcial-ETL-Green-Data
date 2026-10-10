"""
Silver to Gold Transformation Module
Builds the Star Schema dimensional model, analytical fact tables, and integrated business marts
to empower environmental decision-making and business intelligence.
"""

import logging
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd
import numpy as np
import config

class SilverToGoldTransformer:
    """
    Transforms Silver conformed tables into Gold dimensional star schema & business data marts.
    """
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger("SilverToGoldTransformer")
        self.gold_dir = config.GOLD_DIR
        self.gold_dir.mkdir(parents=True, exist_ok=True)

    def build_dim_tiempo(self, min_year: int = 1990, max_year: int = 2024) -> pd.DataFrame:
        """Builds Time Dimension with environmental & climate policy milestones."""
        self.logger.info(f"Building dim_tiempo ({min_year} - {max_year})...")
        years = list(range(min_year, max_year + 1))
        records = []

        for y in years:
            decade = f"{(y // 10) * 10}s"
            
            if y < 1997:
                era = "Pre-Protocolo de Kioto"
            elif y <= 2015:
                era = "Protocolo de Kioto (1997-2015)"
            else:
                era = "Acuerdo de París (2016-Presente)"

            # Key environmental policy milestones
            if y == 1992:
                hito = "Cumbre de la Tierra de Río (CMNUCC)"
            elif y == 1997:
                hito = "Adopción Protocolo de Kioto"
            elif y == 2005:
                hito = "Entrada en vigor Protocolo de Kioto"
            elif y == 2015:
                hito = "Adopción Acuerdo de París (COP21)"
            elif y == 2020:
                hito = "Actualización NDC Colombia (Meta 51% reducción 2030)"
            elif y == 2021:
                hito = "Ley de Acción Climática (Ley 2169 de Colombia)"
            elif y == 2024:
                hito = "COP16 de Biodiversidad (Cali, Colombia)"
            else:
                hito = f"Seguimiento Ordinario Inventario GEI ({y})"

            records.append({
                "anio": y,
                "decada": decade,
                "era_climatica": era,
                "hito_ambiental": hito,
                "periodo_ndc_2030": "Fase de Acción Decenal" if y >= 2020 else "Línea Base Histórica"
            })

        df_dim_tiempo = pd.DataFrame(records)
        df_dim_tiempo.to_csv(self.gold_dir / "dim_tiempo.csv", index=False, encoding="utf-8-sig")
        return df_dim_tiempo

    def build_dim_tipo_gas_contaminante(self) -> pd.DataFrame:
        """Builds Gas and Pollutant Master Dimension (GHGs and Criteria Air Pollutants)."""
        self.logger.info("Building dim_tipo_gas_contaminante...")
        records = []
        
        # 1. GHGs from config
        for code, meta in config.GAS_METADATA.items():
            records.append({
                "gas_codigo": code,
                "nombre_completo": meta["nombre"],
                "categoria_ambiental": meta["tipo"],
                "potencial_gwp_ar5": meta["gwp"],
                "unidad_estandar": "kt CO2eq",
                "impacto_principal": "Calentamiento Global / Efecto Invernadero"
            })

        # 2. Criteria Air Pollutants from API
        air_pollutants = [
            {"gas_codigo": "PM2.5", "nombre": "Material Particulado fino (< 2.5 µm)", "tipo": "Contaminante Criterio", "gwp": 0.0, "unidad": "µg/m³", "impacto": "Salud Respiratoria y Cardiovascular"},
            {"gas_codigo": "PM10", "nombre": "Material Particulado grueso (< 10 µm)", "tipo": "Contaminante Criterio", "gwp": 0.0, "unidad": "µg/m³", "impacto": "Afección Vías Respiratorias Superiores"},
            {"gas_codigo": "O3", "nombre": "Ozono Troposférico", "tipo": "Contaminante Criterio", "gwp": 0.0, "unidad": "ppb", "impacto": "Smog Fotoquímico y Daño Pulmonar"},
            {"gas_codigo": "NO2", "nombre": "Dióxido de Nitrógeno", "tipo": "Contaminante Criterio", "gwp": 0.0, "unidad": "ppb", "impacto": "Inflamación Pulmonar y Lluvia Ácida"},
            {"gas_codigo": "SO2", "nombre": "Dióxido de Azufre", "tipo": "Contaminante Criterio", "gwp": 0.0, "unidad": "ppb", "impacto": "Acidificación y Trastornos Respiratorios"},
            {"gas_codigo": "CO", "nombre": "Monóxido de Carbono", "tipo": "Contaminante Criterio", "gwp": 0.0, "unidad": "ppm", "impacto": "Hipoxia y Toxicidad Sanguínea"}
        ]
        
        for ap in air_pollutants:
            records.append({
                "gas_codigo": ap["gas_codigo"],
                "nombre_completo": ap["nombre"],
                "categoria_ambiental": ap["tipo"],
                "potencial_gwp_ar5": ap["gwp"],
                "unidad_estandar": ap["unidad"],
                "impacto_principal": ap["impacto"]
            })

        df_dim_gas = pd.DataFrame(records).drop_duplicates(subset=["gas_codigo"]).reset_index(drop=True)
        df_dim_gas.to_csv(self.gold_dir / "dim_tipo_gas_contaminante.csv", index=False, encoding="utf-8-sig")
        return df_dim_gas

    def build_gold_models(
        self, 
        df_silver_gei_norm: pd.DataFrame, 
        df_silver_sectores: pd.DataFrame, 
        df_silver_stations: pd.DataFrame, 
        df_silver_measurements: pd.DataFrame
    ) -> Dict[str, pd.DataFrame]:
        """
        Creates all Gold layer dimensional tables, fact tables, and business marts.
        """
        self.logger.info("Assembling Gold Layer Star Schema and Business Marts...")
        
        # 1. Dimensions
        dim_tiempo = self.build_dim_tiempo(
            min_year=int(df_silver_gei_norm["anio"].min()), 
            max_year=int(df_silver_gei_norm["anio"].max())
        )
        dim_gas = self.build_dim_tipo_gas_contaminante()

        # Dimension Sector GEI
        dim_sector_gei = df_silver_sectores[[
            "sector_id", "codigo_ipcc", "sector_macro_id", "sector_macro_nombre",
            "categoria_descripcion", "clasificacion_limpia", "nivel_jerarquia"
        ]].copy().rename(columns={"clasificacion_limpia": "clasificacion_completa"})
        dim_sector_gei.to_csv(self.gold_dir / "dim_sector_gei.csv", index=False, encoding="utf-8-sig")

        # Dimension Estacion Monitoreo
        dim_estacion = df_silver_stations[[
            "estacion_id", "estacion_nombre", "ciudad", "pais", 
            "latitud", "longitud", "agencia_monitoreo", "url_estacion"
        ]].copy()
        dim_estacion.to_csv(self.gold_dir / "dim_estacion_monitoreo.csv", index=False, encoding="utf-8-sig")

        # 2. Fact Tables
        # Fact Emisiones GEI Historico
        fact_gei = df_silver_gei_norm[[
            "anio", "sector_id", "gas_codigo", "emision_kt", "emision_co2eq_kt",
            "emisiones_brutas_sector_kt", "absorciones_sector_kt", "emisiones_netas_sector_kt"
        ]].copy()
        fact_gei.insert(0, "fact_gei_id", range(1, len(fact_gei) + 1))
        fact_gei.to_csv(self.gold_dir / "fact_emisiones_gei_historico.csv", index=False, encoding="utf-8-sig")

        # Fact Calidad de Aire Tiempo Real
        fact_air = df_silver_measurements[[
            "estacion_id", "timestamp_iso", "timestamp_local", "timezone_offset",
            "aqi", "aqi_categoria", "nivel_alerta", "contaminante_dominante",
            "pm25", "pm10", "o3", "no2", "so2", "co", 
            "temperatura_c", "humedad_relativa_pct", "presion_hpa", "velocidad_viento_ms"
        ]].copy()
        fact_air.insert(0, "fact_air_id", range(1, len(fact_air) + 1))
        fact_air.to_csv(self.gold_dir / "fact_calidad_aire_tiempo_real.csv", index=False, encoding="utf-8-sig")

        # 3. Business Data Marts (Decision-Making Views)
        # Mart 1: Consolidado Ambiental por Macro Sector (Series Históricas y Tendencias)
        self.logger.info("Building mart_consolidado_ambiental...")
        df_merged_gei = fact_gei.merge(dim_sector_gei, on="sector_id")
        
        # Aggregate by Year and Macro Sector (Level 1)
        level_1_mask = df_merged_gei["nivel_jerarquia"] == 1
        df_level_1 = df_merged_gei[level_1_mask] if level_1_mask.any() else df_merged_gei

        mart_macro_sectores = df_level_1.groupby(
            ["anio", "sector_macro_id", "sector_macro_nombre"], as_index=False
        ).agg(
            total_co2eq_kt=("emision_co2eq_kt", "sum"),
            emisiones_brutas_kt=("emisiones_brutas_sector_kt", "mean"),
            absorciones_kt=("absorciones_sector_kt", "mean"),
            emisiones_netas_kt=("emisiones_netas_sector_kt", "mean")
        )

        # Calculate annual national totals to compute sector contribution %
        totales_anuales = mart_macro_sectores.groupby("anio")["emisiones_brutas_kt"].transform("sum")
        mart_macro_sectores["participacion_pct"] = np.where(
            totales_anuales > 0, 
            (mart_macro_sectores["emisiones_brutas_kt"] / totales_anuales) * 100.0, 
            0.0
        ).round(2)

        mart_macro_sectores.to_csv(self.gold_dir / "mart_consolidado_ambiental.csv", index=False, encoding="utf-8-sig")

        # Mart 2: KPIs y Resumen Ejecutivo para Toma de Decisiones
        self.logger.info("Building mart_kpis_resumen_ejecutivo...")
        
        # Latest year in inventory
        latest_year = int(fact_gei["anio"].max())
        base_year = int(fact_gei["anio"].min())

        # National Totals in latest year
        nac_latest = mart_macro_sectores[mart_macro_sectores["anio"] == latest_year]
        total_bruto_latest = nac_latest["emisiones_brutas_kt"].sum()
        total_absorciones_latest = nac_latest["absorciones_kt"].sum()
        total_neto_latest = nac_latest["emisiones_netas_kt"].sum()

        # National Totals in base year (1990)
        nac_base = mart_macro_sectores[mart_macro_sectores["anio"] == base_year]
        total_bruto_base = nac_base["emisiones_brutas_kt"].sum()
        total_neto_base = nac_base["emisiones_netas_kt"].sum()

        # Cumulative Growth (CAGR)
        num_years = latest_year - base_year
        cagr_bruto = ((total_bruto_latest / total_bruto_base) ** (1 / num_years) - 1.0) * 100 if total_bruto_base > 0 else 0.0

        # Top Emitting Sector
        top_sector_row = nac_latest.sort_values(by="emisiones_brutas_kt", ascending=False).iloc[0] if len(nac_latest) > 0 else None
        top_sector_name = top_sector_row["sector_macro_nombre"] if top_sector_row is not None else "N/A"
        top_sector_pct = top_sector_row["participacion_pct"] if top_sector_row is not None else 0.0

        # Real-time Air Quality KPIs
        avg_aqi = float(fact_air["aqi"].dropna().mean()) if len(fact_air) > 0 else 0.0
        max_aqi_row = fact_air.sort_values(by="aqi", ascending=False).iloc[0] if len(fact_air) > 0 else None
        station_max = dim_estacion[dim_estacion["estacion_id"] == max_aqi_row["estacion_id"]].iloc[0]["estacion_nombre"] if (max_aqi_row is not None and len(dim_estacion) > 0) else "N/A"
        
        kpi_records = [
            {"kpi_nombre": "Año Más Reciente del Inventario", "valor": f"{latest_year}", "unidad": "Año", "estado": "Actualizado", "interpretacion": "Último año del inventario nacional oficial procesado."},
            {"kpi_nombre": "Emisiones Brutas Nacionales", "valor": f"{total_bruto_latest:,.2f}", "unidad": "kt CO2eq", "estado": "Monitoreo", "interpretacion": "Volumen bruto total emitido a la atmósfera."},
            {"kpi_nombre": "Absorciones / Sumideros Forestales", "valor": f"{total_absorciones_latest:,.2f}", "unidad": "kt CO2eq", "estado": "Favorable", "interpretacion": "Capacidad de captura de carbono por bosques y coberturas vegetales."},
            {"kpi_nombre": "Emisiones Netas Nacionales", "valor": f"{total_neto_latest:,.2f}", "unidad": "kt CO2eq", "estado": "Crítico", "interpretacion": "Balance neto nacional (Brutas - Absorciones)."},
            {"kpi_nombre": "Tasa de Crecimiento Anual Compuesto (CAGR 1990-2024)", "valor": f"{cagr_bruto:+.2f}%", "unidad": "% Anual", "estado": "Alerta" if cagr_bruto > 0 else "Favorable", "interpretacion": "Ritmo de incremento promedio anual de emisiones."},
            {"kpi_nombre": "Sector Principal de Emisiones", "valor": f"{top_sector_name}", "unidad": f"{top_sector_pct:.1f}% del total", "estado": "Prioridad Estratégica", "interpretacion": "Sector económico con mayor huella de carbono nacional."},
            {"kpi_nombre": "Índice de Calidad del Aire Promedio (AQI Tiempo Real)", "valor": f"{avg_aqi:.1f}", "unidad": "Puntos AQI", "estado": "Aceptable" if avg_aqi <= 50 else "Moderado", "interpretacion": "Promedio de estaciones monitoreadas en tiempo real."},
            {"kpi_nombre": "Estación con Mayor Nivel de Contaminación", "valor": f"{station_max}", "unidad": f"AQI {max_aqi_row['aqi'] if max_aqi_row is not None else 0}", "estado": "Alerta Local", "interpretacion": "Punto focal urbano de mayor concentración de material particulado."}
        ]

        mart_kpis = pd.DataFrame(kpi_records)
        mart_kpis.to_csv(self.gold_dir / "mart_kpis_resumen_ejecutivo.csv", index=False, encoding="utf-8-sig")

        gold_models = {
            "dim_tiempo": dim_tiempo,
            "dim_sector_gei": dim_sector_gei,
            "dim_tipo_gas_contaminante": dim_gas,
            "dim_estacion_monitoreo": dim_estacion,
            "fact_emisiones_gei_historico": fact_gei,
            "fact_calidad_aire_tiempo_real": fact_air,
            "mart_consolidado_ambiental": mart_macro_sectores,
            "mart_kpis_resumen_ejecutivo": mart_kpis
        }

        self.logger.info("Gold layer models successfully generated and saved to CSV/Storage.")
        return gold_models
