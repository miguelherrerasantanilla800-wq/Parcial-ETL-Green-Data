"""
Backend API Server - Green Data ETL
FastAPI server that connects the React frontend with:
  - Gold layer data (SQLite warehouse)
  - ETL pipeline execution (step-by-step)
  - Ollama LLM for AI-powered data analysis chat
"""

import sys
import json
import sqlite3
import requests
import time
from pathlib import Path
from typing import Dict, Any, List
from contextlib import contextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
import uvicorn

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

import config
from src.utils.logger import setup_logger
from src.utils.quality_checker import DataQualityChecker
from src.extract.api_extractor import APIExtractor
from src.extract.excel_extractor import ExcelExtractor
from src.transform.bronze_to_silver import BronzeToSilverTransformer
from src.transform.silver_to_gold import SilverToGoldTransformer
from src.load.database_loader import DatabaseLoader

# ──────────────────────────────────────────────────
# FastAPI App
# ──────────────────────────────────────────────────
app = FastAPI(
    title="Green Data ETL API",
    description="API backend for Medallion ETL pipeline with Ollama AI chat",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logger = setup_logger("APIServer", log_file=config.DATA_DIR / "api_server.log")

# ──────────────────────────────────────────────────
# Ollama Configuration (loaded from .env via config)
# ──────────────────────────────────────────────────
OLLAMA_BASE_URL = config.OLLAMA_BASE_URL
OLLAMA_MODEL = config.OLLAMA_MODEL

# ──────────────────────────────────────────────────
# Pydantic Models
# ──────────────────────────────────────────────────
class ChatMessage(BaseModel):
    message: str

class SQLQuery(BaseModel):
    query: str

class PipelineRequest(BaseModel):
    step: str  # "bronze", "silver", "gold"

# ──────────────────────────────────────────────────
# Database Helpers
# ──────────────────────────────────────────────────
@contextmanager
def get_db_connection():
    """Context manager for SQLite connections."""
    conn = sqlite3.connect(str(config.DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def db_exists() -> bool:
    return config.DB_PATH.exists()

def get_data_summary() -> Dict[str, Any]:
    """Generates a comprehensive data summary for the AI context."""
    if not db_exists():
        return {"status": "no_data", "message": "Pipeline no ejecutado aún"}
    
    summary = {}
    try:
        with get_db_connection() as conn:
            # KPIs
            kpis = pd.read_sql_query("SELECT * FROM mart_kpis_resumen_ejecutivo", conn)
            summary["kpis"] = kpis.to_dict(orient="records")
            
            # Latest year sector breakdown
            sectors = pd.read_sql_query("""
                SELECT anio, sector_macro_nombre, 
                       ROUND(emisiones_brutas_kt, 2) as emisiones_brutas_kt,
                       ROUND(absorciones_kt, 2) as absorciones_kt,
                       ROUND(emisiones_netas_kt, 2) as emisiones_netas_kt,
                       participacion_pct
                FROM mart_consolidado_ambiental 
                WHERE anio = (SELECT MAX(anio) FROM mart_consolidado_ambiental)
                ORDER BY emisiones_brutas_kt DESC
            """, conn)
            summary["sectors_latest"] = sectors.to_dict(orient="records")
            
            # Historical trend (aggregated by year)
            trend = pd.read_sql_query("""
                SELECT anio, 
                       ROUND(SUM(emisiones_brutas_kt), 2) as total_brutas,
                       ROUND(SUM(absorciones_kt), 2) as total_absorciones,
                       ROUND(SUM(emisiones_netas_kt), 2) as total_netas
                FROM mart_consolidado_ambiental
                GROUP BY anio ORDER BY anio
            """, conn)
            summary["historical_trend"] = trend.to_dict(orient="records")
            
            # Air quality stations
            air = pd.read_sql_query("""
                SELECT e.estacion_nombre, e.ciudad, e.pais,
                       f.aqi, f.aqi_categoria, f.contaminante_dominante,
                       f.pm25, f.pm10, f.temperatura_c
                FROM fact_calidad_aire_tiempo_real f
                JOIN dim_estacion_monitoreo e ON f.estacion_id = e.estacion_id
                ORDER BY f.aqi DESC
            """, conn)
            summary["air_quality"] = air.to_dict(orient="records")
            
            # Time dimension (climate eras)
            time_dim = pd.read_sql_query("SELECT * FROM dim_tiempo ORDER BY anio", conn)
            summary["time_dimension"] = time_dim.to_dict(orient="records")
            
            # Gas catalog
            gases = pd.read_sql_query("SELECT gas_codigo, nombre_completo, categoria_ambiental, potencial_gwp_ar5 FROM dim_tipo_gas_contaminante", conn)
            summary["gases_catalog"] = gases.to_dict(orient="records")
            
            # Table row counts
            tables = ["fact_emisiones_gei_historico", "fact_calidad_aire_tiempo_real", 
                      "dim_sector_gei", "dim_tiempo", "dim_tipo_gas_contaminante", "dim_estacion_monitoreo",
                      "mart_consolidado_ambiental", "mart_kpis_resumen_ejecutivo"]
            counts = {}
            for t in tables:
                try:
                    c = pd.read_sql_query(f"SELECT COUNT(*) as cnt FROM {t}", conn)
                    counts[t] = int(c.iloc[0]["cnt"])
                except:
                    counts[t] = 0
            summary["table_row_counts"] = counts
            
    except Exception as e:
        summary["error"] = str(e)
    
    return summary

# ──────────────────────────────────────────────────
# ETL Pipeline Endpoints
# ──────────────────────────────────────────────────
@app.get("/api/health")
def health_check():
    return {
        "status": "ok",
        "db_exists": db_exists(),
        "ollama_model": OLLAMA_MODEL,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

@app.get("/api/pipeline/status")
def pipeline_status():
    """Returns the current state of each data layer."""
    bronze_api = (config.BRONZE_API_DIR / "raw_waqi_api_extract.json").exists()
    bronze_excel = (config.BRONZE_EXCEL_DIR / "raw_gei_inventory.csv").exists()
    silver_gei = (config.SILVER_DIR / "silver_inventario_gei_normalizado.csv").exists()
    silver_air = (config.SILVER_DIR / "silver_estaciones_calidad_aire.csv").exists()
    gold_db = db_exists()
    gold_report = config.EXCEL_REPORT_PATH.exists()
    
    return {
        "bronze": {
            "completed": bronze_api and bronze_excel,
            "api_extracted": bronze_api,
            "excel_extracted": bronze_excel
        },
        "silver": {
            "completed": silver_gei and silver_air,
            "gei_normalized": silver_gei,
            "air_quality_conformed": silver_air
        },
        "gold": {
            "completed": gold_db,
            "warehouse_db": gold_db,
            "executive_report": gold_report
        }
    }

@app.post("/api/pipeline/execute")
def execute_pipeline_step(req: PipelineRequest):
    """Executes a specific ETL pipeline step."""
    step = req.step.lower()
    
    try:
        t0 = time.time()
        pipeline_logger = setup_logger("PipelineAPI", log_file=config.DATA_DIR / "pipeline_execution.log")
        
        if step == "bronze":
            pipeline_logger.info(">>> [BRONZE] Starting raw data ingestion...")
            
            # Extract API
            api_ext = APIExtractor(logger=pipeline_logger)
            raw_api = api_ext.extract()
            api_stations = len(raw_api.get("colombia_stations_raw", [])) + 1
            
            # Extract Excel
            excel_ext = ExcelExtractor(logger=pipeline_logger)
            df_bronze, meta = excel_ext.extract()
            
            elapsed = time.time() - t0
            return {
                "status": "success",
                "step": "bronze",
                "elapsed_seconds": round(elapsed, 2),
                "details": {
                    "api_stations_extracted": api_stations,
                    "excel_rows_ingested": len(df_bronze),
                    "excel_file_hash": meta["file_sha256"][:16] + "...",
                    "message": f"Bronze layer ingestion completed. {len(df_bronze):,} rows from Excel, {api_stations} air quality stations from API."
                }
            }
            
        elif step == "silver":
            # Check bronze exists
            if not (config.BRONZE_API_DIR / "raw_waqi_api_extract.json").exists():
                raise HTTPException(status_code=400, detail="Bronze layer not found. Run Bronze extraction first.")
            
            pipeline_logger.info(">>> [SILVER] Starting data cleaning and normalization...")
            
            with open(config.BRONZE_API_DIR / "raw_waqi_api_extract.json", "r", encoding="utf-8") as f:
                raw_api = json.load(f)
            df_bronze = pd.read_csv(config.BRONZE_EXCEL_DIR / "raw_gei_inventory.csv")
            
            b2s = BronzeToSilverTransformer(logger=pipeline_logger)
            _, df_norm, df_sec = b2s.transform_excel_gei(df_bronze)
            df_stat, df_meas = b2s.transform_api_air_quality(raw_api)
            
            elapsed = time.time() - t0
            return {
                "status": "success",
                "step": "silver",
                "elapsed_seconds": round(elapsed, 2),
                "details": {
                    "gei_normalized_facts": len(df_norm),
                    "sectors_cataloged": len(df_sec),
                    "air_stations_conformed": len(df_stat),
                    "air_measurements": len(df_meas),
                    "message": f"Silver layer complete. {len(df_norm):,} normalized GHG facts, {len(df_sec)} IPCC sectors, {len(df_stat)} air quality stations."
                }
            }
            
        elif step == "gold":
            # Check silver exists
            if not (config.SILVER_DIR / "silver_inventario_gei_normalizado.csv").exists():
                raise HTTPException(status_code=400, detail="Silver layer not found. Run Silver transformation first.")
            
            pipeline_logger.info(">>> [GOLD] Building star schema and data warehouse...")
            
            df_norm = pd.read_csv(config.SILVER_DIR / "silver_inventario_gei_normalizado.csv")
            df_sec = pd.read_csv(config.SILVER_DIR / "silver_jerarquia_sectores.csv")
            df_stat = pd.read_csv(config.SILVER_DIR / "silver_estaciones_calidad_aire.csv")
            df_meas = pd.read_csv(config.SILVER_DIR / "silver_mediciones_calidad_aire.csv")
            
            s2g = SilverToGoldTransformer(logger=pipeline_logger)
            gold_models = s2g.build_gold_models(df_norm, df_sec, df_stat, df_meas)
            
            # Quality checks
            checker = DataQualityChecker(logger=pipeline_logger)
            checker.check_referential_integrity(gold_models["fact_emisiones_gei_historico"], gold_models["dim_tiempo"], "fact_gei", "dim_tiempo", "anio")
            checker.check_referential_integrity(gold_models["fact_emisiones_gei_historico"], gold_models["dim_sector_gei"], "fact_gei", "dim_sector", "sector_id")
            checker.check_referential_integrity(gold_models["fact_emisiones_gei_historico"], gold_models["dim_tipo_gas_contaminante"], "fact_gei", "dim_gas", "gas_codigo")
            checker.check_referential_integrity(gold_models["fact_calidad_aire_tiempo_real"], gold_models["dim_estacion_monitoreo"], "fact_air", "dim_estacion", "estacion_id")
            
            # Load to SQLite
            db_loader = DatabaseLoader(logger=pipeline_logger)
            db_loader.load_to_sqlite(gold_models)
            db_loader.generate_excel_report(gold_models)
            
            quality_df = checker.get_summary_dataframe()
            passed = int((quality_df["Status"] == "PASSED").sum())
            total = len(quality_df)
            
            elapsed = time.time() - t0
            return {
                "status": "success",
                "step": "gold",
                "elapsed_seconds": round(elapsed, 2),
                "details": {
                    "tables_created": list(gold_models.keys()),
                    "quality_tests_passed": f"{passed}/{total}",
                    "db_path": str(config.DB_PATH),
                    "report_path": str(config.EXCEL_REPORT_PATH),
                    "message": f"Gold layer complete. Star schema with {len(gold_models)} tables loaded into SQLite. {passed}/{total} quality tests passed."
                }
            }
        else:
            raise HTTPException(status_code=400, detail=f"Invalid step: {step}. Use 'bronze', 'silver', or 'gold'.")
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Pipeline step '{step}' failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def df_to_clean_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Converts a DataFrame to records, replacing NaN/null values with Python None for standard JSON."""
    records = df.to_dict(orient="records")
    for r in records:
        for k, v in list(r.items()):
            if pd.isna(v):
                r[k] = None
    return records

# ──────────────────────────────────────────────────
# Data Endpoints (Gold Layer)
# ──────────────────────────────────────────────────
@app.get("/api/data/kpis")
def get_kpis():
    if not db_exists():
        raise HTTPException(status_code=404, detail="Gold database not found. Run the pipeline first.")
    with get_db_connection() as conn:
        df = pd.read_sql_query("SELECT * FROM mart_kpis_resumen_ejecutivo", conn)
    return df_to_clean_records(df)

@app.get("/api/data/sectors")
def get_sectors():
    if not db_exists():
        raise HTTPException(status_code=404, detail="Gold database not found.")
    with get_db_connection() as conn:
        df = pd.read_sql_query("""
            SELECT anio, sector_macro_nombre, 
                   ROUND(emisiones_brutas_kt, 2) as emisiones_brutas_kt,
                   ROUND(absorciones_kt, 2) as absorciones_kt,
                   ROUND(emisiones_netas_kt, 2) as emisiones_netas_kt,
                   participacion_pct
            FROM mart_consolidado_ambiental 
            ORDER BY anio DESC, emisiones_brutas_kt DESC
        """, conn)
    return df_to_clean_records(df)

@app.get("/api/data/sectors/{year}")
def get_sectors_by_year(year: int):
    if not db_exists():
        raise HTTPException(status_code=404, detail="Gold database not found.")
    with get_db_connection() as conn:
        df = pd.read_sql_query(f"""
            SELECT sector_macro_nombre, 
                   ROUND(emisiones_brutas_kt, 2) as emisiones_brutas_kt,
                   ROUND(absorciones_kt, 2) as absorciones_kt,
                   ROUND(emisiones_netas_kt, 2) as emisiones_netas_kt,
                   participacion_pct
            FROM mart_consolidado_ambiental 
            WHERE anio = {year}
            ORDER BY emisiones_brutas_kt DESC
        """, conn)
    return df_to_clean_records(df)

@app.get("/api/data/trend")
def get_historical_trend():
    if not db_exists():
        raise HTTPException(status_code=404, detail="Gold database not found.")
    with get_db_connection() as conn:
        df = pd.read_sql_query("""
            SELECT anio, 
                   ROUND(SUM(emisiones_brutas_kt), 2) as total_brutas,
                   ROUND(SUM(absorciones_kt), 2) as total_absorciones,
                   ROUND(SUM(emisiones_netas_kt), 2) as total_netas
            FROM mart_consolidado_ambiental
            GROUP BY anio ORDER BY anio
        """, conn)
    return df_to_clean_records(df)

@app.get("/api/data/air-quality")
def get_air_quality():
    if not db_exists():
        raise HTTPException(status_code=404, detail="Gold database not found.")
    try:
        with get_db_connection() as conn:
            df = pd.read_sql_query("""
                SELECT e.estacion_nombre, e.ciudad, e.pais,
                       e.latitud, e.longitud,
                       f.aqi, f.aqi_categoria, f.nivel_alerta,
                       f.contaminante_dominante,
                       f.pm25, f.pm10, f.o3, f.no2, f.so2, f.co,
                       f.temperatura_c, f.humedad_relativa_pct
                FROM fact_calidad_aire_tiempo_real f
                JOIN dim_estacion_monitoreo e ON f.estacion_id = e.estacion_id
                ORDER BY f.aqi DESC
            """, conn)
        return df_to_clean_records(df)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Air quality error: {str(e)}")

@app.get("/api/data/gases")
def get_gases_catalog():
    if not db_exists():
        raise HTTPException(status_code=404, detail="Gold database not found.")
    with get_db_connection() as conn:
        df = pd.read_sql_query("SELECT * FROM dim_tipo_gas_contaminante", conn)
    return df_to_clean_records(df)

@app.get("/api/data/time-dimension")
def get_time_dimension():
    if not db_exists():
        raise HTTPException(status_code=404, detail="Gold database not found.")
    with get_db_connection() as conn:
        df = pd.read_sql_query("SELECT * FROM dim_tiempo ORDER BY anio", conn)
    return df_to_clean_records(df)

@app.post("/api/data/sql")
def execute_custom_sql(req: SQLQuery):
    if not db_exists():
        raise HTTPException(status_code=404, detail="Gold database not found.")
    
    query = req.query.strip()
    # Security: Only allow SELECT queries
    if not query.upper().startswith("SELECT"):
        raise HTTPException(status_code=403, detail="Only SELECT queries are allowed.")
    
    try:
        with get_db_connection() as conn:
            t0 = time.time()
            df = pd.read_sql_query(query, conn)
            elapsed = time.time() - t0
        return {
            "columns": list(df.columns),
            "data": df_to_clean_records(df),
            "row_count": len(df),
            "elapsed_ms": round(elapsed * 1000, 1)
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"SQL Error: {str(e)}")

# ──────────────────────────────────────────────────
# Ollama AI Chat Endpoint
# ──────────────────────────────────────────────────
@app.post("/api/chat")
def chat_with_ai(msg: ChatMessage):
    """
    Sends user message to the configured Ollama model with environmental data context.
    The AI can answer questions about GHG emissions, air quality, sectors, etc.
    """
    user_message = msg.message.strip()
    if not user_message:
        raise HTTPException(status_code=400, detail="Empty message")
    
    # Build compact, high-efficiency context from Gold data
    data_summary = get_data_summary()
    
    # Create concise text summary to optimize prompt prefill time and memory
    kpi_lines = [
        f"- {k.get('kpi_nombre')}: {k.get('valor_actual')} {k.get('unidad')} ({k.get('descripcion')})"
        for k in data_summary.get('kpis', [])
    ]
    sec_lines = [
        f"- {s.get('sector_macro_nombre')}: Brutas {s.get('emisiones_brutas_kt')} kt, Absorciones {s.get('absorciones_kt')} kt, Netas {s.get('emisiones_netas_kt')} kt ({s.get('participacion_pct')}%)"
        for s in data_summary.get('sectors_latest', [])[:6]
    ]
    air_lines = [
        f"- {a.get('estacion_nombre')} ({a.get('ciudad')}): AQI {a.get('aqi')} ({a.get('aqi_categoria')}), PM2.5: {a.get('pm25')}, Contaminante: {a.get('contaminante_dominante')}"
        for a in data_summary.get('air_quality', [])[:8]
    ]
    counts = data_summary.get('table_row_counts', {})
    
    system_prompt = f"""Eres un analista ambiental de Colombia para el proyecto Green Data ETL.
El flujo del proyecto es Bronze (fuentes originales) -> Silver (limpieza y normalización del inventario GEI y de la API de aire) -> Gold (tablas, resúmenes y SQLite).
Tienes acceso a estos datos oficiales ya procesados del Data Warehouse Gold:

### KPIs Ejecutivos:
{chr(10).join(kpi_lines) if kpi_lines else 'No hay KPIs disponibles.'}

### Macro-Sectores Emisores (Año más reciente):
{chr(10).join(sec_lines) if sec_lines else 'No hay datos de sectores.'}

### Estaciones de Calidad del Aire en Tiempo Real:
{chr(10).join(air_lines) if air_lines else 'No hay datos de estaciones.'}

### Almacén de Datos:
Total hechos GEI: {counts.get('fact_emisiones_gei_historico', 0):,} | Mediciones aire: {counts.get('fact_calidad_aire_tiempo_real', 0)}

REGLAS:
- Responde siempre en español de forma profesional, clara y estructurada.
- No confundas las capas de este proyecto con conceptos de otras áreas. Si no tienes la respuesta en el contexto, dilo claramente y no inventes cifras.
- Fundamenta tus respuestas con las cifras exactas provistas arriba.
- Sé breve y responde en hasta 3 frases, salvo que soliciten un reporte detallado.
- Si el usuario pide un reporte, organízalo con subtítulos claros y conclusiones breves.
- Sé preciso con las unidades (kt CO2eq, AQI, µg/m³).
"""

    try:
        response = requests.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "prompt": user_message,
                "system": system_prompt,
                "stream": False,
                "options": {
                    "temperature": 0.6,
                    "top_p": 0.9,
                    "num_predict": 250
                },
                "keep_alive": "5m"
            },
            timeout=300
        )
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=502, 
                detail=f"Ollama returned status {response.status_code}. Make sure Ollama is running."
            )
        
        result = response.json()
        ai_response = result.get("response", "No se recibió respuesta del modelo.")
        
        return {
            "response": ai_response,
            "model": OLLAMA_MODEL,
            "eval_duration_ms": result.get("eval_duration", 0) / 1_000_000,
            "total_duration_ms": result.get("total_duration", 0) / 1_000_000
        }
    
    except requests.exceptions.ConnectionError:
        raise HTTPException(
            status_code=503,
            detail=f"No se puede conectar con Ollama. Verifica que esté activo con: ollama run {OLLAMA_MODEL}"
        )
    except requests.exceptions.Timeout:
        raise HTTPException(
            status_code=504,
            detail="Tiempo de espera agotado al conectar con Ollama. El modelo está cargándose en memoria o procesando una respuesta extensa. Por favor reintenta en unos segundos."
        )

@app.get("/api/chat/status")
def check_ollama_status():
    """Check if Ollama is running and the model is available."""
    try:
        resp = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
        if resp.status_code == 200:
            models = resp.json().get("models", [])
            model_names = [m.get("name", "") for m in models]
            has_target = any(OLLAMA_MODEL in n for n in model_names)
            return {
                "ollama_running": True,
                "model_available": has_target,
                "model_name": OLLAMA_MODEL,
                "available_models": model_names
            }
    except:
        pass
    
    return {
        "ollama_running": False,
        "model_available": False,
        "model_name": OLLAMA_MODEL,
        "available_models": []
    }

# ──────────────────────────────────────────────────
# Reports Endpoint
# ──────────────────────────────────────────────────
@app.get("/api/reports/executive")
def get_executive_report():
    """Generates a comprehensive text-based executive report from Gold data."""
    if not db_exists():
        raise HTTPException(status_code=404, detail="Gold database not found.")
    
    summary = get_data_summary()
    kpis = summary.get("kpis", [])
    sectors = summary.get("sectors_latest", [])
    air = summary.get("air_quality", [])
    trend = summary.get("historical_trend", [])
    
    report = {
        "title": "Reporte Ejecutivo Ambiental - Green Data ETL",
        "subtitle": "Inventario Nacional GEI Colombia + Calidad del Aire",
        "kpis": kpis,
        "sector_analysis": sectors,
        "air_quality_monitoring": air,
        "historical_trend": trend,
        "conclusions": [
            f"Se procesaron {summary.get('table_row_counts', {}).get('fact_emisiones_gei_historico', 0):,} registros de emisiones históricas.",
            f"Se monitorearon {len(air)} estaciones de calidad del aire en tiempo real.",
            f"El Data Warehouse Gold contiene {sum(summary.get('table_row_counts', {}).values()):,} registros totales.",
        ]
    }
    return report

@app.get("/api/reports/quality-audit")
def run_quality_audit():
    """Runs a full data quality audit on the Gold layer."""
    if not db_exists():
        raise HTTPException(status_code=404, detail="Gold database not found.")
    
    checker = DataQualityChecker(logger=logger)
    
    with get_db_connection() as conn:
        fact_gei = pd.read_sql_query("SELECT * FROM fact_emisiones_gei_historico", conn)
        dim_tiempo = pd.read_sql_query("SELECT * FROM dim_tiempo", conn)
        dim_sector = pd.read_sql_query("SELECT * FROM dim_sector_gei", conn)
        dim_gas = pd.read_sql_query("SELECT * FROM dim_tipo_gas_contaminante", conn)
        fact_air = pd.read_sql_query("SELECT * FROM fact_calidad_aire_tiempo_real", conn)
        dim_est = pd.read_sql_query("SELECT * FROM dim_estacion_monitoreo", conn)
    
    checker.check_non_empty(fact_gei, "Fact Emisiones GEI")
    checker.check_non_empty(fact_air, "Fact Calidad Aire")
    checker.check_unique_keys(dim_sector, "Dim Sector GEI", ["sector_id"])
    checker.check_unique_keys(dim_est, "Dim Estación Monitoreo", ["estacion_id"])
    checker.check_unique_keys(dim_tiempo, "Dim Tiempo", ["anio"])
    checker.check_referential_integrity(fact_gei, dim_tiempo, "fact_gei", "dim_tiempo", "anio")
    checker.check_referential_integrity(fact_gei, dim_sector, "fact_gei", "dim_sector", "sector_id")
    checker.check_referential_integrity(fact_gei, dim_gas, "fact_gei", "dim_gas", "gas_codigo")
    checker.check_referential_integrity(fact_air, dim_est, "fact_air", "dim_estacion", "estacion_id")
    
    results = checker.get_summary_dataframe()
    passed = int((results["Status"] == "PASSED").sum())
    total = len(results)
    
    return {
        "total_tests": total,
        "passed": passed,
        "failed": total - passed,
        "pass_rate": f"{(passed/total)*100:.1f}%" if total > 0 else "0%",
        "tests": results.to_dict(orient="records")
    }

# ──────────────────────────────────────────────────
# Entry Point
# ──────────────────────────────────────────────────
if __name__ == "__main__":
    print("🌿 Green Data ETL - Backend API Server")
    print(f"   Ollama Model: {OLLAMA_MODEL}")
    print(f"   Database: {config.DB_PATH}")
    print(f"   Starting on http://{config.API_HOST}:{config.API_PORT}")
    uvicorn.run(app, host=config.API_HOST, port=config.API_PORT, log_level="info")
