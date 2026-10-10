"""
Global Settings Module for Green Data ETL Pipeline
Medallion Architecture (Bronze -> Silver -> Gold)
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Base Paths (Project Root is parent of config folder)
CONFIG_DIR = Path(__file__).resolve().parent
BASE_DIR = CONFIG_DIR.parent

# Load environment variables from .env located at project root
load_dotenv(dotenv_path=BASE_DIR / ".env")

DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
NOTEBOOKS_DIR = BASE_DIR / "notebooks"
SRC_DIR = BASE_DIR / "src"

# Medallion Layers Paths
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# Sub-directories
BRONZE_API_DIR = BRONZE_DIR / "api"
BRONZE_EXCEL_DIR = BRONZE_DIR / "excel"

# Source File Paths
EXCEL_SOURCE_FILE = BASE_DIR / "Series_Históricas_del_Inventario_Nacional_de_Gases_Efecto_Invernadero_20261009.xlsx"

# Database & Reports Output Paths
DB_PATH = GOLD_DIR / "green_data_warehouse.db"
EXCEL_REPORT_PATH = GOLD_DIR / "reporte_ejecutivo_green_data.xlsx"
LOG_FILE_PATH = LOGS_DIR / "etl_pipeline.log"

# REST API Configuration (WAQI - World Air Quality Index) loaded from environment
API_TOKEN = os.getenv("WAQI_API_TOKEN", "f364f4d36e528402b023dddf8214aeb4daadd2de")
API_TARGET_STATION = os.getenv("WAQI_TARGET_STATION", "A07441")
API_FALLBACK_STATION_ID = os.getenv("WAQI_FALLBACK_STATION", "@7441")

API_TARGET_URL = f"https://api.waqi.info/feed/{API_TARGET_STATION}/?token={API_TOKEN}"
API_FALLBACK_URL = f"https://api.waqi.info/feed/{API_FALLBACK_STATION_ID}/?token={API_TOKEN}"
API_BASE_FEED_URL = "https://api.waqi.info/feed"
API_BASE_SEARCH_URL = "https://api.waqi.info/search"

# Ollama LLM Configuration loaded from environment
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1")

# Backend Server Configuration
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))

# Colombian cities/keywords to enrich environmental data context
COLOMBIA_SEARCH_KEYWORDS = ["bogota", "medellin", "cali", "barranquilla"]

# Standard IPCC Main Sectors Mapping (Clean names & codes)
IPCC_MAIN_SECTORS = {
    "1": "Energía",
    "2": "Procesos Industriales y Uso de Productos (IPPU)",
    "3": "Agricultura (AFOLU)",
    "4": "Silvicultura y Otros Usos de la Tierra (FOLU)",
    "5": "Residuos",
}

# Gas Global Warming Potential (GWP AR5 100-year timescale) & Classification
GAS_METADATA = {
    "CO2": {"nombre": "Dióxido de Carbono", "gwp": 1.0, "tipo": "GEI Directo", "unidad_base": "kt"},
    "CH4": {"nombre": "Metano", "gwp": 28.0, "tipo": "GEI Directo", "unidad_base": "kt"},
    "N2O": {"nombre": "Óxido Nitroso", "gwp": 265.0, "tipo": "GEI Directo", "unidad_base": "kt"},
    "SF6": {"nombre": "Hexafluoruro de Azufre", "gwp": 23500.0, "tipo": "Gas Fluorado", "unidad_base": "kt"},
    "HFC-125": {"nombre": "Hidrofluorocarbono 125", "gwp": 3170.0, "tipo": "Gas Fluorado", "unidad_base": "kt"},
    "HFC-134a": {"nombre": "Hidrofluorocarbono 134a", "gwp": 1300.0, "tipo": "Gas Fluorado", "unidad_base": "kt"},
    "HFC-143a": {"nombre": "Hidrofluorocarbono 143a", "gwp": 4800.0, "tipo": "Gas Fluorado", "unidad_base": "kt"},
    "HFC-227ea": {"nombre": "Hidrofluorocarbono 227ea", "gwp": 3350.0, "tipo": "Gas Fluorado", "unidad_base": "kt"},
    "HFC-23": {"nombre": "Trifluorometano (HFC-23)", "gwp": 12400.0, "tipo": "Gas Fluorado", "unidad_base": "kt"},
    "HFC-236fa": {"nombre": "Hidrofluorocarbono 236fa", "gwp": 8060.0, "tipo": "Gas Fluorado", "unidad_base": "kt"},
    "HFC-32": {"nombre": "Difluorometano (HFC-32)", "gwp": 677.0, "tipo": "Gas Fluorado", "unidad_base": "kt"},
    "HFC-152a": {"nombre": "Hidrofluorocarbono 152a", "gwp": 138.0, "tipo": "Gas Fluorado", "unidad_base": "kt"},
    "HFC-245fa": {"nombre": "Hidrofluorocarbono 245fa", "gwp": 858.0, "tipo": "Gas Fluorado", "unidad_base": "kt"},
    "HFC-365mfc": {"nombre": "Hidrofluorocarbono 365mfc", "gwp": 804.0, "tipo": "Gas Fluorado", "unidad_base": "kt"},
    "HFC-4310mee": {"nombre": "Hidrofluorocarbono 4310mee", "gwp": 1650.0, "tipo": "Gas Fluorado", "unidad_base": "kt"},
    "PFC-14": {"nombre": "Tetrafluorometano (CF4)", "gwp": 6630.0, "tipo": "Gas Fluorado", "unidad_base": "kt"},
    "PFC-218": {"nombre": "Octafluoropropano (C3F8)", "gwp": 8900.0, "tipo": "Gas Fluorado", "unidad_base": "kt"},
    "R-116 / PFC-116": {"nombre": "Hexafluoroetano (C2F6)", "gwp": 11100.0, "tipo": "Gas Fluorado", "unidad_base": "kt"},
    "HFO-1234yf": {"nombre": "Hidrofluoroolefina 1234yf", "gwp": 1.0, "tipo": "HFO", "unidad_base": "kt"},
    "HFO-1234ze": {"nombre": "Hidrofluoroolefina 1234ze", "gwp": 1.0, "tipo": "HFO", "unidad_base": "kt"},
}

# Air Quality Categories (EPA / WHO Scale)
AQI_CATEGORIES = [
    {"min": 0, "max": 50, "categoria": "Buena (Good)", "color": "#00E400", "nivel_alerta": 1, "descripcion": "Calidad del aire satisfactoria, riesgo mínimo o nulo."},
    {"min": 51, "max": 100, "categoria": "Moderada (Moderate)", "color": "#FFFF00", "nivel_alerta": 2, "descripcion": "Calidad aceptable; personas muy sensibles pueden presentar síntomas."},
    {"min": 101, "max": 150, "categoria": "Dañina para Grupos Sensibles", "color": "#FF7E00", "nivel_alerta": 3, "descripcion": "Grupos sensibles (niños, ancianos, asmáticos) pueden verse afectados."},
    {"min": 151, "max": 200, "categoria": "Dañina a la Salud (Unhealthy)", "color": "#FF0000", "nivel_alerta": 4, "descripcion": "Todos pueden comenzar a experimentar efectos sobre la salud."},
    {"min": 201, "max": 300, "categoria": "Muy Dañina (Very Unhealthy)", "color": "#8F3F97", "nivel_alerta": 5, "descripcion": "Alerta sanitaria por condiciones de emergencia para toda la población."},
    {"min": 301, "max": 9999, "categoria": "Peligrosa (Hazardous)", "color": "#7E0023", "nivel_alerta": 6, "descripcion": "Advertencia general de salud: efectos graves inmediatos."},
]
