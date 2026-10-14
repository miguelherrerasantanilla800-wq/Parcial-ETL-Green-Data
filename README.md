# 🌿 Green Data ETL Pipeline — Arquitectura Medallion

Pipeline de **Extracción, Transformación y Carga (ETL)** diseñado bajo la **Arquitectura Medallion (Bronze, Silver y Gold)** para la integración, limpieza y análisis de datos ambientales estratégicos: el **Inventario Nacional de Gases de Efecto Invernadero (GEI) de Colombia (1990–2024)** y la **Red de Monitoreo de Calidad del Aire en Tiempo Real (API REST WAQI)**.

---

## 📑 Tabla de Contenidos

1. [Resumen Ejecutivo y Objetivos](#-resumen-ejecutivo-y-objetivos)
2. [Arquitectura General del Sistema (Medallion)](#-arquitectura-general-del-sistema-medallion)
3. [Flujo de Procesamiento de Datos](#-flujo-de-procesamiento-de-datos)
4. [Modelo Dimensional (Esquema en Estrella y Data Marts)](#-modelo-dimensional-esquema-en-estrella-y-data-marts)
5. [Fuentes de Información](#-fuentes-de-información)
6. [Detalle de Transformaciones por Capa](#-detalle-de-transformaciones-por-capa)
7. [Gobernanza y Auditoría de Calidad de Datos](#-gobernanza-y-auditoría-de-calidad-de-datos)
8. [Estructura del Repositorio](#-estructura-del-repositorio)
9. [Guía de Instalación y Ejecución](#-guía-de-instalación-y-ejecución)
10. [Consultas Analíticas SQL y Toma de Decisiones](#-consultas-analíticas-sql-y-toma-de-decisiones)

---

## 🎯 Resumen Ejecutivo y Objetivos

El objetivo principal es construir un flujo de datos robusto, escalable y reproducible en **Python** que unifique dos fuentes ambientales clave:

1. **Fuente Tabular / Batch (Excel):** Serie histórica oficial de inventario de gases de efecto invernadero (12,156 registros, 47 variables, 35 años: 1990–2024, categorizado por directrices IPCC).
2. **Fuente Streaming / API REST:** Mediciones en tiempo real de contaminantes criterio del aire ($PM_{2.5}$, $PM_{10}$, $O_3$, $NO_2$, $SO_2$, $CO$) e Índice de Calidad del Aire (AQI) provistos por el proyecto *World Air Quality Index* (WAQI).

A través del pipeline, los datos sin procesar se transforman progresivamente en un **Data Warehouse analítico en SQLite** y un **Reporte Ejecutivo en Excel**, permitiendo a tomadores de decisiones evaluar trayectorias de descarbonización, cumplimiento de las NDC 2030 y alertas tempranas de contaminación urbana.

---

## 🏛 Arquitectura General del Sistema (Medallion)

```mermaid
flowchart TD
    subgraph SOURCELAYER [" FUENTES DE DATOS "]
        SRC_API["📡 REST API WAQI<br/>(Índice de Calidad del Aire)"]
        SRC_XLS["📊 Archivo Excel<br/>(Inventario Nacional GEI 1990-2024)"]
    end

    subgraph BRONZELAYER [" CAPA BRONZE (Ingestión Cruda) "]
        BRZ_API["📥 data/bronze/api/<br/>raw_waqi_api_extract.json<br/>(Payload crudo + Metadata)"]
        BRZ_XLS["📥 data/bronze/excel/<br/>raw_gei_inventory.csv<br/>(12,156 filas + SHA256 + Time)"]
    end

    subgraph SILVERLAYER [" CAPA SILVER (Limpieza y Conformación) "]
        SLV_CLEAN["🧹 Limpieza de Codificación UTF-8 / Reparación de Caracteres"]
        SLV_PARSE["🌳 Extracción de Jerarquías IPCC (Macro-Sector, Subsector, Nivel)"]
        SLV_NORM["🔄 Normalización y Unpivot de Gases a Formato Largo (Tidy Facts)"]
        SLV_VAL["⚖️ Validación Balance Matemático: Netas = Brutas + Absorciones"]
        SLV_AIR["🌬️ Aplanamiento JSON API, Georreferenciación y Clasificación AQI OMS/EPA"]
    end

    subgraph GOLDLAYER [" CAPA GOLD (Modelado Dimensional y Data Marts) "]
        DIM_T["📅 dim_tiempo<br/>(1990-2024, Eras Climáticas, Hitos)"]
        DIM_S["🏭 dim_sector_gei<br/>(352 Sectores y Códigos IPCC)"]
        DIM_G["🧪 dim_tipo_gas_contaminante<br/>(26 Gases GEI + Contaminantes Criterio + GWP)"]
        DIM_E["📍 dim_estacion_monitoreo<br/>(Estaciones, Coordenadas, Ciudades)"]
        FACT_G["📈 fact_emisiones_gei_historico<br/>(20,083 hechos dimensionales)"]
        FACT_A["⏱️ fact_calidad_aire_tiempo_real<br/>(AQI, PM2.5, PM10, Variables Meteo)"]
        MART_C["📑 mart_consolidado_ambiental<br/>(Series y Participación por Sector)"]
        MART_K["🎯 mart_kpis_resumen_ejecutivo<br/>(Indicadores Clave de Decisión)"]
    end

    subgraph SERVINGLAYER [" CAPA DE SERVICIO Y ANALÍTICA "]
        DB_SQL["🗄️ SQLite Data Warehouse<br/>(green_data_warehouse.db + Índices)"]
        XLS_REP["📑 Reporte Ejecutivo Excel<br/>(reporte_ejecutivo_green_data.xlsx)"]
        CSV_PUB["📄 Data Marts en CSV / Parquet"]
    end

    SRC_API --> BRZ_API
    SRC_XLS --> BRZ_XLS

    BRZ_API --> SLV_AIR
    BRZ_XLS --> SLV_CLEAN --> SLV_PARSE --> SLV_NORM --> SLV_VAL

    SLV_PARSE --> DIM_S
    SLV_NORM --> FACT_G
    SLV_VAL --> DIM_T
    SLV_VAL --> DIM_G
    SLV_AIR --> DIM_E
    SLV_AIR --> FACT_A

    FACT_G & DIM_S & DIM_T --> MART_C
    FACT_G & FACT_A & MART_C --> MART_K

    DIM_T & DIM_S & DIM_G & DIM_E & FACT_G & FACT_A & MART_C & MART_K --> DB_SQL
    DIM_T & DIM_S & DIM_G & DIM_E & FACT_G & FACT_A & MART_C & MART_K --> XLS_REP
    DIM_T & DIM_S & DIM_G & DIM_E & FACT_G & FACT_A & MART_C & MART_K --> CSV_PUB
```

---

## 🔄 Flujo de Procesamiento de Datos

```mermaid
sequenceDiagram
    autonumber
    actor CLI as Usuario / Orquestador
    participant Extractor as Ingestión (Bronze)
    participant Transformer as Transformación (Silver)
    participant Modeler as Modelado (Gold)
    participant Quality as Auditoría de Calidad
    participant Storage as Almacenamiento (Warehouse & Excel)

    CLI->>Extractor: Ejecutar run_pipeline.py
    Extractor->>Extractor: Descargar API WAQI (Target @7441 + Red Colombia)
    Extractor->>Extractor: Leer Excel Inventario GEI + Calcular Checksum SHA256
    Extractor->>Quality: Auditar dataset no vacío y rango temporal
    Quality-->>Extractor: Ingestión Bronze Validada

    Extractor->>Transformer: Transferir datos crudos Bronze
    Transformer->>Transformer: Reparar caracteres en español (Energía, Gestión, etc.)
    Transformer->>Transformer: Parsear códigos IPCC y jerarquías (Niveles 1 a 6)
    Transformer->>Transformer: Unpivot de 20 gases y cálculo de CO2eq (GWP AR5)
    Transformer->>Transformer: Verificar balance matemático (Netas = Brutas + Absorciones)
    Transformer->>Transformer: Aplanar JSON de API y mapear escalas AQI
    Transformer->>Quality: Auditar claves únicas (PK) y umbral nulos
    Quality-->>Transformer: Transformación Silver Validada

    Transformer->>Modeler: Construir Esquema en Estrella Gold
    Modeler->>Modeler: Generar Dimensiones (Tiempo, Sector, Gas, Estación)
    Modeler->>Modeler: Generar Tablas de Hechos (Fact GEI + Fact Calidad Aire)
    Modeler->>Modeler: Construir Data Mart Consolidado y KPIs Ejecutivos
    Modeler->>Quality: Auditar Integridad Referencial (FKs)
    Quality-->>Modeler: Modelo Gold Validado (12/12 Pruebas OK)

    Modeler->>Storage: Cargar tablas e índices en SQLite (green_data_warehouse.db)
    Modeler->>Storage: Generar Libro Excel con múltiples pestañas (openpyxl)
    Storage-->>CLI: Pipeline completado con éxito + Demostración SQL
```

---

## 🌟 Modelo Dimensional (Esquema en Estrella y Data Marts)

```mermaid
erDiagram
    dim_tiempo ||--o{ fact_emisiones_gei_historico : "anio"
    dim_sector_gei ||--o{ fact_emisiones_gei_historico : "sector_id"
    dim_tipo_gas_contaminante ||--o{ fact_emisiones_gei_historico : "gas_codigo"

    dim_estacion_monitoreo ||--o{ fact_calidad_aire_tiempo_real : "estacion_id"

    dim_tiempo {
        int anio PK
        string decada
        string era_climatica
        string hito_ambiental
        string periodo_ndc_2030
    }

    dim_sector_gei {
        string sector_id PK
        string codigo_ipcc
        string sector_macro_id
        string sector_macro_nombre
        string categoria_descripcion
        string clasificacion_completa
        int nivel_jerarquia
    }

    dim_tipo_gas_contaminante {
        string gas_codigo PK
        string nombre_completo
        string categoria_ambiental
        float potencial_gwp_ar5
        string unidad_estandar
        string impacto_principal
    }

    dim_estacion_monitoreo {
        int estacion_id PK
        string estacion_nombre
        string ciudad
        string pais
        float latitud
        float longitud
        string agencia_monitoreo
        string url_estacion
    }

    fact_emisiones_gei_historico {
        int fact_gei_id PK
        int anio FK
        string sector_id FK
        string gas_codigo FK
        float emision_kt
        float emision_co2eq_kt
        float emisiones_brutas_sector_kt
        float absorciones_sector_kt
        float emisiones_netas_sector_kt
    }

    fact_calidad_aire_tiempo_real {
        int fact_air_id PK
        int estacion_id FK
        string timestamp_iso
        string timestamp_local
        string timezone_offset
        float aqi
        string aqi_categoria
        int nivel_alerta
        string contaminante_dominante
        float pm25
        float pm10
        float o3
        float no2
        float so2
        float co
        float temperatura_c
        float humedad_relativa_pct
        float presion_hpa
        float velocidad_viento_ms
    }

    mart_consolidado_ambiental {
        int anio
        string sector_macro_id
        string sector_macro_nombre
        float total_co2eq_kt
        float emisiones_brutas_kt
        float absorciones_kt
        float emisiones_netas_kt
        float participacion_pct
    }

    mart_kpis_resumen_ejecutivo {
        string kpi_nombre
        string valor
        string unidad
        string estado
        string interpretacion
    }
```

---

## 📊 Fuentes de Información

| Fuente                                   | Tipo          | Formato Original  | Volumen / Frecuencia                  | Descripción                                                                                                             |
| :--------------------------------------- | :------------ | :---------------- | :------------------------------------ | :----------------------------------------------------------------------------------------------------------------------- |
| **Inventario Nacional GEI**        | Archivo Local | Excel (`.xlsx`) | 12,156 registros / Anual (1990–2024) | Emisiones históricas de GEI en Colombia clasificadas según metodología IPCC en 352 categorías.                       |
| **World Air Quality Index (WAQI)** | REST API      | JSON              | Tiempo real / Estaciones Urbanas      | Mediciones de calidad del aire ($PM_{2.5}$, $PM_{10}$, $O_3$, etc.), geolocalización y variables meteorológicas. |

> [!NOTE]
> **Resolución de la API**: La URL base solicitada `https://api.waqi.info/feed/A07441/?token=...` fue procesada con resolución automática de la estación `@7441` (estación US EPA en red WAQI) y enriquecida con la red de monitoreo de calidad del aire en ciudades colombianas (Bogotá, Medellín, Cali) para contrastar los inventarios macro con la calidad del aire local.

---

## 🛠 Detalle de Transformaciones por Capa

### 1. Capa Bronze (Raw Ingestion)

- **Excel Ingestion**: Lectura fiel de las 47 columnas de la hoja `Data`, cálculo del hash criptográfico `SHA-256`, tamaño en bytes y adición de metadatos técnicos (`_ingested_at`, `_source_file`, `_source_row_index`).
- **API Ingestion**: Consulta HTTP con reintentos y persistencia completa del JSON crudo con cabeceras y códigos de respuesta en `data/bronze/api/raw_waqi_api_extract.json`.

### 2. Capa Silver (Cleaning & Conformance)

- **Reparación de Encodings**: Corrección de textos corruptos en español (e.g. `AO` $\rightarrow$ `anio`, `Energa` $\rightarrow$ `Energía`, `Gestin` $\rightarrow$ `Gestión`, `Maz` $\rightarrow$ `Maíz`, `Bfalos` $\rightarrow$ `Búfalos`).
- **Parsing de Jerarquías IPCC**: Extracción mediante expresiones regulares del código IPCC (e.g. `3.B.1.a.iv.1`), macro-sector (`1. Energía`, `2. IPPU`, `3. AFOLU`, `4. FOLU`, `5. Residuos`), subsector, descripción de la actividad y nivel de profundidad (1 al 6).
- **Unpivot y Normalización Dimensional**: Transformación del formato ancho (wide) a formato ordenado (tidy/long), generando 20,083 hechos granulares de gases con su respectivo potencial de calentamiento global ($CO_2$, $CH_4$, $N_2O$, $SF_6$, HFCs, PFCs, HFOs).
- **Validación de Balance Físico**: Verificación de la identidad contable ambiental:
  $$
  \text{Emisiones Netas} = \text{Emisiones Brutas} + \text{Absorciones}
  $$

  *(Donde las absorciones de sumideros forestales se expresan con signo negativo en el inventario)*. Satisfacción del 100% de los registros.
- **Normalización de Calidad del Aire**: Desanidación de variables meteorológicas y contaminantes, cálculo de categorías de riesgo según la escala EPA/OMS (Buena, Moderada, Dañina para Grupos Sensibles, Dañina, Muy Dañina, Peligrosa).

### 3. Capa Gold (Dimensional Modeling & Data Marts)

- **Esquema en Estrella**: 4 tablas de dimensiones (`dim_tiempo`, `dim_sector_gei`, `dim_tipo_gas_contaminante`, `dim_estacion_monitoreo`) y 2 tablas de hechos (`fact_emisiones_gei_historico`, `fact_calidad_aire_tiempo_real`).
- **Data Mart Consolidado Ambiental**: Agregaciones anuales por macro-sector con cálculo de participación porcentual nacional y trayectoria de emisiones.
- **Data Mart de KPIs Ejecutivos**: Métricas macro para toma de decisiones (Emisiones netas 2024, CAGR histórico 1990–2024, capacidad de absorción forestal, sector de mayor impacto y alerta en tiempo real).
- **Almacenamiento y Salidas**: Base de datos relacional SQLite `green_data_warehouse.db` con índices optimizados y libro de trabajo Excel `reporte_ejecutivo_green_data.xlsx`.

---

## 🛡 Gobernanza y Auditoría de Calidad de Datos

El pipeline incorpora un motor automatizado de pruebas de calidad (`DataQualityChecker`) que valida las siguientes reglas en cada ejecución:

| Capa             | Test           | Regla de Calidad                                                                              |    Resultado    |
| :--------------- | :------------- | :-------------------------------------------------------------------------------------------- | :--------------: |
| **Bronze** | No Vacío      | `len(df) > 0` (12,156 filas leídas)                                                        | **PASSED** |
| **Bronze** | Rango Temporal | `anio` entre 1990 y 2030 (0 violaciones)                                                    | **PASSED** |
| **Silver** | No Vacío      | Hechos de gases normalizados > 20,000                                                         | **PASSED** |
| **Silver** | No Vacío      | Estaciones de monitoreo > 0                                                                   | **PASSED** |
| **Silver** | Unicidad PK    | `sector_id` único sin duplicados en catálogo                                              | **PASSED** |
| **Silver** | Unicidad PK    | `estacion_id` único sin duplicados en catálogo                                            | **PASSED** |
| **Silver** | Umbral Nulos   | Nulos = 0% en columnas críticas de hechos                                                    | **PASSED** |
| **Silver** | Rango AQI      | $0 \le AQI \le 500$ (0 violaciones)                                                         | **PASSED** |
| **Gold**   | Integridad FK  | `fact_emisiones_gei.anio` $\rightarrow$ `dim_tiempo.anio` (0 huérfanos)                | **PASSED** |
| **Gold**   | Integridad FK  | `fact_emisiones_gei.sector_id` $\rightarrow$ `dim_sector_gei.sector_id` (0 huérfanos)  | **PASSED** |
| **Gold**   | Integridad FK  | `fact_emisiones_gei.gas_codigo` $\rightarrow$ `dim_tipo_gas.gas_codigo` (0 huérfanos)  | **PASSED** |
| **Gold**   | Integridad FK  | `fact_calidad_aire.estacion_id` $\rightarrow$ `dim_estacion.estacion_id` (0 huérfanos) | **PASSED** |

---

## 📁 Estructura del Repositorio (Estructura Modular)

```text
├── config/                        # Módulo de configuración global
│   ├── __init__.py
│   └── settings.py                # Variables de entorno, rutas, endpoints y tokens
├── data/                          # Almacenamiento Medallion (Data Lake y Warehouse)
│   ├── bronze/                    # Ingestión cruda (JSON de API y CSV de Excel)
│   ├── silver/                    # Tablas limpias, reparadas y normalizadas
│   └── gold/                      # Tablas dimensionales, hechos y data marts
│       ├── green_data_warehouse.db           # SQLite Data Warehouse
│       └── reporte_ejecutivo_green_data.xlsx # Reporte Excel Ejecutivo
├── logs/                          # Registro histórico de logs del pipeline
│   └── etl_pipeline.log
├── notebooks/                     # Notebooks de análisis exploratorio
│   └── analisis_ambiental_gei_waqi.ipynb
├── src/                           # Código fuente de la solución ETL
│   ├── __init__.py
│   ├── app_tkinter.py             # 🖥️ Aplicación de Escritorio GUI Interactiva (Tkinter)
│   ├── extract/                   # Módulos de extracción (Bronze)
│   │   ├── __init__.py
│   │   ├── api_extractor.py       # Extracción REST API (WAQI)
│   │   └── excel_extractor.py     # Ingestión Excel Inventario GEI
│   ├── transform/                 # Módulos de transformación (Silver y Gold)
│   │   ├── __init__.py
│   │   ├── bronze_to_silver.py    # Limpieza, unpivot, parsing y balance
│   │   └── silver_to_gold.py      # Esquema en estrella, hechos y data marts
│   ├── load/                      # Módulos de carga y publicación
│   │   ├── __init__.py
│   │   └── database_loader.py     # Carga en SQLite y exportación Excel
│   └── quality/                   # Módulos de calidad y gobernanza
│       ├── __init__.py
│       └── quality_checker.py     # Motor de validación automatizado
├── test/                          # Pruebas unitarias e integración
│   ├── __init__.py
│   └── test_etl.py                # Test suite completo (100% aprobado)
├── Series_Históricas_...xlsx      # Dataset origen: Inventario Nacional GEI
└── requirements.txt               # Dependencias del proyecto
```

---

## 🚀 Guía de Instalación y Ejecución

### 1. Requisitos Previos

- **Python 3.10 o superior**
- Conexión a Internet (para la consulta a la API REST)

### 2. Instalación de Dependencias

```bash
pip install -r requirements.txt
```

### 3. Ejecución mediante la Aplicación de Escritorio (GUI Tkinter)

Para ejecutar y visualizar todo el proceso de forma interactiva con interfaz gráfica:

```bash
python src/app_tkinter.py
```

> **Funcionalidades de la Aplicación Gráfica:**
>
> - 🕹️ **Panel de Control:** Botones para ejecutar el pipeline completo o fases individuales (Bronze, Silver, Gold), barra de progreso y consola de log en vivo.
> - 📊 **Explorador de Datos:** Visor interactivo de tablas con buscador y conteo de registros para examinar cualquier capa del Data Lake.
> - 📈 **Dashboard & Resumen Ejecutivo:** Tarjetas de KPIs macro, ranking de emisiones por sector y monitoreo de calidad del aire en tiempo real.
> - 🗄️ **Consultas SQL en Vivo:** Editor SQL interactivo conectado a `green_data_warehouse.db` con consultas predefinidas.
> - 📁 **Archivos & Reportes:** Botones de acceso directo para abrir el reporte en Excel y la base de datos SQLite.

### 4. Ejecución de Pruebas Automatizadas

```bash
python -m unittest test/test_etl.py
```

---

## 💡 Consultas Analíticas SQL y Toma de Decisiones

La base de datos SQLite `data/gold/green_data_warehouse.db` contiene las tablas e índices listos para conectarse a herramientas de BI (Power BI, Tableau) o ejecutarse mediante consultas SQL:

### Consulta 1: Balance de Emisiones por Macro-Sector en el Año 2024

```sql
SELECT 
    anio,
    sector_macro_nombre,
    ROUND(emisiones_brutas_kt, 2) AS emisiones_brutas_kt,
    ROUND(absorciones_kt, 2) AS absorciones_kt,
    ROUND(emisiones_netas_kt, 2) AS emisiones_netas_kt,
    participacion_pct || '%' AS participacion_nacional
FROM mart_consolidado_ambiental
WHERE anio = (SELECT MAX(anio) FROM mart_consolidado_ambiental)
ORDER BY emisiones_brutas_kt DESC;
```

**Resultado Obtenido:**

- **1. Energía:** $105,306.19\text{ kt } CO_2eq$ (**40.71%** de las emisiones brutas nacionales).
- **2. Agricultura (AFOLU):** $58,033.37\text{ kt } CO_2eq$ (**22.43%**).
- **3. Silvicultura y Usos de la Tierra (FOLU):** $57,689.79\text{ kt } CO_2eq$ (**22.30%** brutas) con **$-12,195.55\text{ kt } CO_2eq$ de absorciones forestales**.
- **4. Residuos:** $25,982.20\text{ kt } CO_2eq$ (**10.04%**).
- **5. Procesos Industriales (IPPU):** $11,669.23\text{ kt } CO_2eq$ (**4.51%**).

---

### Consulta 2: Monitoreo en Tiempo Real de Calidad del Aire por Estación

```sql
SELECT 
    e.estacion_nombre,
    e.ciudad,
    f.aqi,
    f.aqi_categoria,
    f.contaminante_dominante,
    f.pm25,
    f.pm10,
    f.temperatura_c
FROM fact_calidad_aire_tiempo_real f
JOIN dim_estacion_monitoreo e ON f.estacion_id = e.estacion_id
ORDER BY f.aqi DESC;
```

---

### 📌 Hallazgos Clave para Políticas Ambientales

1. **Prioridad en Descarbonización del Sector Energía (40.7%):** El transporte vehicular y la generación termoeléctrica concentran el mayor crecimiento ($CAGR = +1.06\%$ anual). Se requiere acelerar la electrificación de flotas y matrices renovables.
2. **Preservación de Sumideros Forestales (FOLU):** Las absorciones forestales mitigan más de $12,000\text{ kt } CO_2eq$ anuales. Combatir la deforestación en la Amazonía y cordilleras es vital para mantener la meta de carbono neutralidad 2050.
3. **Puntos Críticos Urbanos en Calidad del Aire:** Las estaciones urbanas (como Centro de Alto Rendimiento y Kennedy en Bogotá) presentan concentraciones de $PM_{2.5}$ en rango *Moderado/Alerta*, correlacionadas con las emisiones del transporte diésel y combustión fósil.
