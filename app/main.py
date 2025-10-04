# app/main.py
# =========================================================================================
# API de lectura de múltiples CSV desde DATA_DIR + ejecución de ETL con salidas en db/.
# Endpoints principales:
#   - GET  /datasets                         : lista archivos CSV disponibles en DATA_DIR
#   - GET  /dataset/{name}/schema            : columnas y tipos de un CSV
#   - GET  /dataset/{name}                   : JSON paginado o descarga CSV
#   - GET  /dataset/{name}/select            : subset de columnas (JSON o CSV)
#   - GET  /etl/preview                      : ejecuta ETL y devuelve ventana JSON
#   - GET  /etl/download                     : ejecuta ETL y descarga CSV/XLSX
#   - POST /etl/save-sqlite                  : ejecuta ETL y guarda resultado en SQLite (db/etl.db)
#
# Seguridad: cabecera X-API-Key (si no coincide, 401).
# CORS: abierto en dev (restringir en prod).
# =========================================================================================

# IMPORTS PRINCIPALES
from fastapi import FastAPI, HTTPException, Header, Depends, Query
# FastAPI: framework de APIs. HTTPException: respuestas HTTP con código/detalle.
# Header: leer cabeceras HTTP (X-API-Key). Depends: inyección de dependencias.
# Query: documenta/valida parámetros de query string.

from fastapi.middleware.cors import CORSMiddleware
# CORSMiddleware: permite que orígenes distintos (otro puerto/dominio) consuman la API.

from fastapi.responses import JSONResponse, Response, FileResponse, StreamingResponse
# JSONResponse: para devolver JSON controlando el contenido.
# Response: respuesta genérica (bytes, content-type, headers).
# FileResponse: servir archivos del disco (streaming eficiente).
# StreamingResponse: transmitir contenido al vuelo (CSV grande sin cargar todo en RAM).

from typing import List, Literal, Optional
# List/Optional/Literal: ayudan a FastAPI a documentar y validar los parámetros.

from dotenv import load_dotenv
# Carga variables desde .env (API_KEY, rutas).

import pandas as pd
# pandas: lectura de CSV y manejo de DataFrames (esquema, slicing, exportaciones).

import os, io, pathlib
# os: entorno y utilidades del SO; io: buffers en memoria; pathlib: manejo robusto de rutas.

import numpy as np
# numpy: manejas faltantes y convertirlos a NaN

# Importamos las funciones de la ETL (definidas en app/etl.py):
from app.etl import build_merged_dataframe, save_to_sqlite, save_to_file, OUTPUT_PATH
# build_merged_dataframe(): ejecuta el pipeline completo (lee de data/ y transforma).
# save_to_sqlite(): guarda el DataFrame resultante en SQLite (en db/).
# save_to_file(): guarda CSV/XLSX del resultado en db/.
# OUTPUT_PATH: ruta absoluta a la carpeta de salidas (db/).

# CONFIGURACIÓN INICIAL
load_dotenv()  # carga variables del archivo .env a os.environ

API_KEY = os.getenv("API_KEY", "dev-12345")               # clave requerida en X-API-Key
DATA_DIR = os.getenv("DATA_DIR", "data")                  # carpeta de entradas (CSV)
DEFAULT_DOWNLOAD_NAME = os.getenv("DEFAULT_DOWNLOAD_NAME", "dataset.csv")
MAX_JSON_ROWS = int(os.getenv("MAX_JSON_ROWS", "1000"))   # tope de filas en JSON (protege cliente)

# BASE: raíz del proyecto (carpeta que contiene 'app/')
BASE = pathlib.Path(__file__).resolve().parent.parent
# DATA_PATH: ruta absoluta a la carpeta 'data/' (CSV de entrada)
DATA_PATH = (BASE / DATA_DIR).resolve()

# Extensiones permitidas
ALLOWED_EXTS = {".csv", ".txt"}

# Si DATA_DIR no existe o no es carpeta arroja un error
if not DATA_PATH.exists() or not DATA_PATH.is_dir():
    raise RuntimeError(f"La carpeta DATA_DIR no existe o no es directorio: {DATA_PATH}")

# INSTANCIAR LA APLICACIÓN
app = FastAPI(
    title="API de DataFrames + ETL (data/ → db/)",  # Nombre de la API
    version="1.0.0",  # Versión que le definimos nosotros
    description=(
        "Sirve CSV desde data/ (JSON o descarga) y ejecuta ETL con salidas opcionales en db/ "
        "(CSV/XLSX/SQLite). Protegida con X-API-Key."
    ),
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# SEGURIDAD (API-KEY)
def require_api_key(x_api_key: str = Header(..., alias="X-API-Key")):
    """
    Dependencia de seguridad:
    - Lee la cabecera HTTP 'X-API-Key' (obligatoria, por el '...').
    - Compara con API_KEY (de .env). Si no coincide, devuelve 401.
    """
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="API key inválida")

# UTILIDADES DE ARCHIVOS/DF
def _ext_of(name: str) -> str:
    """Devuelve la extensión del archivo en minúsculas (incluye el punto)."""
    _, ext = os.path.splitext(name)
    return ext.lower()

def _is_allowed_file(path: pathlib.Path) -> bool:
    """True si es archivo regular y su extensión está permitida."""
    return path.is_file() and _ext_of(path.name) in ALLOWED_EXTS

def _sanitize_name(name: str) -> str:
    """
    Seguridad: evita 'path traversal'.
    Solo se aceptan nombres base sin barras ni '..'
    """
    if "/" in name or "\\" in name or ".." in name:
        raise HTTPException(400, "Nombre inválido")
    return name

def _resolve_dataset_path(name: str) -> pathlib.Path:
    """
    Dado un nombre de archivo, devuelve su ruta absoluta dentro de DATA_PATH.
    Verifica que esté bajo data/, que exista y que tenga extensión permitida.
    """
    safe = _sanitize_name(name)
    p = (DATA_PATH / safe).resolve()

    # Impide que se salga de DATA_PATH 
    if DATA_PATH not in p.parents and DATA_PATH != p.parent:
        raise HTTPException(400, "Ruta fuera de DATA_DIR")

    if not p.exists() or not _is_allowed_file(p):
        raise HTTPException(404, f"Dataset no encontrado o no permitido: {safe}")

    return p

def _read_df(path: pathlib.Path) -> pd.DataFrame:
    """
    Lee un DataFrame desde CSV/TXT con pandas.
    """
    ext = _ext_of(path.name)
    if ext in [".csv", ".txt"]:
        return pd.read_csv(path)
    elif ext in ["xls", "xlsx"]:
        return pd.read_excel(path)
    raise HTTPException(400, f"Formato no soportado para lectura con pandas: {ext}")

def _df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """
    Exporta un DataFrame a CSV en memoria (bytes).
    Útil cuando el origen no es CSV (p. ej., Excel) y quieres devolver CSV.
    """
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")

# ENDPOINTS: META
@app.get("/", tags=["Meta"])
def root():
    """
    Ping de salud para comprobar que la API está corriendo.
    """
    return {"status": "ok", "service": "df-api-etl", "version": "1.0.0"}

# ENDPOINTS: DATA
@app.get("/datasets", dependencies=[Depends(require_api_key)], tags=["Data"])
def list_datasets():
    """
    Lista los datasets disponibles en DATA_DIR con extensiones permitidas.
    Retorna: name (para usar en {name}), size_bytes y ext.
    """
    items = []
    for p in sorted(DATA_PATH.iterdir()):  # recorre entradas en data/
        if _is_allowed_file(p):            # solo archivos permitidos
            items.append({
                "name": p.name,
                "size_bytes": p.stat().st_size,
                "ext": _ext_of(p.name),
            })
    return items

@app.get("/dataset/{name}/schema", dependencies=[Depends(require_api_key)], tags=["Data"])
def dataset_schema(name: str):
    """
    Devuelve columnas, tipos y filas totales de un CSV en data/.
    """
    path = _resolve_dataset_path(name)
    df = _read_df(path)
    info = [{"col": c, "dtype": str(t)} for c, t in zip(df.columns, df.dtypes)]
    return {"name": path.name, "rows": int(len(df)), "columns": info}

@app.get("/dataset/{name}", dependencies=[Depends(require_api_key)], tags=["Data"])
def get_dataset(
    name: str,
    format: Literal["json", "csv"] = "csv",          # 'json' → ventana; 'csv' → archivo completo
    limit: int = Query(100, ge=1, le=1_000_000),     # tamaño de ventana JSON
    offset: int = Query(0, ge=0),                    # desplazamiento (paginación)
    filename: Optional[str] = None                   # nombre sugerido de descarga CSV
):
    """
    Obtiene un dataset de data/:
    - format=json → devuelve una ventana (offset+limit) en JSON, recortada por MAX_JSON_ROWS.
    - format=csv  → descarga el archivo completo. Si ya es CSV/TXT, se sirve por streaming eficiente.
    """
    path = _resolve_dataset_path(name)

    if format == "json":
        df = _read_df(path)
        chunk = min(limit, MAX_JSON_ROWS)           # protege a clientes/navegadores
        view = df.iloc[offset: offset + chunk]
        return JSONResponse(content=view.to_dict(orient="records"))

    # En CSV: preparamos headers para forzar descarga
    ext = _ext_of(path.name)
    suggested = filename or path.name               # nombre sugerido de archivo
    headers = {"Content-Disposition": f'attachment; filename="{suggested}"'}

    if ext in [".csv", ".txt"]:
        # Si ya es CSV/TXT, servimos el archivo directamente desde disco (streaming)
        return FileResponse(path, media_type="text/csv", headers=headers)

    # Si en el futuro aceptas Excel, podrías convertir a CSV en memoria:
    df = _read_df(path)
    csv_bytes = _df_to_csv_bytes(df)
    return Response(content=csv_bytes, media_type="text/csv", headers=headers)

@app.get("/dataset/{name}/select", dependencies=[Depends(require_api_key)], tags=["Data"])
def get_dataset_select(
    name: str,
    cols: List[str] = Query(..., description="Repite ?cols= para varias columnas"),
    format: Literal["json", "csv"] = "csv",
    limit: int = Query(100, ge=1, le=1_000_000),
    offset: int = Query(0, ge=0),
    filename: Optional[str] = None
):
    """
    Devuelve un SUBCONJUNTO por columnas del dataset:
    - JSON: ventana paginada (offset+limit), recortada por MAX_JSON_ROWS.
    - CSV : descarga el subset completo generado en memoria.
    """
    path = _resolve_dataset_path(name)
    df = _read_df(path)

    # Validar columnas solicitadas
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise HTTPException(400, f"Columnas inexistentes en '{name}': {missing}")

    sub = df[cols]

    if format == "json":
        chunk = min(limit, MAX_JSON_ROWS)
        view = sub.iloc[offset: offset + chunk]
        return JSONResponse(content=view.to_dict(orient="records"))
    else:
        csv_bytes = _df_to_csv_bytes(sub)
        suggested = filename or f"subset_{path.name}"
        headers = {"Content-Disposition": f'attachment; filename="{suggested}"'}
        return Response(content=csv_bytes, media_type="text/csv", headers=headers)

# ENDPOINTS: ETL
@app.get("/etl/preview", dependencies=[Depends(require_api_key)], tags=["ETL"])
def etl_preview(limit: int = Query(100, ge=1, le=1_000_000),
                offset: int = Query(0, ge=0)):
    """
    Ejecuta tu ETL y devuelve una ventana JSON.
    Captura errores y limpia NaN/Inf -> None (JSON-friendly).
    """
    try:
        df = build_merged_dataframe()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"ETL falló: {type(e).__name__}: {e}")

    chunk = min(limit, MAX_JSON_ROWS)
    view = df.iloc[offset: offset + chunk].copy()

    # --- SANITIZAR PARA JSON ---
    # 1) Reemplaza inf/-inf por NaN
    view.replace([np.inf, -np.inf], pd.NA, inplace=True)
    # 2) Convierte cualquier NaN a None (para que json.dumps lo acepte)
    view = view.where(pd.notna(view), None)

    return JSONResponse(content=view.to_dict(orient="records"))



@app.get("/etl/download", dependencies=[Depends(require_api_key)], tags=["ETL"])
def etl_download(format: Literal["csv", "xlsx"] = "csv",
                 filename: Optional[str] = None,
                 persist: bool = False):
    """
    Ejecuta la ETL y descarga el resultado completo:
      - format=csv  : StreamingResponse (líneas de CSV al vuelo, eficiente para grandes).
                      Si persist=True, además guarda en db/{filename}.csv
      - format=xlsx : Genera Excel (en memoria). Si persist=True, guarda en db/{filename}.xlsx
    """
    df = build_merged_dataframe()
    base = (filename or os.getenv("DEFAULT_DOWNLOAD_NAME", "df_etl.csv")).replace(".csv", "")

    if format == "csv":
        # Si persist=True, guardamos un archivo en db/ además de descargar
        saved_path = None
        if persist:
            saved_path = save_to_file(df, base, fmt="csv")   # guarda en db/base.csv

        # Generador de líneas CSV (cabecera + filas), transmitidas como streaming
        def generate():
            # cabecera
            yield (",".join(map(str, df.columns)) + "\n").encode("utf-8")
            # filas
            for row in df.itertuples(index=False, name=None):
                yield (",".join("" if (x is None) else str(x) for x in row) + "\n").encode("utf-8")

        headers = {"Content-Disposition": f'attachment; filename="{base}.csv"'}
        resp = StreamingResponse(generate(), media_type="text/csv", headers=headers)
        if saved_path:
            # header informativo (opcional) con la ruta donde se guardó en db/
            resp.headers["X-Saved-Path"] = str(saved_path.resolve())
        return resp

    # format == "xlsx"
    if persist:
        # Guardar a archivo en db/ y servirlo directamente
        saved_path = save_to_file(df, base, fmt="xlsx")      # guarda en db/base.xlsx
        headers = {"Content-Disposition": f'attachment; filename="{saved_path.name}"'}
        return FileResponse(saved_path, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers=headers)
    else:
        # Generar XLSX en memoria para descarga directa
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="xlsxwriter") as w:
            df.to_excel(w, sheet_name="DataFrame", index=False)
        bio.seek(0)
        headers = {"Content-Disposition": f'attachment; filename="{base}.xlsx"'}
        return Response(content=bio.read(), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers=headers)

@app.post("/etl/save-sqlite", dependencies=[Depends(require_api_key)], tags=["ETL"])
def etl_save_sqlite(table: Optional[str] = None,
                    if_exists: Literal["replace","append","fail"] = "replace"):
    """
    Ejecuta la ETL y guarda el resultado en SQLite dentro de db/ (ruta definida en .env).
    - table: nombre de la tabla (por defecto SQLITE_TABLE del .env).
    - if_exists: 'replace' (sobrescribe), 'append' (anexa), 'fail' (error si existe).
    Devuelve un pequeño resumen (rutas/filas).
    """
    df = build_merged_dataframe()
    save_to_sqlite(df, table=table, if_exists=if_exists)

    # OUTPUT_PATH proviene de app.etl (normalmente db/)
    sqlite_path = (OUTPUT_PATH / "etl.db").resolve()
    return {
        "status": "ok",
        "rows": int(len(df)),
        "sqlite_path": str(sqlite_path),
        "table": table or os.getenv("SQLITE_TABLE", "nhanes_etl"),
        "if_exists": if_exists
    }
