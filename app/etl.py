#   - Leer SIEMPRE desde DATA_DIR
#   - Guardar salidas SIEMPRE en OUTPUT_DIR
#   - Exponer funciones para usar desde la API

import os
import pathlib
import numpy as np
import pandas as pd
from dotenv import load_dotenv

# Carga las variables definidas en .env a os.environ (API_KEY, DATA_DIR, etc.)
load_dotenv()

# BASE: ruta absoluta a la carpeta raíz del proyecto (que contiene 'app/')
BASE = pathlib.Path(__file__).resolve().parent.parent # __file__: ruta del archivo actual, .resolve(): convierte a ruta absoluta sin símbolos, .parent.parent: sube dos niveles para quedar en la raíz del proyecto (carpeta que contiene app/)

# Entradas (tus CSV existentes)
DATA_DIR = os.getenv("DATA_DIR", "data")
DATA_PATH = (BASE / DATA_DIR).resolve()

# Salidas (todo lo que exportemos lo pondremos aquí)
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "db")
OUTPUT_PATH = (BASE / OUTPUT_DIR).resolve()
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)  # Crea la carpeta db, si ya existe, no lanza error

# SQLite (dentro de db/)
SQLITE_PATH = (BASE / os.getenv("SQLITE_PATH", "db/etl.db")).resolve()
SQLITE_TABLE = os.getenv("SQLITE_TABLE", "nhanes_etl")

if not DATA_PATH.exists() or not DATA_PATH.is_dir():
    raise RuntimeError(f"La carpeta DATA_DIR no existe o no es directorio: {DATA_PATH}")

# Columnas necesarias para cada csv
C_DEMOGRAPHIC = ["SEQN_new","RIDRETH1","DMDEDUC2","DMDMARTL","DMDBORN4","WTMEC2YR","WTINT2YR","SDMVPSU","SDMVSTRA"]
C_RESPONSE    = ["SEQN_new","LBXSCR","URXUMA","URXUCR","BMXWT","BMXHT","BMXBMI","BPXSY1","BPXSY2","BPXSY3","BPXSY4","BPXDI1","BPXDI2","BPXDI3","BPXDI4"]
C_QUEST       = ["SEQN_new","DIQ010","BPQ020","MCQ160B","MCQ160C","MCQ160F","MCQ160M","ALQ101","PAQ650","PAQ665"]
C_MORT        = ["SEQN_new","MORTSTAT","UCOD_LEADING"]
C_DIET        = ["SEQN_new","RIDAGEYR","RIAGENDR"]

# Funcion para leer csvs con las columnas escogidas para cada csv
def _csv(path: pathlib.Path, usecols):
    # Condicional que si no existe el archivo, lance un error
    if not path.exists():
        raise FileNotFoundError(f"No se encontró el archivo: {path}")
    return pd.read_csv(path, usecols=usecols)  # Retorna la lectura del csv 

def read_inputs() -> dict[str, pd.DataFrame]:
    """Lee los 5 CSV desde DATA_PATH con usecols."""
    return {
        "demographic":   _csv(DATA_PATH / "demographics_unclean.csv", C_DEMOGRAPHIC),
        "dietary":       _csv(DATA_PATH / "dietary_unclean.csv",        C_DIET),
        "response":      _csv(DATA_PATH / "response_unclean.csv",     C_RESPONSE),
        "questionnaire": _csv(DATA_PATH / "questionnaire_unclean.csv",C_QUEST),
        "mortality":     _csv(DATA_PATH / "mortality_unclean.csv",    C_MORT),
    }

def run_transformations(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Aplica transformaciones a los dfs y devuelve el DataFrame final."""
    df_demographic   = dfs["demographic"].copy()
    df_dietary       = dfs["dietary"].copy()
    df_response      = dfs["response"].copy()
    df_questionnaire = dfs["questionnaire"].copy()
    df_mortality     = dfs["mortality"].copy()

    # Renombres
    df_demographic = df_demographic.rename(columns={
        "SEQN_new":"id","RIDRETH1":"grupo_etnico","DMDEDUC2":"nivel_educativo","DMDMARTL":"estado_civil",
        "DMDBORN4":"pais_nacimiento","WTMEC2YR":"peso_muestral_examen","WTINT2YR":"peso_muestral_entrevista",
        "SDMVPSU":"unidad_muestral_psu","SDMVSTRA":"estrato_muestral"
    })
    df_response = df_response.rename(columns={
        "SEQN_new":"id","LBXSCR":"creatinina_serica(mg/dL)","URXUMA":"albumina_urinaria(mg/L)",
        "URXUCR":"creatinina_urinaria(g/L)","BMXWT":"peso(kg)","BMXHT":"altura(cm)","BMXBMI":"IMC",
        "BPXSY1":"TAS-1","BPXSY2":"TAS-2","BPXSY3":"TAS-3","BPXSY4":"TAS-4",
        "BPXDI1":"TAD-1","BPXDI2":"TAD-2","BPXDI3":"TAD-3","BPXDI4":"TAD-4"
    })
    df_questionnaire = df_questionnaire.rename(columns={
        "SEQN_new":"id","DIQ010":"DM","BPQ020":"HTA","MCQ160B":"IC","MCQ160C":"EC","MCQ160F":"ACV","MCQ160M":"CA",
        "ALQ101":"consumo_alcohol","PAQ650":"actividad_fisica_moderada","PAQ665":"actividad_fisica_vigorosa"
    })
    df_mortality = df_mortality.rename(columns={"SEQN_new":"id","MORTSTAT":"mortalidad","UCOD_LEADING":"causa_muerte"})
    df_dietary   = df_dietary.rename(columns={"SEQN_new":"id","RIDAGEYR":"edad_años","RIAGENDR":"sexo"})

    # Codificaciones
    df_demographic["estado_civil"] = df_demographic["estado_civil"].replace({6:1, 5:12, 3:2, 4:2}).replace({12:3})
    df_demographic["peso_muestral_examen"] = df_demographic["peso_muestral_examen"].round(1)
    df_demographic["peso_muestral_entrevista"] = df_demographic["peso_muestral_entrevista"].round(1)

    reco = {
        "grupo_etnico": {1:"Mexicano-Americano",2:"Otros hispanos",3:"Blanco no hispano",4:"Negro no hispano",5:"Otra raza, incluida la multirracial"},
        "nivel_educativo": {1:"Menos de 9° grado",2:"9-11º (incluye 12º sin diploma)",3:"Graduado secundaria / GED",4:"Algún título universitario o AA",5:"Graduado universitario o superior",7:"Negado",9:"No sé"},
        "estado_civil": {1:"Casado/Viviendo con pareja",2:"Viudo/Divorciado/Separado",3:"Nunca casado",77:"Negado",99:"No sé"},
        "pais_nacimiento": {1:"EE. UU./Washington",2:"México",3:"Otro país",7:"Negado",9:"No sé"}
    }
    reco_2 = {
        "DM":{1:"Sí",2:"No",3:"Frontera",7:"Negado",9:"No sé"},
        "HTA":{1:"Sí",2:"No",9:"No sé"},
        "IC":{1:"Sí",2:"No",7:"Negado",9:"No sé"},
        "EC":{1:"Sí",2:"No",7:"Negado",9:"No sé"},
        "ACV":{1:"Sí",2:"No",7:"Negado",9:"No sé"},
        "CA":{1:"Sí",2:"No",7:"Negado",9:"No sé"},
        "consumo_alcohol":{1:"Sí",2:"No",9:"No sé"},
        "actividad_fisica_moderada":{1:"Sí",2:"No",7:"Negado",9:"No sé"},
        "actividad_fisica_vigorosa":{1:"Sí",2:"No",7:"Negado",9:"No sé"}
    }
    reco_3 = {
        "mortalidad":{0:"Vivo",1:"Muerto"},
        "causa_muerte":{1:"Enf. corazón",2:"Neoplasmas",3:"Enf. resp. crónicas",4:"Accidentes",5:"Enf. cerebrovasculares",6:"Alzheimer",7:"Diabetes",8:"Influenza/neumonía",9:"Nefritis/síndrome nefrótico/nefrosis",10:"Otras causas"}
    }
    reco_4 = {"sexo":{1:"Hombre",2:"Mujer"}}

    for col,d in reco.items():        df_demographic[col]   = df_demographic[col].map(d)
    for col,d in reco_2.items():      df_questionnaire[col] = df_questionnaire[col].map(d)
    for col,d in reco_3.items():      df_mortality[col]     = df_mortality[col].map(d)
    for col,d in reco_4.items():      df_dietary[col]       = df_dietary[col].map(d)

    # Limpieza de 'id'
    for d in (df_demographic, df_response, df_questionnaire, df_mortality, df_dietary):
        d["id"] = d["id"].astype(str).str.strip()
    df_dietary = df_dietary.drop_duplicates(subset="id", keep="first")

    # Merge
    df_merge = (
        df_demographic
        .merge(df_response,      on="id", how="left", validate="one_to_one")
        .merge(df_questionnaire, on="id", how="left", validate="one_to_one")
        .merge(df_mortality,     on="id", how="left", validate="one_to_one")
        .merge(df_dietary,       on="id", how="left", validate="one_to_one")
    )


    df_merge["creatinina_urinaria(g/L)"] = (df_merge["creatinina_urinaria(g/L)"] * 0.01).round(1)
    df_merge["ACR"] = (df_merge["albumina_urinaria(mg/L)"] / df_merge["creatinina_urinaria(g/L)"]).round(1)

    df_merge["categoria_ACR"] = pd.Series(pd.NA, dtype="string")
    df_merge.loc[df_merge["ACR"] < 30, "categoria_ACR"] = "A1"
    df_merge.loc[(df_merge["ACR"] >= 30) & (df_merge["ACR"] <= 300), "categoria_ACR"] = "A2"
    df_merge.loc[df_merge["ACR"] > 300, "categoria_ACR"] = "A3"

    def calc_egfr(creat, age, gender):
        if gender == "Mujer":  kappa, alpha, factor = 0.7, -0.241, 1.012
        elif gender == "Hombre": kappa, alpha, factor = 0.9, -0.302, 1.0
        else: return np.nan
        rate = creat / kappa
        return 142 * (min(rate,1)**alpha) * (max(rate,1)**-1.200) * (0.9938**age) * factor

    df_merge["eGFR"] = (
        df_merge.apply(lambda x: calc_egfr(x["creatinina_serica(mg/dL)"], x["edad_años"], x["sexo"]), axis=1)
        .round(1)
    )

    df_merge["categoria_eGFR"] = pd.Series(pd.NA, dtype="string")
    df_merge.loc[df_merge["eGFR"] >= 90, "categoria_eGFR"] = "G1"
    df_merge.loc[(df_merge["eGFR"] >= 60) & (df_merge["eGFR"] < 90),  "categoria_eGFR"] = "G2"
    df_merge.loc[(df_merge["eGFR"] >= 45) & (df_merge["eGFR"] < 60),  "categoria_eGFR"] = "G3a"
    df_merge.loc[(df_merge["eGFR"] >= 30) & (df_merge["eGFR"] < 45),  "categoria_eGFR"] = "G3b"
    df_merge.loc[(df_merge["eGFR"] >= 15) & (df_merge["eGFR"] < 30),  "categoria_eGFR"] = "G4"
    df_merge.loc[df_merge["eGFR"] < 15, "categoria_eGFR"] = "G5"

    columns_merge = [
        "id","edad_años","sexo","grupo_etnico","unidad_muestral_psu","estrato_muestral",
        "peso_muestral_entrevista","peso_muestral_examen","DM","peso(kg)","altura(cm)","IMC",
        "creatinina_urinaria(g/L)","albumina_urinaria(mg/L)","ACR","categoria_ACR","mortalidad",
        "TAS-1","TAD-1","TAS-2","TAD-2","TAS-3","TAD-3","HTA","estado_civil","eGFR","categoria_eGFR",
        "creatinina_serica(mg/dL)","IC","EC","ACV","nivel_educativo","CA","pais_nacimiento",
        "actividad_fisica_moderada","actividad_fisica_vigorosa","consumo_alcohol","causa_muerte",
        "TAS-4","TAD-4"
    ]

    df_merge = df_merge[~df_merge["id"].astype(str).str.startswith("I-")]
    cols_existentes = [c for c in columns_merge if c in df_merge.columns]
    df_merge = df_merge[cols_existentes]

    float_columns = df_merge.select_dtypes(include=["number"]).columns
    str_columns   = df_merge.select_dtypes(include=["object","string"]).columns

    for col in str_columns:
        s = df_merge[col].astype("string").str.strip().replace({"": pd.NA})
        if col in ("mortalidad","estado_civil","sexo","pais_nacimiento"):
            df_merge[col] = s.fillna("No definido")
        elif col == "grupo_etnico":
            df_merge[col] = s.fillna("Sin etnia")
        elif col in ("DM","HTA","IC","EC","ACV","CA"):
            df_merge[col] = s.fillna("Sin diagnostico")
        elif col == "causa_muerte":
            df_merge[col] = s.fillna("Sin causa")
        elif col == "categoria_ACR":
            df_merge[col] = s.fillna("A1(por media)")
        elif col == "categoria_eGFR":
            df_merge[col] = s.fillna("G1(por media)")
        else:
            df_merge[col] = s.fillna("Sin categoria")

    median_ = df_merge[float_columns].median().round(1)
    for col in float_columns:
        df_merge[col] = df_merge[col].fillna(median_[col])

    return df_merge

# API helpers
def build_merged_dataframe() -> pd.DataFrame:
    """Orquesta lectura + transformaciones."""
    return run_transformations(read_inputs())

def save_to_sqlite(df: pd.DataFrame, table: str | None = None, if_exists: str = "replace"):
    """Guarda en SQLite dentro de OUTPUT_DIR (db/etl.db por defecto)."""
    import sqlite3
    SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(SQLITE_PATH)) as conn:
        df.to_sql(table or SQLITE_TABLE, conn, if_exists=if_exists, index=False)

def save_to_file(df: pd.DataFrame, filename: str, fmt: str = "csv") -> pathlib.Path:
    """Guarda el DF en OUTPUT_DIR con el nombre/format indicados y devuelve la ruta resultante."""
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    if fmt == "csv":
        out = OUTPUT_PATH / (filename if filename.endswith(".csv") else f"{filename}.csv")
        df.to_csv(out, index=False)
        return out
    elif fmt == "xlsx":
        out = OUTPUT_PATH / (filename if filename.endswith(".xlsx") else f"{filename}.xlsx")
        with pd.ExcelWriter(out, engine="xlsxwriter") as w:
            df.to_excel(w, sheet_name="DataFrame", index=False)
        return out
    else:
        raise ValueError("Formato no soportado (usa 'csv' o 'xlsx').")
