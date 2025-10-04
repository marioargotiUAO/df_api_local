Nota: Debido a las restricciones de GitHub (límite de 100 MB por archivo), se eliminaron los archivos CSV del repositorio, ya que cada uno superaba los 300 MB.

#  API ETL 

Este proyecto expone una API construida con **FastAPI** para:
- Leer los archivos CSV en la carpeta `data/`
- Ejecutar un **ETL** (extracción, transformación y carga) uniendo los CSV
- Descargar el resultado en formato **CSV** o **Excel**
- (Opcional) Guardar el resultado en una base de datos SQLite (`db/etl.db`)

---

##  Cómo usar este proyecto

### 1. Descargar el proyecto
- Abre el enlace de Google Drive compartido
- Descarga la carpeta completa `df_api_local/`
- Descomprime si baja como `.zip`

---

### 2. Requisitos previos
- **Python 3.10 o superior**
- **pip** (se instala junto con Python)

Para verificar instalación:
```bash
python --version
pip --version
```

---

### 3. Abrir el proyecto
En **Windows (PowerShell)**:
```powershell
cd C:\Users\TU_USUARIO\Downloads\df_api_local
```

En **Linux/Mac**:
```bash
cd ~/Downloads/df_api_local
```

---

### 4. Crear entorno virtual
Windows (PowerShell):
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Linux/Mac:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

---

### 5. Instalar dependencias
```bash
pip install -r requirements.txt
```

---

### 6. Configurar variables de entorno
Copiar el archivo de ejemplo:
- Windows:
  ```powershell
  Copy-Item .env.example .env
  ```
- Linux/Mac:
  ```bash
  cp .env.example .env
  ```


---

### 7. Ejecutar la API
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

API corriendo en:
[http://localhost:8000](http://localhost:8000)  
Documentación interactiva: [http://localhost:8000/docs](http://localhost:8000/docs)

En este caso tenemos la Api subida a un servidor http://srv1037997.hstgr.cloud/docs#/ETL/etl_download_etl_download_get
el cual puede ver que archivos estamos evaluando, descargar por paginado o una extraccion en csv o xlsx.

---

### 8. Probar la API

#### Navegador
- Abre [http://localhost:8000/docs](http://localhost:8000/docs)
- Pulsa **Authorize**
- Escribe la clave definida en el `.env` (`API_KEY=etl-12345` por defecto)
- Prueba los endpoints

#### Ejemplo con curl
- Listar datasets disponibles:
  ```bash
  curl -H "X-API-Key: etl-12345" http://localhost:8000/datasets
  ```

- Descargar resultado del ETL en CSV:
  ```bash
  curl -H "X-API-Key: etl-12345" "http://localhost:8000/etl/download?format=csv" -o df_etl.csv
  ```

---

## Carpetas importantes

- `data/` → aquí deben estar los CSV originales (de entrada).
- `db/` → aquí se guardarán los resultados (CSV procesados, Excel o SQLite).
- `app/` → contiene el código de la API (`main.py` y `etl.py`).

---

## Variables de entorno (.env)

El archivo `.env` controla las configuraciones principales:

```env
API_KEY=etl-12345          
MAX_JSON_ROWS=100000       
DATA_DIR=data              
DB_DIR=db                  
DEFAULT_DOWNLOAD_NAME=df_etl.csv
SQLITE_PATH=db/etl.db
SQLITE_TABLE=nhanes_etl
```

---

## Problemas comunes

- **401 Unauthorized** → No pusiste el header `X-API-Key`.  
- **404 Dataset no encontrado** → El nombre del CSV en `data/` no coincide.  
- **500 Internal Server Error** → Puede faltar un CSV o haber un error en los datos.  
- **Puerto ocupado** → Usa otro puerto:
  ```bash
  uvicorn app.main:app --reload --port 8080
  ```

---

## Tecnologías usadas
- **FastAPI** + **Uvicorn**
- **pandas**, **numpy**
- **python-dotenv**
- **xlsxwriter**
--------------------------------
## Para hacer uso de cargaMysql - cargaMySql.py

- Se debe configurar los puertos a donde va dirigido 
    host="72.60.167.78",      
    user="mario",    
    password="clave123",
    database="ercClean"  
en este caso se tiene configurado el servidor para que suba la base de datos final y pueda poblar la tabla datos_ercclean

-- Lo que hace, es hacer un insert leyendo todo el archivo final que obtuvimos en la api http://srv1037997.hstgr.cloud/docs#/ETL/etl_download_etl_download_get
  el cual se conecta a un servidor donde esta almacenada la bd en MySql y hace la poblacion de la data limpia. 
  


