import pandas as pd
import mysql.connector

df = pd.read_excel("datosCsv/df_final.xlsx")

conexion = mysql.connector.connect(
    host="72.60.167.78",      
    user="mario",    
    password="clave123",
    database="ercClean"     
)

cursor = conexion.cursor()
sql = """
INSERT INTO datos_ercclean (
    id, edad_años, sexo, grupo_etnico, unidad_muestral_psu, estrato_muestral,
    peso_muestral_entrevista, peso_muestral_examen, DM, peso_kg, altura_cm, IMC,
    creatinina_urinaria, albumina_urinaria, ACR, categoria_ACR, mortalidad,
    TAS1, TAD1, TAS2, TAD2, TAS3, TAD3, HTA, estado_civil, eGFR, categoria_eGFR,
    creatinina_serica, IC, EC, ACV, nivel_educativo, CA, pais_nacimiento,
    actividad_fisica_moderada, actividad_fisica_vigorosa, consumo_alcohol, causa_muerte,
    TAS4, TAD4
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
          %s, %s, %s, %s, %s, %s, %s, %s)
"""


for _, fila in df.iterrows():
    valores = tuple(fila[col] for col in df.columns)
    cursor.execute(sql, valores)

    conexion.commit()