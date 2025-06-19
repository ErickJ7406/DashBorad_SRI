# main.py

import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import json

def run_etl():
    """
    Función principal que ejecuta todo el proceso ETL.
    """
    print("--- INICIANDO PROCESO ETL DEL SRI ---")

    # --- 1. AUTENTICACIÓN CON GOOGLE SERVICE ACCOUNT ---
    # Kestra pasará el contenido del JSON como una variable de entorno.
    gcp_credentials_json_str = os.getenv('GCP_CREDENTIALS')
    if not gcp_credentials_json_str:
        raise ValueError("La variable de entorno GCP_CREDENTIALS no está definida.")

    gcp_credentials_dict = json.loads(gcp_credentials_json_str)
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(gcp_credentials_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    print("Autenticación con Google exitosa.")

    # --- 2. EXTRACCIÓN (E) ---
    print("\n--- [FASE DE EXTRACCIÓN] ---")
    url_sri = "https://www.sri.gob.ec/datasets"
    seccion_texto = "Contribuyentes autorizados de oficio comprobantes electrónicos"
    response = requests.get(url_sri, timeout=45)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')
    strong_tag = soup.find('strong', string=lambda t: t and seccion_texto in t.strip())
    container_div = strong_tag.find_parent('h3').find_next_sibling('div')
    links_csv = container_div.find_all('a', href=lambda h: h and h.endswith('.csv'))

    if not links_csv:
        print("No se encontraron CSVs. Finalizando.")
        return

    # --- 3. TRANSFORMACIÓN (T) ---
    print("\n--- [FASE DE TRANSFORMACIÓN] ---")
    lista_df = []
    for link in links_csv:
        url_archivo = link['href']
        print(f"Procesando: {url_archivo.split('/')[-1]}")
        df_temp = pd.read_csv(url_archivo, sep=';', encoding='latin-1', low_memory=False)
        lista_df.append(df_temp)

    df_completo = pd.concat(lista_df, ignore_index=True)
    print(f"DataFrame unificado con {df_completo.shape[0]} filas.")

    # Limpieza
    df_completo.columns = df_completo.columns.str.strip().str.lower().str.replace(' ', '_')
    df_completo.drop_duplicates(inplace=True)
    print("Datos limpiados.")

    # --- 4. CARGA (L) ---
    print("\n--- [FASE DE CARGA] ---")
    # Cargar a un Google Sheet en lugar de un archivo en Drive para mayor facilidad.
    sheet_name = "SRI_Contribuyentes_Autorizados"
    try:
        spreadsheet = gc.open(sheet_name)
        print(f"Abriendo Google Sheet existente: '{sheet_name}'")
    except gspread.exceptions.SpreadsheetNotFound:
        spreadsheet = gc.create(sheet_name)
        # Comparte la hoja con tu cuenta de Google personal para poder verla
        spreadsheet.share('tu-email-personal@gmail.com', perm_type='user', role='writer')
        print(f"Creando nuevo Google Sheet: '{sheet_name}'")

    worksheet = spreadsheet.sheet1
    worksheet.clear()
    set_with_dataframe(worksheet, df_completo)
    print(f"Datos cargados exitosamente en Google Sheet: {spreadsheet.url}")

    print("\n--- PROCESO ETL FINALIZADO CON ÉXITO ---")


if __name__ == "__main__":
    run_etl()