import streamlit as st
import pandas as pd
import xml.etree.ElementTree as ET
import zipfile
import io
import re
from sqlalchemy import create_engine, text

# 1. Configuración de la App y BD
st.set_page_config(page_title="Suite Aduanera Pro", layout="wide")

# Conexión a tu BD 'test' en TiDB (se leerá de los Secrets de Streamlit)
DB_URI = st.secrets.get("TIDB_URI", "sqlite:///local.db")
engine = create_engine(DB_URI)

def parse_monto(texto):
    if not texto: return 0.0
    try: return float(str(texto).replace(',', '').strip())
    except ValueError: return 0.0

# 2. Interfaz Principal
st.title("🏛️ Suite de Consolidación Aduanera")
usuario = st.sidebar.text_input("👤 Usuario Operador:", "Operador_1")

tab1, tab2, tab3 = st.tabs(["1️⃣ Carga y Auditoría", "2️⃣ Renombrado PDFs y Nube", "🕒 Historial Global"])

with tab1:
    st.header("1. Cruce de Datos (XML + Excel)")
    col1, col2 = st.columns(2)
    xml_files = col1.file_uploader("XMLs (DUCAs)", type=['xml'], accept_multiple_files=True)
    excel_file = col2.file_uploader("StarShip.xlsx", type=['xlsx', 'xls'])

    if xml_files and excel_file and st.button("Procesar y Auditar"):
        # Procesar XMLs
        datos = []
        for file in xml_files:
            root = ET.parse(file).getroot()
            for item in root.findall('.//Item'):
                guia = item.findtext('.//Summary_declaration', default="N/A").strip()
                fob = parse_monto(item.findtext('.//Item_Invoice_Amount_national_currency'))
                freight = parse_monto(item.findtext('.//item_external_freight_Amount_national_currency'))
                insurance = parse_monto(item.findtext('.//item_insurance_Amount_national_currency'))
                cif = parse_monto(item.findtext('.//Total_CIF_itm'))
                
                dai, iva = 0.0, 0.0
                for tax in item.findall('.//Taxation_line'):
                    code = tax.findtext('.//Duty_tax_code')
                    amt = parse_monto(tax.findtext('.//Duty_tax_amount'))
                    if code == 'DAI': dai = amt
                    if code == 'IVA': iva = amt
                datos.append({"Guia": guia, "FOB": fob, "Freight": freight, "Insurance": insurance, "CIF": cif, "DAI": dai, "IVA": iva})
        
        df_xml = pd.DataFrame(datos)
        
        # Procesar StarShip
        df_star = pd.read_excel(excel_file, skiprows=2, header=None)
        df_star = df_star[[1, 2, 25]].rename(columns={1: 'AWB', 2: 'Tracking', 25: 'PackageZ'})
        df_star['PackageZ'] = df_star['PackageZ'].astype(str).str.strip()
        
        # Cruce
        df_final = pd.merge(df_xml, df_star, left_on='Guia', right_on='PackageZ', how='left')
        df_final['AWB'] = df_final['AWB'].fillna('N/A')
        df_final['Tracking'] = df_final['Tracking'].fillna('N/A')
        
        # Autocorrección de Fletes
        df_final['Suma_Calc'] = df_final['FOB'] + df_final['Freight'] + df_final['Insurance']
        df_final['Diferencia'] = (df_final['CIF'] - df_final['Suma_Calc']).round(2)
        errores = len(df_final[df_final['Diferencia'] != 0.00])
        
        if errores > 0:
            df_final.loc[df_final['Diferencia'] != 0.00, 'Freight'] = df_final['CIF'] - df_final['FOB'] - df_final['Insurance']
            st.warning(f"🔧 Se corrigieron automáticamente {errores} fletes para cuadrar con el Value For Duty.")
            
        st.session_state['df_final'] = df_final
        st.session_state['master'] = str(df_final['AWB'].iloc[0]) if not df_final.empty else "Desconocido"
        st.success(f"Cruce completado. Máster asignado: **{st.session_state['master']}**. Ve a la pestaña 2.")
        st.dataframe(df_final[['Guia', 'AWB', 'Tracking', 'FOB', 'Freight', 'Insurance', 'CIF']])

with tab2:
    st.header("2. Renombrado y Guardado en Nube")
    if 'df_final' in st.session_state:
        st.info(f"Trabajando con el Máster: **{st.session_state['master']}**")
        pdf_files = st.file_uploader("Cargar PDFs (DUCAs)", type=['pdf'], accept_multiple_files=True)
        
        if pdf_files and st.button("Finalizar, Generar Archivos y Guardar en BD"):
            master = st.session_state['master']
            df = st.session_state['df_final']
            
            # 1. Crear el Excel de SV
            excel_buffer = io.BytesIO()
            
            # Mapeo exacto de columnas para El Salvador
            df_export = pd.DataFrame()
            df_export['Platform Package Number (required)'] = df['Tracking'].replace('N/A', '')
            df_export['Country (required)'] = 'El Salvador'
            df_export['Province of customs clearance (optional)'] = ''
            df_export['Province of receipt (optional)'] = ''
            df_export['Currency (required)'] = 'USD'
            df_export['Value For Duty (required)'] = df['CIF']
            df_export['Total payable amount (required)'] = df['DAI'] + df['IVA']
            df_export['Total Duty (required)'] = df['DAI']
            df_export['Total Excise Tax (required)'] = 0
            df_export['Total GST (required)'] = df['IVA']
            df_export['Total SIMA (required)'] = 0
            df_export['Total Other Tax (required)'] = 0
            df_export['Currency（Service Fee)'] = 'USD'
            df_export['Service Fee (required)'] = 0
            df_export['Logistics number(required)'] = df['Guia']
            df_export['AWB Number(optional)'] = df['AWB'].replace('N/A', '')
            df_export['FOB Price (required)'] = df['FOB']
            df_export['Freight (required)'] = df['Freight']
            df_export['Insurance(required)'] = df['Insurance']
            
            df_export.to_excel(excel_buffer, index=False, sheet_name="Template")
            excel_bytes = excel_buffer.getvalue()
            
            # 2. Generar el ZIP renombrado
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w") as zf:
                for pdf in pdf_files:
                    num = re.sub(r'\D', '', pdf.name)
                    zf.writestr(f"Duca_{num}.pdf" if num else f"Error_{pdf.name}", pdf.read())
            zip_bytes = zip_buffer.getvalue()
            
            # 3. Guardar en TiDB
            try:
                with engine.connect() as conn:
                    query = text("INSERT INTO historial_trabajos (usuario, master_awb, excel_file, zip_file) VALUES (:u, :m, :e, :z)")
                    conn.execute(query, {"u": usuario, "m": master, "e": excel_bytes, "z": zip_bytes})
                    conn.commit()
                st.success("¡Datos guardados permanentemente en la nube!")
                
                # Botones de descarga
                colA, colB = st.columns(2)
                colA.download_button("📥 Bajar Excel Final", data=excel_bytes, file_name=f"{master}.xlsx")
                colB.download_button("📦 Bajar ZIP Renombrado", data=zip_bytes, file_name=f"{master}_Ducas.zip")
            except Exception as e:
                st.error(f"Error guardando en la BD: {e}")
    else:
        st.warning("Completa el paso 1 primero.")

with tab3:
    st.header("3. Historial Global")
    if st.button("Actualizar Historial"):
        try:
            df_hist = pd.read_sql("SELECT id, usuario, master_awb, fecha FROM historial_trabajos ORDER BY fecha DESC", engine)
            st.dataframe(df_hist, use_container_width=True)
            
            id_bajar = st.number_input("Ingresa el ID del trabajo para descargar sus archivos:", min_value=0, step=1)
            if id_bajar > 0:
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT master_awb, excel_file, zip_file FROM historial_trabajos WHERE id = :id"), {"id": id_bajar}).fetchone()
                    if result:
                        st.download_button("📥 Excel", data=result[1], file_name=f"{result[0]}.xlsx")
                        st.download_button("📦 ZIP", data=result[2], file_name=f"{result[0]}_Ducas.zip")
        except Exception as e:
            st.error("No hay conexión a la BD o la tabla está vacía.")
