import streamlit as st
import pandas as pd
import xml.etree.ElementTree as ET
import zipfile
import io
import re
from sqlalchemy import create_engine, text
from datetime import datetime

# ==========================================
# 1. CONFIGURACIÓN Y ESTÉTICA (CSS)
# ==========================================
st.set_page_config(page_title="Suite Aduanera Pro", page_icon="📦", layout="wide")

# Inyección de CSS para calcar el diseño HTML original
st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
        
        html, body, [class*="st-"] { font-family: 'Inter', sans-serif; }
        .stApp { background-color: #f8fafc; }
        
        /* Ocultar elementos de Streamlit */
        #MainMenu, footer, header { visibility: hidden; }

        /* Contenedor Principal Estilo HTML */
        .main-container {
            background: white;
            padding: 40px;
            border-radius: 16px;
            box-shadow: 0 10px 25px -3px rgba(0, 0, 0, 0.05);
            margin-top: -50px;
        }

        /* Stepper (Indicador de pasos) */
        .stepper-wrapper {
            display: flex;
            justify-content: center;
            align-items: center;
            gap: 15px;
            margin-bottom: 40px;
        }
        .step-item {
            display: flex;
            align-items: center;
            gap: 10px;
            font-weight: 600;
            color: #94a3b8;
        }
        .step-active { color: #4f46e5; }
        .step-completed { color: #10b981; }
        .step-circle {
            width: 30px; height: 30px; border-radius: 50%;
            background: #e2e8f0; display: flex;
            align-items: center; justify-content: center; font-size: 14px;
        }
        .step-active .step-circle { background: #4f46e5; color: white; box-shadow: 0 0 0 4px rgba(79, 70, 229, 0.2); }
        .step-completed .step-circle { background: #10b981; color: white; }
        .step-line { width: 40px; height: 3px; background: #e2e8f0; }
        .line-completed { background: #10b981; }

        /* Tablas */
        .row-error { background-color: #fef2f2 !important; color: #b91c1c !important; font-weight: bold; }
        
        /* Info Boxes */
        .master-badge {
            background: #eff6ff; border: 1px solid #bfdbfe;
            color: #1e3a8a; padding: 15px; border-radius: 8px;
            margin-bottom: 20px; font-weight: 600;
        }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 2. ESTADO GLOBAL (SESSION STATE)
# ==========================================
if 'step' not in st.session_state: st.session_state.step = 1
if 'df_final' not in st.session_state: st.session_state.df_final = None
if 'master' not in st.session_state: st.session_state.master = "Desconocido"
if 'excel_bytes' not in st.session_state: st.session_state.excel_bytes = None
if 'zip_bytes' not in st.session_state: st.session_state.zip_bytes = None

# Conexión a TiDB Cloud
try:
    engine = create_engine(st.secrets["TIDB_URI"])
except:
    st.error("🚨 Error: No se encontró la configuración de TiDB en Secrets.")

# ==========================================
# 3. FUNCIONES DE LÓGICA
# ==========================================
def parse_monto(texto):
    if not texto: return 0.0
    try: return float(str(texto).replace(',', '').strip())
    except: return 0.0

def procesar_archivos(xmls, excel):
    # Procesar XMLs
    xml_data = []
    for x in xmls:
        tree = ET.parse(x)
        root = tree.getroot()
        for item in root.findall('.//Item'):
            guia = (item.findtext('.//Summary_declaration') or "N/A").strip()
            fob = parse_monto(item.findtext('.//Item_Invoice_Amount_national_currency'))
            freight = parse_monto(item.findtext('.//item_external_freight_Amount_national_currency'))
            insurance = parse_monto(item.findtext('.//item_insurance_Amount_national_currency'))
            cif = parse_monto(item.findtext('.//Total_CIF_itm'))
            dai = sum([parse_monto(t.findtext('.//Duty_tax_amount')) for t in item.findall('.//Taxation_line') if t.findtext('.//Duty_tax_code') == 'DAI'])
            iva = sum([parse_monto(t.findtext('.//Duty_tax_amount')) for t in item.findall('.//Taxation_line') if t.findtext('.//Duty_tax_code') == 'IVA'])
            xml_data.append({"guia": guia, "fob": fob, "freight": freight, "insurance": insurance, "cif": cif, "dai": dai, "iva": iva})
    
    df_xml = pd.DataFrame(xml_data)
    
    # Procesar StarShip Excel
    df_star = pd.read_excel(excel, skiprows=2)
    # Seleccionamos columnas por índice según tu lógica JS: AWB (1), Tracking (2), PkgZ (25)
    df_star = df_star.iloc[:, [1, 2, 25]]
    df_star.columns = ['awb', 'tracking', 'pkgZ']
    df_star['pkgZ'] = df_star['pkgZ'].astype(str).str.strip()
    
    # Cruce (Merge)
    df_final = pd.merge(df_xml, df_star, left_on='guia', right_on='pkgZ', how='left')
    df_final['awb'] = df_final['awb'].fillna('N/A')
    df_final['tracking'] = df_final['tracking'].fillna('N/A')
    
    # Cálculos de Auditoría
    df_final['suma_calc'] = (df_final['fob'] + df_final['freight'] + df_final['insurance']).round(2)
    df_final['diferencia'] = (df_final['cif'] - df_final['suma_calc']).round(2)
    
    return df_final

# ==========================================
# 4. INTERFAZ (UI)
# ==========================================

# Header
col_h1, col_h2 = st.columns([3, 1])
with col_h1:
    st.title("Suite Aduanera Pro")
    st.markdown("<p style='color:#64748b; margin-top:-20px;'>Flujo consolidado: Extracción, Auditoría y Renombrado</p>", unsafe_allow_html=True)
with col_h2:
    if st.button("🕒 Ver Historial"): st.session_state.step = 5

# STEPPER VISUAL (IGUAL AL HTML)
s = st.session_state.step
st.markdown(f"""
    <div class="stepper-wrapper">
        <div class="step-item {'step-active' if s==1 else 'step-completed' if s>1 else ''}"><div class="step-circle">1</div> Datos Base</div>
        <div class="step-line {'line-completed' if s>1 else ''}"></div>
        <div class="step-item {'step-active' if s==2 else 'step-completed' if s>2 else ''}"><div class="step-circle">2</div> Auditoría</div>
        <div class="step-line {'line-completed' if s>2 else ''}"></div>
        <div class="step-item {'step-active' if s==3 else 'step-completed' if s>3 else ''}"><div class="step-circle">3</div> PDFs DUCAs</div>
        <div class="step-line {'line-completed' if s>3 else ''}"></div>
        <div class="step-item {'step-active' if s==4 else 'step-completed' if s>4 else ''}"><div class="step-circle">✓</div> Fin</div>
    </div>
""", unsafe_allow_html=True)

# --- PASO 1: CARGA ---
if s == 1:
    col1, col2 = st.columns(2)
    xmls = col1.file_uploader("📄 Arrastra los XML (DUCA)", accept_multiple_files=True, type=['xml'])
    star = col2.file_uploader("📊 Arrastra StarShip.xlsx", type=['xlsx'])
    
    if xmls and star:
        if st.button("Siguiente: Auditar Datos ➡️", type="primary", use_container_width=True):
            df = procesar_archivos(xmls, star)
            st.session_state.df_final = df
            st.session_state.master = str(df['awb'].iloc[0]) if not df.empty else "Desconocido"
            st.session_state.step = 2
            st.rerun()

# --- PASO 2: AUDITORÍA ---
elif s == 2:
    df = st.session_state.df_final
    col_t1, col_t2 = st.columns([2, 1])
    with col_t1:
        if st.button("🔧 Corregir Automáticamente", type="secondary"):
            df['freight'] = (df['cif'] - df['fob'] - df['insurance']).round(2)
            df['suma_calc'] = (df['fob'] + df['freight'] + df['insurance']).round(2)
            df['diferencia'] = (df['cif'] - df['suma_calc']).round(2)
            st.session_state.df_final = df
            st.rerun()
    
    # Pintar errores en rojo (Estética HTML)
    def style_row(row):
        return ['background-color: #fef2f2; color: #b91c1c' if row.diferencia != 0 else '' for _ in row]

    st.dataframe(df.style.apply(style_row, axis=1), use_container_width=True)
    
    if st.button("📥 Descargar Excel y Continuar ➡️", type="primary", use_container_width=True):
        # Generar Excel en memoria (Mismo formato que el Template)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # Recreamos las cabeceras exactas del CSV template cargado
            headers = ["Platform Package Number (required)", "Country (required)", "Province of customs clearance (optional)", "Province of receipt (optional)", "Currency (required)", "Value For Duty (required)", "Total payable amount (required)", "Total Duty (required)", "Total Excise Tax (required)", "Total GST (required)", "Total SIMA (required)", "Total Other Tax (required)", "Currency（Service Fee)", "Service Fee (required)", "Logistics number(required)", "AWB Number(optional)", "FOB Price (required)", "Freight (required)", "Insurance(required)"]
            
            df_exp = pd.DataFrame(columns=headers)
            df_exp["Platform Package Number (required)"] = df["tracking"]
            df_exp["Country (required)"] = "El Salvador"
            df_exp["Currency (required)"] = "USD"
            df_exp["Value For Duty (required)"] = df["cif"]
            df_exp["Total payable amount (required)"] = df["dai"] + df["iva"]
            df_exp["Total Duty (required)"] = df["dai"]
            df_exp["Total GST (required)"] = df["iva"]
            df_exp["Logistics number(required)"] = df["guia"]
            df_exp["AWB Number(optional)"] = df["awb"]
            df_exp["FOB Price (required)"] = df["fob"]
            df_exp["Freight (required)"] = df["freight"]
            df_exp["Insurance(required)"] = df["insurance"]
            df_exp.fillna(0, inplace=True)
            
            df_exp.to_excel(writer, index=False, sheet_name='Template')
        
        st.session_state.excel_bytes = output.getvalue()
        st.session_state.step = 3
        st.rerun()

# --- PASO 3: PDFS Y RENOMBRADO ---
elif s == 3:
    st.markdown(f'<div class="master-badge">ℹ️ Asignando al Máster: {st.session_state.master}</div>', unsafe_allow_html=True)
    pdfs = st.file_uploader("📂 Arrastra los PDFs (DUCAs) aquí", accept_multiple_files=True, type=['pdf'])
    
    if pdfs:
        if st.button("📦 Descargar ZIP y Finalizar ✓", type="primary", use_container_width=True):
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w") as zf:
                for p in pdfs:
                    num = "".join(re.findall(r'\d+', p.name))
                    nuevo_nombre = f"Duca_{num}.pdf" if num else p.name
                    zf.writestr(nuevo_nombre, p.read())
            
            st.session_state.zip_bytes = zip_buffer.getvalue()
            
            # GUARDAR EN TIDB (Persistencia real)
            try:
                with engine.connect() as conn:
                    query = text("INSERT INTO historial_trabajos (usuario, master_awb, excel_file, zip_file) VALUES (:u, :m, :e, :z)")
                    conn.execute(query, {"u": "Operador", "m": st.session_state.master, "e": st.session_state.excel_bytes, "z": st.session_state.zip_bytes})
                    conn.commit()
            except Exception as e:
                st.error(f"Error al guardar en TiDB: {e}")
                
            st.session_state.step = 4
            st.rerun()

# --- PASO 4: FIN ---
elif s == 4:
    st.balloons()
    st.markdown(f"""
        <div style="text-align: center; padding: 40px; background: #ecfdf5; border: 2px dashed #34d399; border-radius: 16px;">
            <h1 style="color: #047857;">🎉 ¡Proceso Completado!</h1>
            <p>El Máster <b>{st.session_state.master}</b> ha sido guardado en TiDB Cloud.</p>
        </div>
    """, unsafe_allow_html=True)
    
    c1, c2 = st.columns(2)
    c1.download_button("📥 Descargar Excel Final", data=st.session_state.excel_bytes, file_name=f"{st.session_state.master}.xlsx")
    c2.download_button("📦 Descargar ZIP DUCAs", data=st.session_state.zip_bytes, file_name=f"{st.session_state.master}_DUCAs.zip")
    
    if st.button("+ Iniciar Nuevo Proceso", use_container_width=True):
        st.session_state.step = 1
        st.rerun()

# --- PASO 5: HISTORIAL (DB) ---
elif s == 5:
    st.subheader("🕒 Historial Global (TiDB Cloud)")
    if st.button("⬅️ Volver"): st.session_state.step = 1; st.rerun()
    
    df_h = pd.read_sql("SELECT id, master_awb, fecha FROM historial_trabajos ORDER BY fecha DESC", engine)
    st.table(df_h)
