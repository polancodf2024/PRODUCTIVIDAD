import streamlit as st
import pandas as pd
import csv
from pathlib import Path
from datetime import datetime
import paramiko
import time
import os
import logging

# Configuración de logging
logging.basicConfig(
    filename='manual.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ====================
# CONFIGURACIÓN INICIAL
# ====================
class Config:
    def __init__(self):
        # Configuración de la aplicación
        self.APP_TITLE = "📝 Captura Manual de Productos"
        self.APP_ICON = "📝"
        self.LOGO_IMAGE = "escudo_COLOR.jpg"
        self.LOGO_WIDTH = 80
        self.CSV_FILENAME = st.secrets.get("remote_manual")
        self.MAX_KEYWORDS = 3
        self.HIGHLIGHT_COLOR = "#90EE90"
        self.REQUIRED_ECONOMIC_NUMBER = True
        
        # Configuración remota desde secrets
        try:
            self.REMOTE = {
                'HOST': st.secrets["remote_host"],
                'USER': st.secrets["remote_user"],
                'PASSWORD': st.secrets["remote_password"],
                'PORT': int(st.secrets.get("remote_port")),
                'DIR': st.secrets["remote_dir"]
            }
            self.REMOTE_MANUAL_FILE = st.secrets.get("remote_manual")
            self.TIMEOUT_SECONDS = 30
        except Exception as e:
            logging.error(f"Error cargando configuración remota: {str(e)}")
            self.REMOTE = None

CONFIG = Config()

# Categorías de palabras clave
KEYWORD_CATEGORIES = {
    "Accidente Cerebrovascular": ["accidente cerebrovascular", "acv", "ictus", "stroke"],
    "Alzheimer": ["alzheimer", "demencia", "enfermedad neurodegenerativa"],
    "Arritmias": [
        "arritmia", "fibrilación auricular", "fa", "flutter auricular", 
        "taquicardia ventricular", "tv", "fibrilación ventricular", "fv",
        "bradicardia", "bloqueo auriculoventricular", "síndrome de brugada", 
        "síndrome de qt largo", "marcapasos", "desfibrilador automático"
    ],
    "Bioinformática": ["bioinformática", "genómica computacional", "análisis de secuencias", "biología de sistemas"],
    "Bioquímica": ["bioquímica", "metabolismo", "enzimas", "rutas metabólicas"],
    "Biología Molecular": ["adn", "arn", "transcripción", "replicación"],
    "Biomarcadores Cardíacos": [
        "troponina", "nt-probnp", "bnp", "ck-mb", "lactato deshidrogenasa", 
        "mioglobina", "péptidos natriuréticos"
    ],
    "Biotecnología": ["biotecnología", "terapia génica", "crispr", "organismos modificados genéticamente"],
    "Cáncer de Mama": ["cáncer de mama", "tumor mamario", "neoplasia mamaria"],
    "Cardiología Pediátrica": [
        "cardiopatía congénita", "comunicación interauricular", "cia", 
        "comunicación interventricular", "civ", "tetralogía de fallot", 
        "transposición grandes vasos", "ductus arterioso persistente"
    ],
    "Cardiomiopatías": [
        "cardiomiopatía", "miocardiopatía", "cardiomiopatía hipertrófica", "hcm", 
        "cardiomiopatía dilatada", "dcm", "cardiomiopatía restrictiva", 
        "displasia arritmogénica", "miocardiopatía no compactada", "amiloidosis cardíaca"
    ],
    "Endocrinología": ["diabetes", "tiroides", "hormonas", "metabolismo"],
    "Enfermedad Vascular Periférica": [
        "enfermedad arterial periférica", "eap", "claudicación intermitente", 
        "índice tobillo-brazo", "isquemia crítica", "arteriopatía obliterante"
    ],
    "Epidemiología": ["epidemiología", "estudios poblacionales", "incidencia", "prevalencia"],
    "Epilepsia": ["epilepsia", "crisis epiléptica", "convulsiones"],
    "Farmacología": ["farmacología", "fármacos", "dosis-respuesta", "toxicidad"],
    "Gastroenterología": ["colon", "hígado", "páncreas", "enfermedad inflamatoria intestinal"],
    "Genética": ["genética", "mutaciones", "genoma humano", "síndromes genéticos"],
    "Hipertensión y Riesgo Cardiovascular": [
        "hipertensión arterial", "hta", "hipertensión pulmonar", 
        "crisis hipertensiva", "mapa", "monitorización ambulatoria", 
        "riesgo cardiovascular", "score framingham", "ascvd"
    ],
    "Inmunología": ["autoinmunidad", "inmunodeficiencia", "alergias", "linfocitos"],
    "Inmunoterapia": ["inmunoterapia", "terapia car-t", "checkpoint inmunológico"],
    "Insuficiencia Cardíaca": [
        "insuficiencia cardíaca", "ic", "fallo cardíaco", "disfunción ventricular", 
        "icfe", "icfd", "fracción de eyección reducida", "fracción de eyección preservada",
        "nyha clase ii", "nyha clase iii", "edema pulmonar", "congestión venosa"
    ],
    "Investigación Clínica": ["ensayo clínico", "randomizado", "estudio de cohorte", "fase iii"],
    "Leucemia": ["leucemia", "leucemias agudas", "leucemia mieloide"],
    "Microbiología": ["microbiología", "bacterias", "virus", "antimicrobianos"],
    "Nefrología": ["insuficiencia renal", "glomerulonefritis", "diálisis"],
    "Neumología": ["asma", "epoc", "fibrosis pulmonar", "síndrome de apnea del sueño"],
    "Neurociencia": ["neurociencia", "plasticidad neuronal", "sinapsis", "neurodegeneración"],
    "Oncología Molecular": ["oncología molecular", "mutaciones tumorales", "biomarcadores cáncer"],
    "Procedimientos Cardiológicos": [
        "cateterismo cardíaco", "angioplastia", "stent coronario", 
        "bypass coronario", "cabg", "ecocardiograma", "eco stress", 
        "resonancia cardíaca", "prueba de esfuerzo", "holter"
    ],
    "Síndrome Coronario Agudo": [
        "síndrome coronario agudo", "sca", "infarto agudo de miocardio", "iam", 
        "iamcest", "iamnest", "angina inestable", "troponina elevada", 
        "oclusión coronaria", "elevación st", "depresión st"
    ],
    "Valvulopatías": [
        "valvulopatía", "estenosis aórtica", "insuficiencia aórtica", 
        "estenosis mitral", "insuficiencia mitral", "prolapso mitral", 
        "tavi", "taavi", "anillo mitral", "reemplazo valvular"
    ],
}

# ==================
# CLASE SSH MANAGER
# ==================
class SSHManager:
    MAX_RETRIES = 3
    RETRY_DELAY = 5  # segundos

    @staticmethod
    def get_connection():
        if not CONFIG.REMOTE or not CONFIG.REMOTE['HOST']:
            logging.error("Configuración remota no disponible")
            return None
            
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        for attempt in range(SSHManager.MAX_RETRIES):
            try:
                ssh.connect(
                    hostname=CONFIG.REMOTE['HOST'],
                    port=CONFIG.REMOTE['PORT'],  # Usando el puerto configurado en secrets.toml
                    username=CONFIG.REMOTE['USER'],
                    password=CONFIG.REMOTE['PASSWORD'],
                    timeout=CONFIG.TIMEOUT_SECONDS
                )
                logging.info("Conexión SSH establecida")
                return ssh
            except Exception as e:
                logging.warning(f"Intento {attempt+1} fallido: {str(e)}")
                if attempt < SSHManager.MAX_RETRIES - 1:
                    time.sleep(SSHManager.RETRY_DELAY)
                else:
                    logging.error("Fallo al conectar via SSH")
                    return None

    @staticmethod
    def verify_file_integrity(local_path, remote_path, sftp):
        try:
            local_size = os.path.getsize(local_path)
            remote_size = sftp.stat(remote_path).st_size
            return local_size == remote_size
        except Exception as e:
            logging.error(f"Error verificando integridad: {str(e)}")
            return False

    @staticmethod
    def download_remote_file():
        if not CONFIG.REMOTE or not CONFIG.REMOTE['HOST']:
            return False
            
        remote_path = os.path.join(CONFIG.REMOTE['DIR'], CONFIG.REMOTE_MANUAL_FILE)
        
        for attempt in range(SSHManager.MAX_RETRIES):
            ssh = SSHManager.get_connection()
            if not ssh:
                return False
                
            try:
                with ssh.open_sftp() as sftp:
                    try:
                        sftp.stat(remote_path)
                    except FileNotFoundError:
                        logging.info("Archivo remoto no encontrado")
                        return False
                        
                    sftp.get(remote_path, CONFIG.CSV_FILENAME)
                    
                    if SSHManager.verify_file_integrity(CONFIG.CSV_FILENAME, remote_path, sftp):
                        logging.info("Archivo descargado correctamente")
                        return True
                    else:
                        logging.warning(f"Error de integridad, reintentando... (intento {attempt+1})")
                        if attempt < SSHManager.MAX_RETRIES - 1:
                            time.sleep(SSHManager.RETRY_DELAY)
                        else:
                            raise Exception("Fallo en verificación de integridad")
                            
            except Exception as e:
                logging.error(f"Error en descarga (intento {attempt+1}): {str(e)}")
                if attempt == SSHManager.MAX_RETRIES - 1:
                    return False
                    
            finally:
                ssh.close()

    @staticmethod
    def upload_remote_file():
        if not CONFIG.REMOTE or not CONFIG.REMOTE['HOST']:
            return False
            
        if not os.path.exists(CONFIG.CSV_FILENAME):
            logging.error("Archivo local no existe")
            return False
            
        remote_path = os.path.join(CONFIG.REMOTE['DIR'], CONFIG.REMOTE_MANUAL_FILE)
        
        for attempt in range(SSHManager.MAX_RETRIES):
            ssh = SSHManager.get_connection()
            if not ssh:
                return False
                
            try:
                with ssh.open_sftp() as sftp:
                    sftp.put(CONFIG.CSV_FILENAME, remote_path)
                    
                    if SSHManager.verify_file_integrity(CONFIG.CSV_FILENAME, remote_path, sftp):
                        logging.info("Archivo subido correctamente")
                        return True
                    else:
                        logging.warning(f"Error de integridad, reintentando... (intento {attempt+1})")
                        if attempt < SSHManager.MAX_RETRIES - 1:
                            time.sleep(SSHManager.RETRY_DELAY)
                        else:
                            raise Exception("Fallo en verificación de integridad")
                            
            except Exception as e:
                logging.error(f"Error en subida (intento {attempt+1}): {str(e)}")
                if attempt == SSHManager.MAX_RETRIES - 1:
                    return False
                    
            finally:
                ssh.close()

# ====================
# FUNCIONES PRINCIPALES
# ====================
def determinar_grupo(jif5years):
    if pd.isna(jif5years):
        return "Grupo 1 (sin factor de impacto)"
    try:
        jif = float(jif5years)
        if jif <= 0.9:
            return "Grupo 2 (FI ≤ 0.9)"
        elif jif <= 2.99:
            return "Grupo 3 (FI 1-2.99)"
        elif jif <= 5.99:
            return "Grupo 4 (FI 3-5.99)"
        elif jif <= 8.99:
            return "Grupo 5 (FI 6-8.99)"
        elif jif <= 11.99:
            return "Grupo 6 (FI 9-11.99)"
        else:
            return "Grupo 7 (FI ≥ 12)"
    except ValueError:
        return "Grupo 1 (sin factor de impacto)"

def highlight_author(author: str, investigator_name: str) -> str:
    if investigator_name and investigator_name.lower() == author.lower():
        return f"<span style='background-color: {CONFIG.HIGHLIGHT_COLOR};'>{author}</span>"
    return author

def sync_with_remote():
    try:
        if not CONFIG.REMOTE:
            return False
            
        st.info("🔄 Sincronizando con el servidor remoto...")
        
        if SSHManager.download_remote_file():
            st.success("✅ Sincronización completada")
            return True
        else:
            if not Path(CONFIG.CSV_FILENAME).exists():
                columns = [
                    'economic_number', 'participation_key', 'investigator_name',
                    'corresponding_author', 'coauthors', 'article_title', 'year',
                    'pub_date', 'volume', 'number', 'pages', 'journal_full',
                    'journal_abbrev', 'doi', 'jcr_group', 'pmid', 'selected_keywords'
                ]
                pd.DataFrame(columns=columns).to_csv(CONFIG.CSV_FILENAME, index=False)
                st.info("ℹ️ No se encontró archivo remoto. Se creó uno nuevo localmente")
            
            return False

    except Exception as e:
        st.error(f"❌ Error en sincronización: {str(e)}")
        logging.error(f"Sync Error: {str(e)}")
        return False

def save_to_csv(data: dict):
    try:
        sync_success = sync_with_remote()
        
        columns = [
            'economic_number', 'participation_key', 'investigator_name',
            'corresponding_author', 'coauthors', 'article_title', 'year',
            'pub_date', 'volume', 'number', 'pages', 'journal_full',
            'journal_abbrev', 'doi', 'jcr_group', 'pmid', 'selected_keywords'
        ]

        if Path(CONFIG.CSV_FILENAME).exists():
            try:
                df_existing = pd.read_csv(
                    CONFIG.CSV_FILENAME,
                    encoding='utf-8-sig',
                    dtype={'economic_number': str}
                )
                missing_cols = set(columns) - set(df_existing.columns)
                for col in missing_cols:
                    df_existing[col] = ""
            except:
                df_existing = pd.DataFrame(columns=columns)
        else:
            df_existing = pd.DataFrame(columns=columns)

        df_new = pd.DataFrame([data])
        for col in df_new.columns:
            if df_new[col].dtype == object:
                df_new[col] = df_new[col].astype(str).str.replace(r'\r\n|\n|\r', ' ', regex=True).str.strip()

        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        
        for col in columns:
            if col not in df_combined.columns:
                df_combined[col] = ""

        df_combined.to_csv(CONFIG.CSV_FILENAME, index=False, encoding='utf-8-sig')

        if CONFIG.REMOTE:
            with st.spinner("Subiendo datos al servidor remoto..."):
                if SSHManager.upload_remote_file():
                    st.success("✅ Registro guardado exitosamente en el servidor remoto!")
                    return True
                else:
                    st.warning("⚠️ Los datos se guardaron localmente pero no se pudo subir al remoto")
                    return False
        else:
            st.success("✅ Registro guardado localmente (no hay configuración remota)")
            return True

    except Exception as e:
        st.error(f"❌ Error al guardar en CSV: {str(e)}")
        logging.error(f"Save CSV Error: {str(e)}")
        return False

def main():
    st.set_page_config(
        page_title=CONFIG.APP_TITLE,
        page_icon=CONFIG.APP_ICON,
        layout="centered"
    )

    col1, col2 = st.columns([1, 3])
    with col1:
        st.image(CONFIG.LOGO_IMAGE, width=CONFIG.LOGO_WIDTH)
    with col2:
        st.title(CONFIG.APP_TITLE)
    
    with st.spinner("Conectando con el servidor remoto..."):
        if not sync_with_remote():
            st.warning("""
            ⚠️ No se pudo conectar con el servidor remoto. 
            Trabajando en modo local. Los datos se sincronizarán cuando se restablezca la conexión.
            """)

    st.header("1. Información del investigador")
    economic_number = st.text_input("🔢 Número económico del investigador:", help="Ingrese solo dígitos")
    
    if CONFIG.REQUIRED_ECONOMIC_NUMBER and (not economic_number or not economic_number.isdigit()):
        st.warning("Por favor ingrese un número económico válido (solo dígitos)")
        return

    st.markdown(f"**🧾 Número económico ingresado:** `{economic_number}`")

    if Path(CONFIG.CSV_FILENAME).exists():
        try:
            manual_df = pd.read_csv(CONFIG.CSV_FILENAME, encoding='utf-8-sig', dtype={'economic_number': str})
            manual_df['economic_number'] = manual_df['economic_number'].astype(str).str.strip()

            filtered_records = manual_df[manual_df['economic_number'] == economic_number]

            if not filtered_records.empty:
                st.subheader(f"📋 Registros existentes para el número económico {economic_number}")
                cols_to_show = ['economic_number', 'participation_key', 'investigator_name', 'article_title', 'journal_full']
                st.dataframe(filtered_records[cols_to_show], hide_index=True, use_container_width=True)
            
            if st.radio("¿Desea añadir un nuevo registro?", ["No", "Sí"], index=0) == "No":
                st.success("Proceso finalizado. Puede cerrar la aplicación.")
                return

        except Exception as e:
            st.error(f"Error al leer {CONFIG.CSV_FILENAME}: {str(e)}")
            return
    else:
        st.info(f"No se encontró un archivo {CONFIG.CSV_FILENAME} existente. Se creará uno nuevo al guardar el primer registro.")

    st.header("2. Información del artículo")
    article_title = st.text_area("📄 Título del artículo:", height=100)
    year = st.text_input("📅 Año de publicación:")
    pub_date = st.text_input("🗓️ Fecha completa de publicación (YYYY-MM-DD):", help="Formato: AAAA-MM-DD")
    volume = st.text_input("📚 Volumen (ej 79(3), volumen = 79)")
    number = st.text_input("# Número (ej 79(3), número = 3)")
    pages = st.text_input("🔖 Páginas (ej. 123-130):")
    doi = st.text_input("🌐 DOI:")
    pmid = st.text_input("🔍 PMID (opcional):")
    
    st.header("3. Información de la revista")
    journal_full = st.text_input("🏛️ Nombre completo de la revista:")
    journal_abbrev = st.text_input("🏷️ Abreviatura de la revista:")
    jcr_group = st.selectbox(
        "🏆 Grupo JCR:",
        options=[
            "Grupo 1 (sin factor de impacto)",
            "Grupo 2 (FI ≤ 0.9)",
            "Grupo 3 (FI 1-2.99)",
            "Grupo 4 (FI 3-5.99)",
            "Grupo 5 (FI 6-8.99)",
            "Grupo 6 (FI 9-11.99)",
            "Grupo 7 (FI ≥ 12)",
            "Grupo no determinado"
        ],
        index=0
    )
    
    st.header("4. Autores del artículo")
    corresponding_author = st.text_input("📌 Autor de correspondencia:")
    coauthors = st.text_area("👥 Coautores (separados por punto y coma ';' y excluya al autor para correspondencia):", help="Ejemplo: Autor1; Autor2; Autor3")
    
    st.header("5. Palabras clave")
    st.markdown(f"Seleccione {CONFIG.MAX_KEYWORDS} palabras clave relevantes para el artículo:")
    all_categories = list(KEYWORD_CATEGORIES.keys())
    selected_categories = st.multiselect(
        "Palabras clave:",
        options=all_categories,
        default=[],
        max_selections=CONFIG.MAX_KEYWORDS
    )
    if len(selected_categories) < CONFIG.MAX_KEYWORDS:
        st.warning(f"Por favor seleccione al menos {CONFIG.MAX_KEYWORDS} palabras clave (seleccionadas: {len(selected_categories)})")
    
    st.header("6. Verificación de autoría")
    authors_list = []
    if corresponding_author:
        authors_list.append(corresponding_author)
    if coauthors:
        authors_list.extend([author.strip() for author in coauthors.split(";") if author.strip()])

    if not authors_list:
        st.error("Debe ingresar al menos un autor")
        return
    
    investigator_name = st.selectbox("Seleccione su nombre como aparece en la publicación:", authors_list)
    st.markdown(f"**Selección actual:** {highlight_author(investigator_name, investigator_name)}", unsafe_allow_html=True)
    
    participation_key = "CA" if investigator_name == corresponding_author else f"{authors_list.index(investigator_name)}C"
    
    st.header("📋 Resumen del registro")
    st.markdown("**Información del artículo**")
    st.write(f"📄 Título: {article_title}")
    st.write(f"📅 Año: {year}")
    st.write(f"🏛️ Revista: {journal_full}")
    
    st.markdown("**Autores**")
    st.markdown(f"📌 Correspondencia: {highlight_author(corresponding_author, investigator_name)}", unsafe_allow_html=True)
    if coauthors:
        st.markdown("👥 Coautores:")
        for author in [a.strip() for a in coauthors.split(";") if a.strip()]:
            st.markdown(f"- {highlight_author(author, investigator_name)}", unsafe_allow_html=True)
    
    st.markdown("**Identificación**")
    st.write(f"🔢 Número económico: {economic_number}")
    st.write(f"👤 Investigador: {investigator_name}")
    st.write(f"🔑 Clave participación: {participation_key}")
    
    selected_keywords_str = str(selected_categories[:CONFIG.MAX_KEYWORDS]) if selected_categories else "[]"
    
    data = {
        'economic_number': economic_number,
        'participation_key': participation_key,
        'investigator_name': investigator_name,
        'corresponding_author': corresponding_author,
        'coauthors': coauthors,
        'article_title': article_title,
        'year': year,
        'pub_date': pub_date if pub_date else year,
        'volume': volume,
        'number': number,
        'pages': pages,
        'journal_full': journal_full,
        'journal_abbrev': journal_abbrev,
        'doi': doi,
        'jcr_group': jcr_group,
        'pmid': pmid,
        'selected_keywords': selected_keywords_str
    }
    
    if st.button("💾 Guardar registro manual, verifique que haya todos los datos sean correctos", type="primary"):
        if save_to_csv(data):
            st.balloons()
            st.success(f"✅ Registro guardado exitosamente en {CONFIG.CSV_FILENAME}!")
            st.subheader("📄 Registro completo capturado")
            st.json(data)

if __name__ == "__main__":
    main()
