import streamlit as st
import pandas as pd
import csv
from pathlib import Path
from datetime import datetime
import paramiko
import time
import os
import logging
from PIL import Image

# Configuración de logging mejorada
logging.basicConfig(
    filename='tesis.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ====================
# CATEGORÍAS DE KEYWORDS
# ====================
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

# ====================
# CONFIGURACIÓN INICIAL
# ====================
class Config:
    def __init__(self):
        self.SMTP_SERVER = st.secrets.get("smtp_server")
        self.SMTP_PORT = st.secrets.get("smtp_port")
        self.EMAIL_USER = st.secrets.get("email_user")
        self.EMAIL_PASSWORD = st.secrets.get("email_password")
        self.NOTIFICATION_EMAIL = st.secrets.get("notification_email")
        self.CSV_FILENAME = "tesis.csv"
        self.REMOTE_PRODUCTOS_FILE = st.secrets.get("remote_tesis")
        self.TIMEOUT_SECONDS = 30
        self.MAX_KEYWORDS = 3
        self.HIGHLIGHT_COLOR = "#90EE90"
        self.LOGO_PATH = "escudo_COLOR.jpg"
        
        self.REMOTE = {
            'HOST': st.secrets.get("remote_host"),
            'USER': st.secrets.get("remote_user"),
            'PASSWORD': st.secrets.get("remote_password"),
            'PORT': st.secrets.get("remote_port"),
            'DIR': st.secrets.get("remote_dir")
        }

CONFIG = Config()

# ==================
# CLASE SSH MANAGER
# ==================
class SSHManager:
    MAX_RETRIES = 3
    RETRY_DELAY = 5  # segundos

    @staticmethod
    def get_connection():
        """Establece conexión SSH segura con reintentos"""
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        for attempt in range(SSHManager.MAX_RETRIES):
            try:
                ssh.connect(
                    hostname=CONFIG.REMOTE['HOST'],
                    port=CONFIG.REMOTE['PORT'],
                    username=CONFIG.REMOTE['USER'],
                    password=CONFIG.REMOTE['PASSWORD'],
                    timeout=CONFIG.TIMEOUT_SECONDS
                )
                logging.info(f"Conexión SSH establecida (intento {attempt + 1})")
                return ssh
            except Exception as e:
                logging.warning(f"Intento {attempt + 1} fallido: {str(e)}")
                if attempt < SSHManager.MAX_RETRIES - 1:
                    time.sleep(SSHManager.RETRY_DELAY)
                else:
                    logging.error("Fallo definitivo al conectar via SSH")
                    st.error(f"Error de conexión SSH después de {SSHManager.MAX_RETRIES} intentos: {str(e)}")
                    return None

    @staticmethod
    def verify_file_integrity(local_path, remote_path, sftp):
        """Verifica que el archivo se transfirió correctamente"""
        try:
            local_size = os.path.getsize(local_path)
            remote_size = sftp.stat(remote_path).st_size
            return local_size == remote_size
        except Exception as e:
            logging.error(f"Error verificando integridad: {str(e)}")
            return False

    @staticmethod
    def download_remote_file(remote_path, local_path):
        """Descarga un archivo con verificación de integridad"""
        for attempt in range(SSHManager.MAX_RETRIES):
            ssh = SSHManager.get_connection()
            if not ssh:
                return False
                
            try:
                with ssh.open_sftp() as sftp:
                    try:
                        sftp.stat(remote_path)
                    except FileNotFoundError:
                        # Crear archivo local con estructura correcta
                        columns = [
                            'economic_number', 'titulo_tesis', 'tipo_tesis', 'year',
                            'pub_date', 'departamento', 'directores', 'paginas',
                            'idioma', 'estudiante', 'coautores', 'selected_keywords'
                        ]
                        pd.DataFrame(columns=columns).to_csv(local_path, index=False)
                        logging.info(f"Archivo remoto no encontrado, creado local con estructura: {local_path}")
                        return True
                        
                    sftp.get(remote_path, local_path)
                    
                    if SSHManager.verify_file_integrity(local_path, remote_path, sftp):
                        logging.info(f"Archivo descargado correctamente: {remote_path} a {local_path}")
                        return True
                    else:
                        logging.warning(f"Error de integridad en descarga, reintentando... (intento {attempt + 1})")
                        if attempt < SSHManager.MAX_RETRIES - 1:
                            time.sleep(SSHManager.RETRY_DELAY)
                        else:
                            raise Exception("Fallo en verificación de integridad después de múltiples intentos")
                            
            except Exception as e:
                logging.error(f"Error en descarga (intento {attempt + 1}): {str(e)}")
                if attempt == SSHManager.MAX_RETRIES - 1:
                    st.error(f"Error descargando archivo remoto después de {SSHManager.MAX_RETRIES} intentos: {str(e)}")
                    return False
                    
            finally:
                ssh.close()

    @staticmethod
    def upload_remote_file(local_path, remote_path):
        """Sube un archivo con verificación de integridad"""
        if not os.path.exists(local_path):
            logging.error(f"Archivo local no existe: {local_path}")
            st.error("El archivo local no existe")
            return False
            
        for attempt in range(SSHManager.MAX_RETRIES):
            ssh = SSHManager.get_connection()
            if not ssh:
                return False
                
            try:
                with ssh.open_sftp() as sftp:
                    sftp.put(local_path, remote_path)
                    
                    if SSHManager.verify_file_integrity(local_path, remote_path, sftp):
                        logging.info(f"Archivo subido correctamente: {local_path} a {remote_path}")
                        return True
                    else:
                        logging.warning(f"Error de integridad en subida, reintentando... (intento {attempt + 1})")
                        if attempt < SSHManager.MAX_RETRIES - 1:
                            time.sleep(SSHManager.RETRY_DELAY)
                        else:
                            raise Exception("Fallo en verificación de integridad después de múltiples intentos")
                            
            except Exception as e:
                logging.error(f"Error en subida (intento {attempt + 1}): {str(e)}")
                if attempt == SSHManager.MAX_RETRIES - 1:
                    st.error(f"Error subiendo archivo remoto después de {SSHManager.MAX_RETRIES} intentos: {str(e)}")
                    return False
                    
            finally:
                ssh.close()

# ====================
# FUNCIONES PRINCIPALES
# ====================
def highlight_author(author: str, investigator_name: str) -> str:
    if investigator_name and investigator_name.lower() == author.lower():
        return f"<span style='background-color: {CONFIG.HIGHLIGHT_COLOR};'>{author}</span>"
    return author

def sync_with_remote():
    """Sincroniza el archivo local con el remoto"""
    try:
        st.info("🔄 Sincronizando con el servidor remoto...")
        remote_path = os.path.join(CONFIG.REMOTE['DIR'], CONFIG.REMOTE_PRODUCTOS_FILE)

        # Intenta descargar el archivo remoto
        download_success = SSHManager.download_remote_file(remote_path, CONFIG.CSV_FILENAME)

        if not download_success:
            # Si no existe el archivo remoto, crea uno local con estructura correcta
            columns = [
                'economic_number', 'titulo_tesis', 'tipo_tesis', 'year',
                'pub_date', 'departamento', 'directores', 'paginas',
                'idioma', 'estudiante', 'coautores', 'selected_keywords'
            ]

            # Verifica si el archivo local ya existe
            if not Path(CONFIG.CSV_FILENAME).exists():
                pd.DataFrame(columns=columns).to_csv(CONFIG.CSV_FILENAME, index=False)
                st.info("ℹ️ No se encontró archivo remoto. Se creó uno nuevo localmente con la estructura correcta.")
            else:
                # Si el archivo local existe pero está vacío o corrupto
                try:
                    df = pd.read_csv(CONFIG.CSV_FILENAME)
                    if df.empty:
                        pd.DataFrame(columns=columns).to_csv(CONFIG.CSV_FILENAME, index=False)
                except:
                    pd.DataFrame(columns=columns).to_csv(CONFIG.CSV_FILENAME, index=False)

            return False

        # Verifica que el archivo descargado no esté vacío
        try:
            df = pd.read_csv(CONFIG.CSV_FILENAME)
            if df.empty:
                st.warning("El archivo remoto está vacío")
        except pd.errors.EmptyDataError:
            st.warning("El archivo remoto está vacío o corrupto")
            columns = [
                'economic_number', 'titulo_tesis', 'tipo_tesis', 'year',
                'pub_date', 'departamento', 'directores', 'paginas',
                'idioma', 'estudiante', 'coautores', 'selected_keywords'
            ]
            pd.DataFrame(columns=columns).to_csv(CONFIG.CSV_FILENAME, index=False)
            return False

        st.success("✅ Sincronización con servidor remoto completada")
        return True

    except Exception as e:
        st.error(f"❌ Error en sincronización: {str(e)}")
        logging.error(f"Sync Error: {str(e)}")
        return False

def save_to_csv(data: dict):
    """Guarda los datos en el CSV local y remoto"""
    try:
        with st.spinner("Sincronizando datos con el servidor..."):
            if not sync_with_remote():
                st.warning("⚠️ Trabajando con copia local debido a problemas de conexión")

        columns = [
            'economic_number', 'titulo_tesis', 'tipo_tesis', 'year',
            'pub_date', 'departamento', 'directores', 'paginas',
            'idioma', 'estudiante', 'coautores', 'selected_keywords'
        ]

        # Verificar si el archivo existe y tiene contenido válido
        if not Path(CONFIG.CSV_FILENAME).exists():
            df_existing = pd.DataFrame(columns=columns)
        else:
            try:
                df_existing = pd.read_csv(
                    CONFIG.CSV_FILENAME,
                    encoding='utf-8-sig',
                    dtype={'economic_number': str}
                )
                # Verificar si el DataFrame está vacío
                if df_existing.empty:
                    df_existing = pd.DataFrame(columns=columns)
                # Verificar si tiene todas las columnas necesarias
                missing_cols = set(columns) - set(df_existing.columns)
                if missing_cols:
                    for col in missing_cols:
                        df_existing[col] = ""
            except (pd.errors.EmptyDataError, pd.errors.ParserError):
                df_existing = pd.DataFrame(columns=columns)

        # Preparar el nuevo registro
        df_new = pd.DataFrame([data])

        # Limpiar los datos del nuevo registro
        for col in df_new.columns:
            if df_new[col].dtype == object:
                df_new[col] = df_new[col].astype(str).str.replace(r'\r\n|\n|\r', ' ', regex=True).str.strip()

        # Combinar los datos existentes con los nuevos
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)

        # Asegurar que todas las columnas estén presentes
        for col in columns:
            if col not in df_combined.columns:
                df_combined[col] = ""

        # Reordenar columnas
        df_combined = df_combined[columns]

        # Guardar localmente
        df_combined.to_csv(CONFIG.CSV_FILENAME, index=False, encoding='utf-8-sig')

        # Intentar subir al servidor remoto
        with st.spinner("Subiendo datos al servidor remoto..."):
            remote_path = os.path.join(CONFIG.REMOTE['DIR'], CONFIG.REMOTE_PRODUCTOS_FILE)
            if SSHManager.upload_remote_file(CONFIG.CSV_FILENAME, remote_path):
                st.success("✅ Registro guardado exitosamente en el servidor remoto!")
                return True
            else:
                st.error("❌ No se pudo subir el archivo al servidor remoto")
                st.info("ℹ️ Los datos se guardaron localmente y se intentará subir en la próxima sincronización")
                return False

    except Exception as e:
        st.error(f"❌ Error al guardar en CSV: {str(e)}")
        logging.error(f"Save CSV Error: {str(e)}")
        return False

def main():
    st.set_page_config(
        page_title="Captura Tesis",
        page_icon="📚",
        layout="centered"
    )

    # Mostrar logo si existe
    if Path(CONFIG.LOGO_PATH).exists():
        logo = Image.open(CONFIG.LOGO_PATH)
        st.image(logo, width=200)

    st.title("📚 Captura Tesis")
    
    # Sincronización inicial
    with st.spinner("Conectando con el servidor remoto..."):
        if not sync_with_remote():
            st.warning("""
            ⚠️ No se pudo conectar con el servidor remoto. 
            Trabajando en modo local. Los datos se sincronizarán cuando se restablezca la conexión.
            """)
    
    # Validación del número económico
    economic_number = st.text_input("🔢 Número económico del investigador (solo dígitos):").strip()
    
    if not economic_number:
        st.warning("Por favor ingrese un número económico")
        return
        
    if not economic_number.isdigit():
        st.error("El número económico debe contener solo dígitos (0-9)")
        return

    try:
        if not Path(CONFIG.CSV_FILENAME).exists():
            pd.DataFrame().to_csv(CONFIG.CSV_FILENAME, index=False)
            tesis_df = pd.DataFrame()
        else:
            tesis_df = pd.read_csv(CONFIG.CSV_FILENAME, encoding='utf-8-sig', dtype={'economic_number': str})
            tesis_df['economic_number'] = tesis_df['economic_number'].astype(str).str.strip()

        filtered_records = tesis_df[tesis_df['economic_number'] == economic_number]

        if not filtered_records.empty:
            st.subheader(f"📋 Tesis registradas para {economic_number}")
            st.dataframe(filtered_records[['titulo_tesis', 'tipo_tesis', 'year']], hide_index=True)
        
        if st.radio("¿Desea añadir un nuevo registro?", ["No", "Sí"], index=0) == "No":
            return
    except Exception as e:
        st.error(f"❌ Error al leer {CONFIG.CSV_FILENAME}: {str(e)}")
        logging.error(f"CSV Read Error: {str(e)}")

    st.subheader("📝 Información de la tesis")
    
    # Campos de entrada manual para tesis
    titulo_tesis = st.text_area("📄 Título de la tesis:", height=100, key="titulo_tesis")
    tipo_tesis = st.selectbox(
        "🎓 Tipo de tesis:",
        options=["Licenciatura", "Maestría", "Doctorado"],
        index=0,
        key="tipo_tesis"
    )
    year = st.text_input("📅 Año de publicación:", key="year")
    pub_date = st.text_input("🗓️ Fecha completa de publicación (YYYY-MM-DD):", help="Formato: AAAA-MM-DD", key="pub_date")
    departamento = st.text_input("🏛️ Departamento (INCICh):", key="departamento")
    directores = st.text_input("👨‍🏫 Director(es) de tesis (separados por ';'):", key="directores")
    paginas = st.text_input("🔖 Número de páginas:", key="paginas")
    idioma = st.text_input("🌐 Idioma principal:", key="idioma")
    estudiante = st.text_input("👤 Nombre completo del estudiante:", key="estudiante")
    coautores = st.text_area("👥 Coautores (si aplica, separados por ';'):", key="coautores")
    
    # Sección de palabras clave
    st.header("🔍 Palabras clave")
    st.markdown(f"Seleccione {CONFIG.MAX_KEYWORDS} palabras clave relevantes:")
    all_categories = list(KEYWORD_CATEGORIES.keys())
    selected_categories = st.multiselect(
        "Palabras clave:",
        options=all_categories,
        default=[],
        max_selections=CONFIG.MAX_KEYWORDS,
        key="keywords"
    )
    if len(selected_categories) < CONFIG.MAX_KEYWORDS:
        st.warning(f"Se recomiendan {CONFIG.MAX_KEYWORDS} palabras clave (seleccionadas: {len(selected_categories)})")

    # Resumen del registro
    st.subheader("📋 Resumen del registro")
    st.markdown("**Información de la tesis**")
    st.write(f"📄 Título: {titulo_tesis}")
    st.write(f"🎓 Tipo: {tipo_tesis}")
    st.write(f"📅 Año: {year}")
    st.write(f"🏛️ Departamento: {departamento}")
    
    st.markdown("**Autores**")
    st.markdown(f"👤 Estudiante: {highlight_author(estudiante, estudiante)}", unsafe_allow_html=True)
    if coautores:
        st.markdown("👥 Coautores:")
        for author in [a.strip() for a in coautores.split(";") if a.strip()]:
            st.markdown(f"- {highlight_author(author, estudiante)}", unsafe_allow_html=True)
    
    st.markdown("**Identificación**")
    st.write(f"🔢 Número económico: {economic_number}")
    
    # Preparar datos para guardar
    data = {
        'economic_number': economic_number,
        'titulo_tesis': titulo_tesis,
        'tipo_tesis': tipo_tesis,
        'year': year,
        'pub_date': pub_date if pub_date else year,
        'departamento': departamento,
        'directores': directores,
        'paginas': paginas,
        'idioma': idioma,
        'estudiante': estudiante,
        'coautores': coautores,
        'selected_keywords': str(selected_categories[:CONFIG.MAX_KEYWORDS])
    }
    
    if st.button("💾 Guardar registro de tesis", type="primary"):
        with st.spinner("Guardando datos..."):
            if save_to_csv(data):
                st.balloons()
                st.success("✅ Registro guardado exitosamente!")
                st.subheader("📄 Registro completo capturado")
                st.json(data)

if __name__ == "__main__":
    main()
