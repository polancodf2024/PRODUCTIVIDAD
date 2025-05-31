import streamlit as st
import pandas as pd
import paramiko
import time
import os
import logging
from pathlib import Path
from PIL import Image
import fcntl
from contextlib import contextmanager

# Configuración de logging mejorada
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
        self.SMTP_SERVER = st.secrets.get("smtp_server")
        self.SMTP_PORT = st.secrets.get("smtp_port")
        self.EMAIL_USER = st.secrets.get("email_user")
        self.EMAIL_PASSWORD = st.secrets.get("email_password")
        self.NOTIFICATION_EMAIL = st.secrets.get("notification_email")
        self.CSV_FILENAME = "manual.csv"
        self.REMOTE_PRODUCTOS_FILE = st.secrets.get("remote_manual")
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

# ==================
# GESTIÓN DE BLOQUEOS
# ==================
class LockManager:
    @staticmethod
    @contextmanager
    def file_lock(file_path):
        """Gestión robusta de bloqueos de archivos"""
        lock_path = f"{file_path}.lock"
        lock_file = None
        try:
            # Limpiar lock previo si existe y es viejo
            if os.path.exists(lock_path):
                lock_age = time.time() - os.path.getmtime(lock_path)
                if lock_age > 300:  # 5 minutos
                    try:
                        os.remove(lock_path)
                    except:
                        pass

            # Adquirir nuevo lock
            lock_file = open(lock_path, "w")
            fcntl.flock(lock_file, fcntl.LOCK_EX)
            yield
        finally:
            # Liberar lock de forma segura
            if lock_file is not None:
                try:
                    fcntl.flock(lock_file, fcntl.LOCK_UN)
                    lock_file.close()
                except:
                    pass
            if os.path.exists(lock_path):
                try:
                    os.remove(lock_path)
                except:
                    pass

# ==================
# GESTIÓN SSH MEJORADA
# ==================
class SSHManager:
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    CONNECTION_TIMEOUT = 15

    @staticmethod
    def get_connection():
        """Conexión SSH con verificación mejorada"""
        ssh = None
        last_error = None
        
        for attempt in range(SSHManager.MAX_RETRIES):
            try:
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                
                ssh.connect(
                    hostname=CONFIG.REMOTE['HOST'],
                    port=CONFIG.REMOTE['PORT'],
                    username=CONFIG.REMOTE['USER'],
                    password=CONFIG.REMOTE['PASSWORD'],
                    timeout=SSHManager.CONNECTION_TIMEOUT,
                    banner_timeout=30,
                    auth_timeout=30
                )
                
                # Verificación adicional de conexión
                stdin, stdout, stderr = ssh.exec_command('echo "Test connection"', timeout=10)
                if stdout.channel.recv_exit_status() != 0:
                    raise Exception("La prueba de conexión falló")
                
                logging.info(f"Conexión SSH establecida (intento {attempt + 1})")
                return ssh
                
            except paramiko.AuthenticationException as e:
                last_error = f"Error de autenticación: {str(e)}"
                logging.error(last_error)
                break
                
            except Exception as e:
                last_error = str(e)
                logging.warning(f"Intento {attempt + 1} fallido: {last_error}")
                if attempt < SSHManager.MAX_RETRIES - 1:
                    time.sleep(SSHManager.RETRY_DELAY)
                if ssh:
                    ssh.close()
        
        error_msg = f"Fallo al conectar via SSH: {last_error}"
        logging.error(error_msg)
        st.error(error_msg)
        return None

    @staticmethod
    def verify_file_integrity(local_path, remote_path, sftp):
        """Verificación mejorada de integridad de archivos"""
        try:
            local_size = os.path.getsize(local_path)
            remote_size = sftp.stat(remote_path).st_size
            return local_size == remote_size and local_size > 0
        except Exception as e:
            logging.error(f"Error verificando integridad: {str(e)}")
            return False

    @staticmethod
    def download_remote_file(remote_path, local_path):
        """Descarga con manejo mejorado de errores"""
        ssh = None
        for attempt in range(SSHManager.MAX_RETRIES):
            ssh = SSHManager.get_connection()
            if not ssh:
                return False

            try:
                with ssh.open_sftp() as sftp:
                    try:
                        sftp.stat(remote_path)
                    except FileNotFoundError:
                        logging.info(f"Archivo remoto no encontrado: {remote_path}")
                        return False

                    sftp.get(remote_path, local_path)

                    if SSHManager.verify_file_integrity(local_path, remote_path, sftp):
                        logging.info(f"Archivo descargado correctamente: {remote_path} a {local_path}")
                        return True
                    
                    logging.warning(f"Error de integridad en descarga, reintentando... (intento {attempt + 1})")
                    if attempt < SSHManager.MAX_RETRIES - 1:
                        time.sleep(SSHManager.RETRY_DELAY)

            except Exception as e:
                logging.error(f"Error en descarga (intento {attempt + 1}): {str(e)}")
                if attempt == SSHManager.MAX_RETRIES - 1:
                    st.error(f"Error descargando archivo remoto: {str(e)}")
                if ssh:
                    ssh.close()
                if os.path.exists(local_path):
                    try:
                        os.remove(local_path)
                    except:
                        pass

        return False

    @staticmethod
    def upload_remote_file(local_path, remote_path):
        """Subida con manejo mejorado de errores"""
        if not os.path.exists(local_path):
            logging.error(f"Archivo local no existe: {local_path}")
            st.error("El archivo local no existe")
            return False

        ssh = None
        for attempt in range(SSHManager.MAX_RETRIES):
            ssh = SSHManager.get_connection()
            if not ssh:
                return False

            try:
                with ssh.open_sftp() as sftp:
                    # Crear directorio si no existe
                    try:
                        sftp.stat(CONFIG.REMOTE['DIR'])
                    except:
                        sftp.mkdir(CONFIG.REMOTE['DIR'])

                    sftp.put(local_path, remote_path)

                    if SSHManager.verify_file_integrity(local_path, remote_path, sftp):
                        logging.info(f"Archivo subido correctamente: {local_path} a {remote_path}")
                        return True
                    
                    logging.warning(f"Error de integridad en subida, reintentando... (intento {attempt + 1})")
                    if attempt < SSHManager.MAX_RETRIES - 1:
                        time.sleep(SSHManager.RETRY_DELAY)

            except Exception as e:
                logging.error(f"Error en subida (intento {attempt + 1}): {str(e)}")
                if attempt == SSHManager.MAX_RETRIES - 1:
                    st.error(f"Error subiendo archivo remoto: {str(e)}")
                if ssh:
                    ssh.close()

        return False

# ====================
# FUNCIONES PRINCIPALES
# ====================
def create_empty_csv():
    """Crea un archivo CSV vacío con estructura correcta"""
    columns = [
        'economic_number', 'participation_key', 'investigator_name',
        'corresponding_author', 'coauthors', 'article_title', 'year',
        'pub_date', 'volume', 'number', 'pages', 'journal_full',
        'journal_abbrev', 'doi', 'jcr_group', 'pmid', 'selected_keywords', 'estado'
    ]
    pd.DataFrame(columns=columns).to_csv(CONFIG.CSV_FILENAME, index=False)
    logging.info("Archivo CSV vacío creado con estructura inicial")

def check_remote_connection():
    """Verifica la conexión al servidor remoto"""
    try:
        with st.spinner("🔍 Verificando conexión al servidor..."):
            ssh = SSHManager.get_connection()
            if not ssh:
                return False
            
            try:
                with ssh.open_sftp() as sftp:
                    try:
                        sftp.listdir(CONFIG.REMOTE['DIR'])
                        return True
                    except IOError as e:
                        st.error(f"No se puede acceder al directorio remoto: {str(e)}")
                        return False
            finally:
                ssh.close()
    except Exception as e:
        st.error(f"Error en verificación de conexión: {str(e)}")
        return False

def sync_with_remote():
    """Sincronización mejorada con el servidor remoto"""
    try:
        if not check_remote_connection():
            st.warning("⚠️ No se pudo establecer conexión con el servidor remoto")
            return False

        st.info("🔄 Sincronizando con el servidor remoto...")
        progress_bar = st.progress(0)
        
        remote_path = os.path.join(CONFIG.REMOTE['DIR'], CONFIG.REMOTE_PRODUCTOS_FILE)
        temp_file = f"temp_{int(time.time())}_{CONFIG.CSV_FILENAME}"
        
        try:
            with LockManager.file_lock(CONFIG.CSV_FILENAME):
                progress_bar.progress(10)
                
                # 1. Descargar archivo remoto
                if not SSHManager.download_remote_file(remote_path, temp_file):
                    progress_bar.progress(100)
                    if not Path(CONFIG.CSV_FILENAME).exists():
                        create_empty_csv()
                    return False
                
                progress_bar.progress(40)
                
                # 2. Procesar archivos
                local_df = pd.DataFrame()
                if Path(CONFIG.CSV_FILENAME).exists():
                    try:
                        local_df = pd.read_csv(CONFIG.CSV_FILENAME, dtype={'economic_number': str})
                    except Exception as e:
                        st.warning(f"Error leyendo archivo local: {str(e)}")
                
                remote_df = pd.DataFrame()
                if Path(temp_file).exists():
                    try:
                        remote_df = pd.read_csv(temp_file, dtype={'economic_number': str})
                    except Exception as e:
                        st.error(f"Error leyendo archivo remoto: {str(e)}")
                        return False
                
                progress_bar.progress(70)
                
                # 3. Combinar datos
                combined_df = pd.concat([local_df, remote_df]).drop_duplicates(
                    subset=['economic_number', 'article_title', 'doi'],
                    keep='last'
                ).reset_index(drop=True)
                
                # 4. Guardar resultado
                combined_df.to_csv(CONFIG.CSV_FILENAME, index=False)
                progress_bar.progress(90)
                
                st.success("✅ Sincronización completada")
                return True

        except Exception as e:
            st.error(f"❌ Error durante sincronización: {str(e)}")
            logging.error(f"Sync Error: {str(e)}", exc_info=True)
            return False
            
        finally:
            progress_bar.progress(100)
            if temp_file and Path(temp_file).exists():
                try:
                    os.remove(temp_file)
                except:
                    pass

    except Exception as e:
        st.error(f"❌ Error inesperado en sincronización: {str(e)}")
        return False

def save_to_csv(data: dict):
    """Guarda los datos en CSV con manejo robusto"""
    temp_file = None
    try:
        # Validación de campos requeridos
        required_fields = ['economic_number', 'article_title', 'investigator_name']
        for field in required_fields:
            if not data.get(field):
                raise ValueError(f"Campo requerido faltante: {field}")

        with LockManager.file_lock(CONFIG.CSV_FILENAME):
            # 1. Preparar archivo temporal
            temp_file = f"temp_{int(time.time())}_{CONFIG.CSV_FILENAME}"
            
            # 2. Cargar datos existentes
            df_existing = pd.DataFrame()
            if Path(CONFIG.CSV_FILENAME).exists():
                try:
                    df_existing = pd.read_csv(CONFIG.CSV_FILENAME, dtype={'economic_number': str})
                except Exception as e:
                    st.warning(f"Error leyendo archivo existente: {str(e)}")
                    df_existing = pd.DataFrame()

            # 3. Añadir nuevo registro
            df_new = pd.DataFrame([data])
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)

            # 4. Guardar en temporal primero
            df_combined.to_csv(temp_file, index=False)
            
            # 5. Reemplazar archivo original
            os.replace(temp_file, CONFIG.CSV_FILENAME)
            
            # 6. Sincronizar con remoto
            remote_path = os.path.join(CONFIG.REMOTE['DIR'], CONFIG.REMOTE_PRODUCTOS_FILE)
            if SSHManager.upload_remote_file(CONFIG.CSV_FILENAME, remote_path):
                st.success("✅ Registro guardado y sincronizado")
                return True
            else:
                st.warning("⚠️ Datos guardados localmente (no sincronizados)")
                return False

    except Exception as e:
        st.error(f"❌ Error al guardar: {str(e)}")
        logging.error(f"Save Error: {str(e)}", exc_info=True)
        return False
        
    finally:
        if temp_file and Path(temp_file).exists():
            try:
                os.remove(temp_file)
            except:
                pass

def highlight_author(author: str, investigator_name: str) -> str:
    """Resalta el nombre del investigador principal"""
    if investigator_name and investigator_name.lower() == author.lower():
        return f"<span style='background-color: {CONFIG.HIGHLIGHT_COLOR};'>{author}</span>"
    return author

def display_author_info(data, investigator_name):
    """Muestra información de autores con formato"""
    st.markdown("**Autores**")
    st.markdown(f"📌 Correspondencia: {highlight_author(data['corresponding_author'], investigator_name)}", unsafe_allow_html=True)
    if data['coauthors']:
        st.markdown("👥 Coautores:")
        for author in data['coauthors'].split("; "):
            st.markdown(f"- {highlight_author(author, investigator_name)}", unsafe_allow_html=True)

def display_publication_info(data):
    """Muestra detalles de la publicación"""
    st.markdown("**Detalles de publicación**")
    st.write(f"📅 Año: {data['year']}")
    st.write(f"🗓️ Fecha exacta de publicación: {data['pub_date']}")
    st.write(f"📚 Vol/Núm: {data['volume']}/{data['number']}")
    st.write(f"🔖 Páginas: {data['pages']}")
    st.write(f"🌐 DOI: {data['doi'] or 'No disponible'}")

def main():
    st.set_page_config(
        page_title="Artículos no en PubMed",
        page_icon="📝",
        layout="centered"
    )

    # Mostrar logo
    if Path(CONFIG.LOGO_PATH).exists():
        logo = Image.open(CONFIG.LOGO_PATH)
        st.image(logo, width=200)

    st.title("📝 Artículos no en PubMed")

    # Sección de estado de conexión
    with st.expander("🔌 Estado de conexión", expanded=True):
        if check_remote_connection():
            st.success("✅ Conectado al servidor remoto")
            if not sync_with_remote():
                st.warning("⚠️ Usando datos locales (la sincronización falló)")
        else:
            st.warning("⚠️ Trabajando en modo local (sin conexión al servidor)")
            if not Path(CONFIG.CSV_FILENAME).exists():
                create_empty_csv()

    # Validación del número económico
    economic_number = st.text_input("🔢 Número económico del investigador (solo dígitos):").strip()

    if not economic_number:
        st.warning("Por favor ingrese un número económico")
        return

    if not economic_number.isdigit():
        st.error("El número económico debe contener solo dígitos (0-9)")
        return

    try:
        # Leer datos con bloqueo
        with LockManager.file_lock(CONFIG.CSV_FILENAME):
            if not Path(CONFIG.CSV_FILENAME).exists():
                create_empty_csv()
                manual_df = pd.DataFrame()
            else:
                manual_df = pd.read_csv(
                    CONFIG.CSV_FILENAME,
                    encoding='utf-8-sig',
                    dtype={'economic_number': str}
                )
                manual_df['economic_number'] = manual_df['economic_number'].astype(str).str.strip()
                
                # Inicialización robusta del campo 'estado'
                if 'estado' not in manual_df.columns:
                    manual_df['estado'] = 'A'
                else:
                    manual_df['estado'] = manual_df['estado'].apply(
                        lambda x: 'A' if pd.isna(x) or str(x).strip() not in ['A', 'X'] else str(x).strip()
                    )

        # Filtrar registros del usuario actual
        filtered_records = manual_df[manual_df['economic_number'] == economic_number]

        # Mostrar registros existentes
        if not filtered_records.empty:
            st.subheader(f"📋 Registros existentes para {economic_number}")
            
            st.info("""
            **Nota sobre el campo Estado:**  
            - 'A' = Artículo activo (valor por defecto)  
            - 'X' = Artículo marcado para borrar  
            Si desea eliminar un registro, cambie su Estado a 'X' y guarde los cambios.
            """)
            
            # Editor de datos con bloqueo
            edited_df = st.data_editor(
                filtered_records[['article_title', 'journal_full', 'estado']],
                column_config={
                    "estado": st.column_config.SelectboxColumn(
                        "Estado",
                        help="Seleccione 'A' para activo o 'X' para marcar para borrar",
                        options=["A", "X"],
                        required=True,
                        default="A",
                        width="small"
                    )
                },
                hide_index=True,
                use_container_width=True
            )
            
            # Guardar cambios si se modificó el estado
            if not edited_df.equals(filtered_records[['article_title', 'journal_full', 'estado']]):
                with LockManager.file_lock(CONFIG.CSV_FILENAME):
                    manual_df.loc[filtered_records.index, 'estado'] = edited_df['estado']
                    manual_df.to_csv(CONFIG.CSV_FILENAME, index=False, encoding='utf-8-sig')
                    
                    # Sincronizar con remoto
                    remote_path = os.path.join(CONFIG.REMOTE['DIR'], CONFIG.REMOTE_PRODUCTOS_FILE)
                    if SSHManager.upload_remote_file(CONFIG.CSV_FILENAME, remote_path):
                        st.success("✅ Cambios guardados y sincronizados con el servidor remoto")
                    else:
                        st.warning("⚠️ Cambios guardados localmente (no se pudo sincronizar)")

        if st.radio("¿Desea añadir un nuevo registro?", ["No", "Sí"], index=0) == "No":
            return

        st.subheader("📝 Información del artículo")

        # Campos de entrada manual
        article_title = st.text_area("📄 Título del artículo:", height=100)
        year = st.text_input("📅 Año de publicación:")
        pub_date = st.text_input("🗓️ Fecha exacta de publicación (YYYY-MM-DD):", help="Formato: AAAA-MM-DD")
        volume = st.text_input("📚 Volumen (ej. 79(3), volumen 79):")
        number = st.text_input("# Número (ej. 79(3), número 3):")
        pages = st.text_input("🔖 Páginas (ej. 123-130):")
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
        doi = st.text_input("🌐 DOI:")
        pmid = st.text_input("🔍 PMID (opcional):")
        corresponding_author = st.text_input("📌 Autor de correspondencia:")
        coauthors = st.text_area("👥 Coautores (separados por punto y coma ';'):", help="Ejemplo: Autor1; Autor2; Autor3")

        # Palabras clave
        st.subheader("🔑 Palabras clave")
        selected_categories = st.multiselect(
            f"Seleccione {CONFIG.MAX_KEYWORDS} palabras clave:",
            options=list(KEYWORD_CATEGORIES.keys()),
            default=[],
            max_selections=CONFIG.MAX_KEYWORDS
        )

        if len(selected_categories) < CONFIG.MAX_KEYWORDS:
            st.error(f"Debe seleccionar exactamente {CONFIG.MAX_KEYWORDS} palabras clave")
            return

        # Verificación de autoría
        st.subheader("👤 Verificación de autoría")
        authors_list = []
        if corresponding_author:
            authors_list.append(corresponding_author)
        if coauthors:
            authors_list.extend([author.strip() for author in coauthors.split(";") if author.strip()])

        if not authors_list:
            st.error("Debe ingresar al menos un autor")
            return

        investigator_name = st.selectbox("Seleccione su nombre como aparece en la publicación:", authors_list)
        
        # CALCULAR participation_key ANTES de usarlo
        participation_key = "CA" if investigator_name == corresponding_author else f"{authors_list.index(investigator_name)}C"

        st.subheader("📋 Resumen del registro")
        st.markdown("**Información del artículo**")
        st.write(f"📄 Título: {article_title}")
        st.write(f"📅 Año: {year}")
        st.write(f"🗓️ Fecha exacta de publicación: {pub_date if pub_date else year}")
        st.write(f"🏛️ Revista (nombre completo): {journal_full}")
        st.write(f"🏷️ Revista (abreviatura): {journal_abbrev}")
        st.write(f"📚 Volumen: {volume}")
        st.write(f"📚 Número: {number}")
        st.write(f"🔖 Páginas: {pages}")

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

        # Preparar datos para guardar
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
            'selected_keywords': str(selected_categories[:CONFIG.MAX_KEYWORDS]),
            'estado': 'A'
        }

        if st.button("💾 Guardar registro", type="primary"):
            with st.spinner("Guardando datos..."):
                if save_to_csv(data):
                    st.balloons()
                    st.success("✅ Registro guardado exitosamente!")
                    st.subheader("📄 Registro completo capturado")
                    st.json(data)

    except Exception as e:
        st.error(f"❌ Error crítico: {str(e)}")
        logging.error(f"Main Error: {str(e)}")

if __name__ == "__main__":
    main()
