import streamlit as st
import re
import pandas as pd
import csv
from pathlib import Path
from datetime import datetime
from difflib import get_close_matches
import paramiko
import time
import os
import logging
from PIL import Image

# Configuración de logging mejorada
logging.basicConfig(
    filename='productividad.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ====================
# CONFIGURACIÓN INICIAL
# ====================
class Config:
    def __init__(self):
        # Configuración SMTP
        self.SMTP_SERVER = st.secrets["smtp"]["server"]
        self.SMTP_PORT = st.secrets["smtp"]["port"]
        self.EMAIL_USER = st.secrets["smtp"]["user"]
        self.EMAIL_PASSWORD = st.secrets["smtp"]["password"]
        self.NOTIFICATION_EMAIL = st.secrets["smtp"]["notification_email"]
        
        # Configuración SFTP
        self.CSV_PRODUCTOS_PREFIX = "productos_"  # Prefijo para archivos CSV locales
        self.REMOTE_PRODUCTOS_PREFIX = st.secrets["prefixes"]["productos"]
        self.TIMEOUT_SECONDS = 30
        self.LOGO_PATH = "escudo_COLOR.jpg"        
        
        self.REMOTE = {
            'HOST': st.secrets["sftp"]["host"],
            'USER': st.secrets["sftp"]["user"],
            'PASSWORD': st.secrets["sftp"]["password"],
            'PORT': st.secrets["sftp"]["port"],
            'DIR': st.secrets["sftp"]["dir"]
        }

CONFIG = Config()

# ====================
# CATEGORÍAS DE KEYWORDS
# ====================
KEYWORD_CATEGORIES = {
    "Enfermedad coronaria": [],
    "Síndrome metabólico": [],
    "Hipertensión arterial sistémica/pulmonar primaria": [],
    "Enfermedad valvular": [],
    "Miocardiopatías y enfermedad de Chagas": [],
    "Sistemas biológicos: celular, molecular y producción de energía": [],
    "Cardiopatías congénitas": [],
    "Nefropatías": [],
    "Elaboración de dispositivos intracardiacos": [],
    "Medio ambiente y sociomedicina": [],
    "COVID-19 (SARS-Cov-2)": [],
    "Otros": [],
}

# ====================
# OPCIONES DISPONIBLES
# ====================
DEPARTAMENTOS_INCICH = [
    "Bioquímica",
    "Biología Molecular",
    "Biomedicina Cardiovascular",
    "Consulta Externa (Dermatología, Endocrinología, etc.)",
    "Endocrinología",
    "Farmacología",
    "Fisiología",
    "Fisiopatología Cardio-Renal",
    "Fisiotepatologíay  Cardiorenal",
    "Inmunología",
    "Instrumentación Electromecánica",
    "Unidad de Investigación UNAM-INC",
    "Otro (especifique abajo)"
]

SNI_OPCIONES = ["C", "I", "II", "III", "Emérito"]
SII_OPCIONES = ["A", "B", "C", "D", "E", "F", "Emérito"]
NOMBRAMIENTO_OPCIONES = ["Ayudante de investigador", "Investigador", "Mando medio", "Médico", "Médico especialista", "Técnico", "Otro"]

# ==================
# CLASE SSH MEJORADA
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
                            'economic_number', 'nombramiento', 'departamento', 'participation_key', 'investigator_name',
                            'corresponding_author', 'coauthors', 'article_title', 'year',
                            'pub_date', 'volume', 'number', 'pages', 'journal_full',
                            'journal_abbrev', 'doi', 'jcr_group', 'pmid', 'selected_keywords',
                            'sni', 'sii', 'pdf_filename', 'estado'  # Nuevos campos añadidos
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
# CACHE PARA REVISTAS
# ====================
class JournalCache:
    _instance = None
    _cache = {}
    _impact_factor_data = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(JournalCache, cls).__new__(cls)
            cls._instance._load_impact_factor_data()
        return cls._instance
    
    def _load_impact_factor_data(self):
        """Carga los datos de factores de impacto una sola vez"""
        if self._impact_factor_data is None:
            try:
                self._impact_factor_data = pd.read_excel(
                    'CopyofImpactFactor2024.xlsx',
                    sheet_name='2024最新完整版IF'
                )
                self._impact_factor_data['Name_lower'] = self._impact_factor_data['Name'].str.lower()
                self._impact_factor_data['Abbr_Name_lower'] = self._impact_factor_data['Abbr Name'].str.lower()
                self._impact_factor_data['JIF5Years'] = pd.to_numeric(
                    self._impact_factor_data['JIF5Years'],
                    errors='coerce'
                )
                logging.info("Datos de factores de impacto cargados exitosamente")
            except Exception as e:
                logging.error(f"Error cargando datos de factores de impacto: {str(e)}")
                self._impact_factor_data = pd.DataFrame()
    
    def get_journal_group(self, journal_name):
        """Obtiene el grupo de impacto con cache"""
        if not journal_name:
            return "Grupo no determinado"
            
        cache_key = journal_name.lower()
        if cache_key in self._cache:
            return self._cache[cache_key]
            
        group = self._find_journal_group(journal_name)
        self._cache[cache_key] = group
        return group
    
    def _find_journal_group(self, journal_name):
        """Busca el grupo de impacto para una revista"""
        if self._impact_factor_data.empty:
            return "Grupo no determinado"
            
        journal_lower = journal_name.lower()
        
        exact_match = self._impact_factor_data[
            (self._impact_factor_data['Name_lower'] == journal_lower) |
            (self._impact_factor_data['Abbr_Name_lower'] == journal_lower)
        ]
        
        if not exact_match.empty:
            return determinar_grupo(exact_match.iloc[0]['JIF5Years'])
        
        closest_match = get_close_matches(
            journal_lower,
            self._impact_factor_data['Name_lower'].tolist() + 
            self._impact_factor_data['Abbr_Name_lower'].tolist(),
            n=1, cutoff=0.6
        )
        
        if closest_match:
            match_row = self._impact_factor_data[
                (self._impact_factor_data['Name_lower'] == closest_match[0]) |
                (self._impact_factor_data['Abbr_Name_lower'] == closest_match[0])
            ].iloc[0]
            return determinar_grupo(match_row['JIF5Years'])
        
        return "Grupo no determinado"

# ====================
# FUNCIONES PRINCIPALES
# ====================
def determinar_grupo(jif5years):
    """Determina el grupo de impacto de la revista"""
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

def buscar_grupo_revista(nombre_revista):
    """Busca el grupo de impacto usando cache"""
    return JournalCache().get_journal_group(nombre_revista)

def extract_keywords(title):
    """Extrae palabras clave del título del artículo"""
    if not title:
        return []
    found_keywords = set()
    title_lower = title.lower()
    for category, keywords in KEYWORD_CATEGORIES.items():
        for keyword in keywords:
            if keyword.lower() in title_lower:
                found_keywords.add(category)
                break
    return sorted(found_keywords)

def parse_nbib_file(content: str) -> dict:
    """Parsea el contenido de un archivo .nbib"""
    data = {
        'corresponding_author': '',
        'coauthors': '',
        'article_title': '',
        'year': '',
        'pub_date': '',
        'volume': '0',  # Valor por defecto 0
        'number': '0',  # Valor por defecto 0
        'pages': '0',  # Valor por defecto 0
        'journal_full': '',
        'journal_abbrev': '',
        'doi': '',
        'jcr_group': '',
        'pmid': '',
        'investigator_name': '',
        'economic_number': '',
        'nombramiento': '',  # Nuevo campo añadido
        'departamento': '',  # Nuevo campo añadido
        'participation_key': '',
        'selected_keywords': [],
        'sni': '',  # Nuevo campo SNI
        'sii': '',   # Nuevo campo SII
        'pdf_filename': '',
        'estado': 'A'
    }

    def extract_field(pattern, multi_line=False):
        nonlocal content
        flags = re.DOTALL if multi_line else 0
        match = re.search(pattern, content, flags)
        return match.group(1).strip() if match else ''

    try:
        # Extraer PMID
        data['pmid'] = extract_field(r'PMID-\s+(\d+)')

        # Extraer autores
        authors = re.findall(r'FAU\s+-\s+(.*?)\n', content)
        if authors:
            data['corresponding_author'] = authors[0].strip()
            data['coauthors'] = "; ".join(a.strip() for a in authors[1:])

        # Extraer título del artículo
        data['article_title'] = extract_field(r'TI\s+-\s+(.*?)(?:\n[A-Z]{2}\s+-|$)', True)

        # Extraer fecha de publicación
        pub_date_match = re.search(r'DP\s+-\s+(\d{4}\s+[A-Za-z]{3}\s+\d{1,2})', content)
        if pub_date_match:
            try:
                date_obj = datetime.strptime(pub_date_match.group(1), '%Y %b %d')
                data['pub_date'] = date_obj.strftime('%Y-%m-%d')
                data['year'] = date_obj.strftime('%Y')
            except:
                data['pub_date'] = pub_date_match.group(1)
                data['year'] = pub_date_match.group(1).split()[0]
        else:
            data['year'] = extract_field(r'DP\s+-\s+(\d{4})')
            data['pub_date'] = data['year']

        # Interfaz para fecha de publicación
        st.subheader("📅 Fecha de publicación")
        st.markdown("**Suministre manualmente la fecha de publicación, no siempre PubMed la tiene  registrada**")
        default_date = f"{data['year']}-01-01" if data['year'] else ""
        pub_date = st.text_input("Ingrese la fecha de publicación (YYYY-MM-DD):",
                               value=default_date,
                               help="Formato: Año-Mes-Día (ej. 2023-05-15)")
        try:
            datetime.strptime(pub_date, '%Y-%m-%d')
            data['pub_date'] = pub_date
        except ValueError:
            st.error("Formato de fecha inválido. Por favor use YYYY-MM-DD")
            return None

        # Extraer volumen, número y páginas (con 0 por defecto)
        data['volume'] = extract_field(r'VI\s+-\s+(\S+)') or '0'
        data['number'] = extract_field(r'IP\s+-\s+(\S+)') or '0'
        data['pages'] = extract_field(r'PG\s+-\s+(\S+)') or '0'

        # Extraer información de la revista
        data['journal_full'] = extract_field(r'JT\s+-\s+(.*?)\n')
        data['journal_abbrev'] = extract_field(r'TA\s+-\s+(.*?)\n')
        if data['journal_full'] or data['journal_abbrev']:
            data['jcr_group'] = buscar_grupo_revista(data['journal_full'] or data['journal_abbrev'])

        # Extraer DOI
        doi_match = re.search(r'DO\s+-\s+(.*?)\n', content) or \
                   re.search(r'(?:LID|AID)\s+-\s+(.*?doi\.org/.*?)\s', content) or \
                   re.search(r'(?:LID|AID)\s+-\s+(10\.\S+)', content)
        if doi_match:
            data['doi'] = doi_match.group(1).strip()
        else:
            data['doi'] = ''  # Cadena vacía como valor por defecto

    except Exception as e:
        st.error(f"Error al procesar archivo .nbib: {str(e)}")
        logging.error(f"NBIB Parse Error: {str(e)}")
        return None

    return data

def sync_with_remote(economic_number):
    """Sincroniza el archivo local con el remoto para un número económico específico"""
    try:
        st.info("🔄 Sincronizando con el servidor remoto...")
        csv_filename = f"{CONFIG.CSV_PRODUCTOS_PREFIX}{economic_number}.csv"
        remote_filename = f"{CONFIG.REMOTE_PRODUCTOS_PREFIX}{economic_number}.csv"
        remote_path = os.path.join(CONFIG.REMOTE['DIR'], remote_filename)

        # Intenta descargar el archivo remoto
        download_success = SSHManager.download_remote_file(remote_path, csv_filename)

        if not download_success:
            # Si no existe el archivo remoto, crea uno local con estructura correcta
            columns = [
                'economic_number', 'nombramiento', 'departamento', 'participation_key', 'investigator_name',
                'corresponding_author', 'coauthors', 'article_title', 'year',
                'pub_date', 'volume', 'number', 'pages', 'journal_full',
                'journal_abbrev', 'doi', 'jcr_group', 'pmid', 'selected_keywords',
                'sni', 'sii', 'pdf_filename', 'estado'  # Nuevos campos añadidos
            ]

            # Verifica si el archivo local ya existe
            if not Path(csv_filename).exists():
                pd.DataFrame(columns=columns).to_csv(csv_filename, index=False)
                st.info("ℹ️ No se encontró archivo remoto. Se creó uno nuevo localmente con la estructura correcta.")
            else:
                # Si el archivo local existe pero está vacío o corrupto
                try:
                    df = pd.read_csv(csv_filename)
                    if df.empty:
                        pd.DataFrame(columns=columns).to_csv(csv_filename, index=False)
                except:
                    pd.DataFrame(columns=columns).to_csv(csv_filename, index=False)

            return False

        # Verifica que el archivo descargado no esté vacío
        try:
            df = pd.read_csv(csv_filename)
            if df.empty:
                st.warning("El archivo remoto está vacío")
        except pd.errors.EmptyDataError:
            st.warning("El archivo remoto está vacío o corrupto")
            columns = [
                'economic_number', 'nombramiento', 'departamento', 'participation_key', 'investigator_name',
                'corresponding_author', 'coauthors', 'article_title', 'year',
                'pub_date', 'volume', 'number', 'pages', 'journal_full',
                'journal_abbrev', 'doi', 'jcr_group', 'pmid', 'selected_keywords',
                'sni', 'sii', 'pdf_filename', 'estado'  # Nuevos campos añadidos
            ]
            pd.DataFrame(columns=columns).to_csv(csv_filename, index=False)
            return False

        st.success("✅ Sincronización con servidor remoto completada")
        return True

    except Exception as e:
        st.error(f"❌ Error en sincronización: {str(e)}")
        logging.error(f"Sync Error: {str(e)}")
        return False

def save_to_csv(data: dict, sni: str, sii: str):
    """Guarda los datos en el CSV local y remoto, eliminando registros marcados con 'X'"""
    try:
        economic_number = data['economic_number']
        csv_filename = f"{CONFIG.CSV_PRODUCTOS_PREFIX}{economic_number}.csv"

        with st.spinner("Sincronizando datos con el servidor..."):
            if not sync_with_remote(economic_number):
                st.warning("⚠️ Trabajando con copia local debido a problemas de conexión")

        columns = [
            'economic_number', 'nombramiento', 'sni', 'sii', 'departamento', 'participation_key', 'investigator_name',
            'corresponding_author', 'coauthors', 'article_title', 'year',
            'pub_date', 'volume', 'number', 'pages', 'journal_full',
            'journal_abbrev', 'doi', 'jcr_group', 'pmid', 'selected_keywords',
            'pdf_filename', 'estado'
        ]

        # Verificar si el archivo existe y tiene contenido válido
        if not Path(csv_filename).exists():
            df_existing = pd.DataFrame(columns=columns)
        else:
            try:
                df_existing = pd.read_csv(
                    csv_filename,
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

        # FILTRAR: Eliminar registros con estado 'X' antes de agregar el nuevo
        df_existing = df_existing[df_existing['estado'] != 'X']

        # Añadir los valores de SNI y SII al diccionario de datos
        data['sni'] = sni
        data['sii'] = sii

        # Preparar el nuevo registro
        df_new = pd.DataFrame([data])

        # Limpiar los datos del nuevo registro
        for col in df_new.columns:
            if df_new[col].dtype == object:
                df_new[col] = df_new[col].astype(str).str.replace(r'\r\n|\n|\r', ' ', regex=True).str.strip()

        # Combinar los datos existentes (ya filtrados) con los nuevos
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)

        # Asegurar que todas las columnas estén presentes
        for col in columns:
            if col not in df_combined.columns:
                df_combined[col] = ""

        # Reordenar columnas según el orden especificado
        df_combined = df_combined[columns]

        # Guardar localmente
        df_combined.to_csv(csv_filename, index=False, encoding='utf-8-sig')

        # Intentar subir al servidor remoto
        with st.spinner("Subiendo datos al servidor remoto..."):
            remote_filename = f"{CONFIG.REMOTE_PRODUCTOS_PREFIX}{economic_number}.csv"
            remote_path = os.path.join(CONFIG.REMOTE['DIR'], remote_filename)
            if SSHManager.upload_remote_file(csv_filename, remote_path):
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

def highlight_author(author: str, investigator_name: str) -> str:
    """Resalta el nombre del investigador principal"""
    if investigator_name and investigator_name.lower() == author.lower():
        return f"<span style='background-color: #90EE90;'>{author}</span>"
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
    st.write(f"**📅 Fecha de publicación:**  \n`{data['pub_date']}`")
    st.write(f"📚 Vol/Núm: {data['volume']}/{data['number']}")
    st.write(f"🔖 Páginas: {data['pages']}")
    st.write(f"🌐 DOI: {data['doi'] or 'No disponible'}")

def show_progress_bar(operation_name):
    """Muestra una barra de progreso para operaciones largas"""
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i in range(101):
        time.sleep(0.02)
        progress_bar.progress(i)
        status_text.text(f"{operation_name}... {i}%")
    
    progress_bar.empty()
    status_text.empty()

def main():
    st.set_page_config(
        page_title="Artículos en PubMed",
        page_icon="📊",
        layout="centered"
    )

    # Mostrar logo si existe
    if Path(CONFIG.LOGO_PATH).exists():
        logo = Image.open(CONFIG.LOGO_PATH)
        st.image(logo, width=200)

    st.title("📊 Artículos en PubMed")

    # Precargar datos de factores de impacto
    with st.spinner("Cargando base de datos de factores de impacto..."):
        _ = JournalCache()

    # Validación mejorada del número económico
    economic_number = st.text_input("🔢 Número económico del investigador (solo números, sin guiones o letras):").strip()

    if not economic_number:
        st.warning("Por favor ingrese un número económico. Si no cuenta con uno, ingrese: 123456")
        return

    if not economic_number.isdigit():
        st.error("El número económico debe contener solo dígitos (0-9)")
        return

    # Nuevo campo: nombramiento
    nombramiento = st.selectbox(
        "📋 Tipo de nombramiento:",
        options=["Ayudante de investigador", "Investigador", "Mando medio", "Médico", "Médico especialista", "Técnico"],
        index=0
    )

    # Capturar SNI y SII
    col1, col2 = st.columns(2)
    with col1:
        sni = st.selectbox("SNI", options=SNI_OPCIONES)
    with col2:
        sii = st.selectbox("SII", options=SII_OPCIONES)

    # Validar que se hayan seleccionado ambos campos
    if not sni or not sii:
        st.warning("Por favor seleccione tanto SNI como SII")
        return

    # Departamento en su propia línea
    departamento_seleccionado = st.selectbox(
        "🏢 Departamento de adscripción:",
        options=DEPARTAMENTOS_INCICH,
        index=0
    )

    # Inicializar la variable departamento
    departamento = ""

    # Mostrar campo de texto si se selecciona "Otro"
    if departamento_seleccionado == "Otro (especifique abajo)":
        departamento = st.text_input("Por favor, escriba el nombre completo de su departamento:")
        if not departamento:
            st.warning("Por favor ingrese el nombre del departamento")
            st.stop()
    else:
        departamento = departamento_seleccionado

    # Botón para sincronización manual
    if st.button("🔄 Sincronizar con servidor", key="sync_button"):
        with st.spinner("Conectando con el servidor remoto..."):
            if sync_with_remote(economic_number):
                st.session_state.synced = True
                st.rerun()

    # Sincronizar archivo remoto de productos
    remote_productos_filename = f"{CONFIG.REMOTE_PRODUCTOS_PREFIX}{economic_number}.csv"
    local_productos_filename = f"{CONFIG.CSV_PRODUCTOS_PREFIX}{economic_number}.csv"

    with st.spinner("Sincronizando archivo de productos..."):
        if not SSHManager.download_remote_file(remote_productos_filename, local_productos_filename):
            st.warning("No se pudo descargar el archivo remoto de productos. Trabajando con versión local.")

    # Cargar o inicializar el DataFrame
    if Path(local_productos_filename).exists():
        try:
            productos_df = pd.read_csv(local_productos_filename, encoding='utf-8-sig', dtype={'economic_number': str})
            productos_df['economic_number'] = productos_df['economic_number'].astype(str).str.strip()

            # Asegurar que los campos SNI y SII existan y tengan valores
            if 'sni' not in productos_df.columns:
                productos_df['sni'] = sni
            else:
                productos_df['sni'] = productos_df['sni'].fillna(sni)

            if 'sii' not in productos_df.columns:
                productos_df['sii'] = sii
            else:
                productos_df['sii'] = productos_df['sii'].fillna(sii)

            # Asegurar que el campo 'estado' exista
            if 'estado' not in productos_df.columns:
                productos_df['estado'] = 'A'
            else:
                productos_df['estado'] = productos_df['estado'].fillna('A').str.strip().replace('', 'A')

            # Asegurar que el campo 'pdf_filename' exista
            if 'pdf_filename' not in productos_df.columns:
                productos_df['pdf_filename'] = ''
        except Exception as e:
            st.error(f"Error al leer el archivo: {str(e)}")
            productos_df = pd.DataFrame(columns=[
                'economic_number', 'nombramiento', 'sni', 'sii', 'departamento', 'participation_key', 'investigator_name',
                'corresponding_author', 'coauthors', 'article_title', 'year',
                'pub_date', 'volume', 'number', 'pages', 'journal_full',
                'journal_abbrev', 'doi', 'jcr_group', 'pmid', 'selected_keywords',
                'pdf_filename', 'estado'
            ])
    else:
        productos_df = pd.DataFrame(columns=[
            'economic_number', 'nombramiento', 'sni', 'sii', 'departamento', 'participation_key', 'investigator_name',
            'corresponding_author', 'coauthors', 'article_title', 'year',
            'pub_date', 'volume', 'number', 'pages', 'journal_full',
            'journal_abbrev', 'doi', 'jcr_group', 'pmid', 'selected_keywords',
            'pdf_filename', 'estado'
        ])

    # Mostrar registros existentes si los hay
    if not productos_df.empty:
        st.subheader(f"📋 Registros existentes para {economic_number}")
        st.info("""
        **Instrucciones para eliminar registros:**

        1. **Localice** el registro que desea eliminar de la lista.
        2. **Edite el estado** haciendo doble clic sobre la letra 'A' en la columna "Estado".
        3. **Seleccione una opción**:
           - 'X' para marcar el registro para eliminación.
           - 'A' para mantener el registro activo.

        *Nota:* El botón **"Confirmar baja de registros"** aparecerá automáticamente cuando haya registros marcados con 'X'.
        Este botón le permitirá eliminar definitivamente los registros seleccionados.
        """)

        # Crear copia editable solo con las columnas necesarias
        columnas_mostrar = ['article_title', 'journal_full', 'pdf_filename', 'estado']
        edited_df = st.data_editor(
            productos_df[columnas_mostrar],
            column_config={
                "estado": st.column_config.SelectboxColumn(
                    "Estado",
                    options=["A", "X"],
                    required=True,
                    width="small"
                )
            },
            hide_index=True,
            use_container_width=True,
            key="editor_tabla"
        )

        # Verificar si hay cambios en los estados
        cambios = not edited_df.equals(productos_df[columnas_mostrar])
        registros_marcados = edited_df[edited_df['estado'] == 'X']

        # Mostrar botón solo si hay cambios y registros marcados con X
        if cambios and not registros_marcados.empty:
            if st.button("🗑️ Dar de baja registros 'X'", type="primary"):
                # Actualizar el estado en el DataFrame original
                productos_df['estado'] = edited_df['estado']

                # Filtrar solo los registros activos (estado 'A')
                productos_df = productos_df[productos_df['estado'] == 'A'].copy()

                # Guardar cambios en el archivo
                productos_df.to_csv(local_productos_filename, index=False, encoding='utf-8-sig')

                # Sincronizar con servidor remoto
                with st.spinner("Eliminando registros del servidor remoto..."):
                    upload_success = SSHManager.upload_remote_file(
                        local_productos_filename,
                        os.path.join(CONFIG.REMOTE['DIR'], remote_productos_filename)
                    )

                if upload_success:
                    st.success("✅ Registros eliminados exitosamente!")
                    st.balloons()
                    time.sleep(2)
                    st.rerun()
                else:
                    st.error("❌ Error al sincronizar con el servidor remoto")
    else:
        st.info("No se encontraron registros existentes para este número económico")

    # Preguntar si desea añadir nuevo registro
    st.divider()
    if st.radio("¿Desea añadir un nuevo registro?", ["No", "Sí"], index=0) == "Sí":
        st.subheader("📤 Subir artículo científico")
        st.info("""
        **Instrucciones para subir el artículo:**

        1. **Busque** su artículo en PubMed
        2. **Haga clic** en el botón 'Cite' (localizado a la derecha del título)
        3. **Seleccione** 'Download .nbib' en el menú desplegable
        4. **Suba** el archivo descargado en el siguiente campo

        *Nota:* El sistema procesará automáticamente la información del artículo al subir el archivo.
        """)
        uploaded_file = st.file_uploader("Seleccione el archivo .nbib", type=".nbib")

        # Sección para subir PDF del artículo
        st.subheader("📄 Documento completo del artículo")
        articulo_pdf = st.file_uploader(
            "Suba el documento completo del artículo en formato PDF:",
            type=["pdf"],
            accept_multiple_files=False
        )
        st.caption("Nota: El nombre del archivo se generará automáticamente con el formato ART.YYYY-MM-DD-HH-MM.economic_number.pdf")

        if uploaded_file:
            try:
                content = uploaded_file.read().decode("utf-8")
                data = parse_nbib_file(content)

                if data:
                    st.subheader("📝 Información extraída")
                    st.info(data['article_title'])

                    # Añadir campos adicionales al diccionario de datos
                    data['nombramiento'] = nombramiento
                    data['sni'] = sni
                    data['sii'] = sii
                    data['departamento'] = departamento

                    selected_categories = st.multiselect(
                        "Seleccione al menos 1 línea de investigación:",
                        options=list(KEYWORD_CATEGORIES.keys()),
                        default=extract_keywords(data['article_title'])[:1],
                        max_selections=3
                    )

                    # Validación de palabras clave
                    if len(selected_categories) < 1:
                        st.error("Debe seleccionar al menos 1 línea de investigación")
                        st.stop()

                    data['selected_keywords'] = selected_categories

                    # Selección del investigador principal
                    authors_list = []
                    if data['corresponding_author']:
                        authors_list.append(data['corresponding_author'])
                    if data['coauthors']:
                        authors_list.extend(data['coauthors'].split("; "))

                    investigator_name = st.selectbox("Seleccione su nombre:", authors_list)
                    data['investigator_name'] = investigator_name
                    data['economic_number'] = economic_number
                    data['participation_key'] = "CA" if investigator_name == data['corresponding_author'] else f"{authors_list.index(investigator_name)}C"

                    # Procesar el PDF si se subió
                    pdf_filename = ""
                    if articulo_pdf is not None:
                        if not articulo_pdf.name.lower().endswith('.pdf'):
                            st.error("El archivo debe ser un PDF válido")
                            st.stop()

                        try:
                            timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
                            pdf_filename = f"ART.{timestamp}.{economic_number}.pdf"
                            pdf_remote_path = os.path.join(CONFIG.REMOTE['DIR'], pdf_filename)

                            # Verificar espacio en disco
                            try:
                                stat = os.statvfs('/')
                                if stat.f_bavail * stat.f_frsize < 10 * 1024 * 1024:  # 10MB mínimo
                                    st.error("Espacio en disco insuficiente para guardar el PDF")
                                    st.stop()
                            except:
                                pass  # Si no puede verificar, continuar

                            # Guardar temporalmente el archivo localmente
                            with open(pdf_filename, "wb") as f:
                                f.write(articulo_pdf.getbuffer())

                            # Subir al servidor remoto
                            with st.spinner("Subiendo documento del artículo..."):
                                upload_success = SSHManager.upload_remote_file(pdf_filename, pdf_remote_path)

                            if upload_success:
                                st.success(f"✅ Documento subido correctamente: {pdf_filename}")
                            else:
                                st.error("Error al subir el documento del artículo. El registro se guardará sin el documento.")
                        except Exception as e:
                            st.error(f"Error al procesar el documento: {str(e)}")
                            logging.error(f"PDF Upload Error: {str(e)}")
                            pdf_filename = ""  # Asegurar que no quede un nombre inválido
                    else:
                        st.warning("No se subió ningún documento para este artículo")

                    # Asignar el nombre del archivo PDF al registro
                    data['pdf_filename'] = pdf_filename

                    if pdf_filename:
                        st.info(f"Archivo PDF asociado: {pdf_filename}")

                    if st.button("💾 Guardar registro", type="primary"):
                        with st.spinner("Guardando datos..."):
                            if save_to_csv(data, sni, sii):
                                st.balloons()
                                st.success("✅ Registro guardado exitosamente!")

                                # Intentar subir el archivo actualizado al servidor remoto
                                with st.spinner("Sincronizando con servidor remoto..."):
                                    upload_success = SSHManager.upload_remote_file(
                                        local_productos_filename,
                                        os.path.join(CONFIG.REMOTE['DIR'], remote_productos_filename)
                                    )
                                    if not upload_success:
                                        st.warning("No se pudo sincronizar con el servidor remoto. Los datos se guardaron localmente.")

            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
                logging.error(f"Main App Error: {str(e)}")

if __name__ == "__main__":
    main()

