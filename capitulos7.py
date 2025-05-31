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
    filename='capitulos.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Diccionario de categorías de palabras clave
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
    "Ciencias Básicas": ["matemáticas", "actuaría", "computación"],
    "Ciencias Naturales": ["biología", "química", "física", "geología"],
    "Ciencias Sociales": ["sociología", "psicología", "economía", "antropología"],
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
    "Humanidades": ["historia", "filosofía", "literatura", "arte"],
    "Inmunología": ["autoinmunidad", "inmunodeficiencia", "alergias", "linfocitos"],
    "Inmunoterapia": ["inmunoterapia", "terapia car-t", "checkpoint inmunológico"],
    "Insuficiencia Cardíaca": [
        "insuficiencia cardíaca", "ic", "fallo cardíaco", "disfunción ventricular",
        "icfe", "icfd", "fracción de eyección reducida", "fracción de eyección preservada",
        "nyha clase ii", "nyha clase iii", "edema pulmonar", "congestión venosa"
    ],
    "Ingeniería": ["robótica", "inteligencia artificial", "materiales", "energías renovables"],
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

# Lista de países para distribución
PAISES = [
    "Alemania", "Argentina", "Australia", "Austria", "Bélgica", "Bolivia", "Brasil", "Canadá",
    "Chile", "China", "Colombia", "Corea del Sur", "Costa Rica", "Dinamarca", "Ecuador",
    "El Salvador", "España", "Estados Unidos", "Finlandia", "Francia", "Grecia", "Guatemala",
    "Holanda", "Honduras", "India", "Inglaterra", "Irlanda", "Israel", "Italia", "Japón",
    "México", "Noruega", "Nueva Zelanda", "Panamá", "Paraguay", "Perú", "Polonia", "Portugal",
    "Puerto Rico", "Reino Unido", "República Checa", "Rusia", "Singapur", "Sudáfrica",
    "Suecia", "Suiza", "Uruguay", "Venezuela", "Otro"
]

# Lista de idiomas principales
IDIOMAS_PRINCIPALES = [
    "Español", "Inglés", "Francés", "Alemán", "Portugués", "Italiano", 
    "Chino", "Japonés", "Ruso", "Árabe", "Otro"
]

# Opciones para participación
PARTICIPATION_OPTIONS = {
    "Coautor": "CO"
}

# Formatos disponibles
FORMATOS_DISPONIBLES = ["Impreso", "Digital", "Audiolibro"]

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
        self.CSV_FILENAME = "capitulos.csv"
        self.REMOTE_CAPITULOS_FILE = st.secrets.get("remote_capitulos")
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
                            'nombre_capitulo', 'titulo_libro', 'editorial', 'autores', 'anio_publicacion',
                            'pub_date', 'isbn_issn', 'numero_edicion', 'numero_paginas',
                            'paises_distribucion', 'idiomas_disponibles', 'formatos_disponibles',
                            'keywords', 'economic_number', 'participation_key', 'investigator_name'
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
def sync_with_remote():
    """Sincroniza el archivo local con el remoto"""
    try:
        st.info("🔄 Sincronizando con el servidor remoto...")
        remote_path = os.path.join(CONFIG.REMOTE['DIR'], CONFIG.REMOTE_CAPITULOS_FILE)

        # Intenta descargar el archivo remoto
        download_success = SSHManager.download_remote_file(remote_path, CONFIG.CSV_FILENAME)

        if not download_success:
            # Si no existe el archivo remoto, crea uno local con estructura correcta
            columns = [
                'nombre_capitulo', 'titulo_libro', 'editorial', 'autores', 'anio_publicacion',
                'pub_date', 'isbn_issn', 'numero_edicion', 'numero_paginas',
                'paises_distribucion', 'idiomas_disponibles', 'formatos_disponibles',
                'keywords', 'economic_number', 'participation_key', 'investigator_name'
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
                'nombre_capitulo', 'titulo_libro', 'editorial', 'autores', 'anio_publicacion',
                'pub_date', 'isbn_issn', 'numero_edicion', 'numero_paginas',
                'paises_distribucion', 'idiomas_disponibles', 'formatos_disponibles',
                'keywords', 'economic_number', 'participation_key', 'investigator_name'
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
            'nombre_capitulo', 'titulo_libro', 'editorial', 'autores', 'anio_publicacion',
            'pub_date', 'isbn_issn', 'numero_edicion', 'numero_paginas',
            'paises_distribucion', 'idiomas_disponibles', 'formatos_disponibles',
            'keywords', 'economic_number', 'participation_key', 'investigator_name'
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
            remote_path = os.path.join(CONFIG.REMOTE['DIR'], CONFIG.REMOTE_CAPITULOS_FILE)
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
    # Configuración responsive
    st.set_page_config(
        page_title="Captura de Capítulos de Libros",
        page_icon="📚",
        layout="centered"
    )
    
    # Logo y título
    col1, col2 = st.columns([1, 3])
    with col1:
        st.image("escudo_COLOR.jpg", width=80)
    with col2:
        st.title("Captura Capítulos")

    # Sincronización inicial
    with st.spinner("Conectando con el servidor remoto..."):
        if not sync_with_remote():
            st.warning("""
            ⚠️ No se pudo conectar con el servidor remoto. 
            Trabajando en modo local. Los datos se sincronizarán cuando se restablezca la conexión.
            """)

    # Sección 1: Información del Investigador
    st.header("👤 Información del Investigador")
    economic_number = st.text_input("Número económico:", help="Ingrese su número económico identificador")
    investigator_name = st.text_input("Nombre completo:", help="Ingrese su nombre completo (apellidos y nombres)")
    
    # Selector de participación
    participation_key = st.selectbox(
        "Tipo de participación:",
        options=list(PARTICIPATION_OPTIONS.keys()),
        help="La única opción de rol en esta publicación es coautor)"
    )

    # Sección 2: Información del Capítulo y Libro
    st.header("📖 Información del Capítulo y Libro")
    nombre_capitulo = st.text_area("Título del capítulo:", height=100, help="Ingrese el título del capítulo")
    titulo_libro = st.text_area("Título del libro:", height=100, help="Ingrese el título completo del libro")
    editorial = st.text_input("Editorial:", help="Nombre de la editorial que publicó el libro")
    autores = st.text_area("Autores del Capítulo (separados por ';'):", height=100,
                         help="Lista de autores del capítulo separados por punto y coma (;)")

    # Sección 3: Detalles de Publicación
    st.header("📅 Detalles de Publicación")
    col1, col2 = st.columns(2)
    with col1:
        anio_publicacion = st.text_input("Año de publicación:", help="Año en que se publicó el libro")
    with col2:
        pub_date = st.text_input("Fecha (YYYY-MM-DD):", help="Fecha exacta de publicación en formato AAAA-MM-DD")
    
    col1, col2 = st.columns(2)
    with col1:
        isbn_issn = st.text_input("ISBN/ISSN:", help="Identificador único del libro o revista")
    with col2:
        numero_edicion = st.text_input("N° Edición:", help="Número de edición del libro, ejemplo primera edición 1, segunda edición 2")
    
    numero_paginas = st.text_input("N° de páginas del capítulo:", help="Cantidad de páginas del capítulo")

    # Sección 4: Distribución
    st.header("🌍 Distribución")
    paises_distribucion = st.multiselect(
        "Países de distribución:",
        options=PAISES,
        default=["México"],
        help="Seleccione los países donde está disponible el libro"
    )
    
    # Campo de idiomas como multiselect
    idiomas_disponibles = st.multiselect(
        "Idiomas disponibles:",
        options=IDIOMAS_PRINCIPALES,
        default=["Español"],
        help="Seleccione los idiomas en que está disponible el libro"
    )

    # Selector de formatos
    formatos_disponibles = st.multiselect(
        "Formatos disponibles:",
        options=FORMATOS_DISPONIBLES,
        default=["Impreso"],
        help="Seleccione los formatos en que está disponible el libro"
    )

    # Sección 5: Palabras clave
    st.header("🔑 Palabras Clave")
    keywords = st.multiselect(
        "Seleccione exactamente 3 palabras clave:",
        options=list(KEYWORD_CATEGORIES.keys()),
        help="Seleccione 3 categorías que describan el contenido del capítulo"
    )

    # Validación y guardado
    if st.button("💾 Guardar Registro", type="primary", use_container_width=True):
        errors = []
        if not nombre_capitulo: errors.append("Título del capítulo")
        if not titulo_libro: errors.append("Título del libro")
        if not editorial: errors.append("Editorial")
        if not autores: errors.append("Autores")
        if not economic_number: errors.append("Número económico")
        if not investigator_name: errors.append("Nombre investigador")
        if len(keywords) != 3: errors.append("Se requieren exactamente 3 palabras clave")
        
        if errors:
            st.error(f"Faltan campos obligatorios: {', '.join(errors)}")
        else:
            data = {
                'nombre_capitulo': nombre_capitulo,
                'titulo_libro': titulo_libro,
                'editorial': editorial,
                'autores': autores,
                'anio_publicacion': anio_publicacion,
                'pub_date': pub_date,
                'isbn_issn': isbn_issn,
                'numero_edicion': numero_edicion,
                'numero_paginas': numero_paginas,
                'paises_distribucion': ', '.join(paises_distribucion),
                'idiomas_disponibles': ', '.join(idiomas_disponibles),
                'formatos_disponibles': ', '.join(formatos_disponibles),
                'keywords': ', '.join(keywords),
                'economic_number': economic_number,
                'participation_key': PARTICIPATION_OPTIONS[participation_key],
                'investigator_name': investigator_name
            }
            
            if save_to_csv(data):
                st.success("¡Registro guardado exitosamente!")
                st.balloons()
                st.json(data, expanded=False)

if __name__ == "__main__":
    main()
