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
        "iamcest", "iamnest", "angina inestabile", "troponina elevada",
        "oclusión coronaria", "elevación st", "depresión st"
    ],
    "Valvulopatías": [
        "valvulopatía", "estenosis aórtica", "insuficiencia aórtica",
        "stenosis mitral", "insuficiencia mitral", "prolapso mitral",
        "tavi", "taavi", "anillo mitral", "reemplazo valvular"
    ],
}

# ====================
# OPCIONES DISPONIBLES
# ====================
TIPOS_PARTICIPACION = ["Autor único", "Coautor", "Editor"]
FORMATOS_LIBRO = ["Impreso", "Audiolibro", "Digital"]
PAISES_PRINCIPALES = [
    "México", "Estados Unidos", "España", "Argentina", "Colombia", 
    "Chile", "Perú", "Brasil", "Reino Unido", "Alemania", 
    "Francia", "Italia", "China", "Japón", "Otro"
]
IDIOMAS_PRINCIPALES = [
    "Español", "Inglés", "Francés", "Alemán", "Portugués", 
    "Italiano", "Chino", "Japonés", "Ruso", "Otro"
]

DEPARTAMENTOS_INCICH = [
    "Bioquímica",
    "Biología Molecular",
    "Biomedicina Cardiovascular",
    "Consulta Externa (Dermatología, Endocrinología, etc.)",
    "Departamento de Enseñanza de Enfermería (DEE)",
    "Endocrinología",
    "Farmacología",
    "Fisiología",
    "Fisiopatología Cardio-Renal",
    "Fisiotepatología Cardiorenal",
    "Inmunología",
    "Instrumentación Electromecánica",
    "Oficina de Apoyo Sistemático para la Investigación Superior (OASIS)",
    "Unidad de Investigación UNAM-INC"
]

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
        self.CSV_PREFIX = "capitulos_"  # Cambiado a "capitulos_" en lugar de "capitulos_"
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
                            'economic_number', 'departamento', 'autor_principal', 'tipo_participacion', 
                            'titulo_libro', 'titulo_capitulo', 'editorial', 'coautores_secundarios', 
                            'year', 'pub_date', 'isbn_issn', 'numero_edicion', 'paginas', 
                            'paises_distribucion', 'idiomas_disponibles', 'formatos_disponibles', 
                            'selected_keywords', 'estado'
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
    """Resalta el nombre del investigador principal"""
    if investigator_name and investigator_name.lower() == author.lower():
        return f"<span style='background-color: {CONFIG.HIGHLIGHT_COLOR};'>{author}</span>"
    return author

def sync_with_remote(economic_number):
    """Sincroniza el archivo local con el remoto para un número económico específico"""
    try:
        st.info("🔄 Sincronizando con el servidor remoto...")
        csv_filename = f"{CONFIG.CSV_PREFIX}{economic_number}.csv"
        remote_filename = f"{CONFIG.CSV_PREFIX}{economic_number}.csv"
        remote_path = os.path.join(CONFIG.REMOTE['DIR'], remote_filename)

        # Intenta descargar el archivo remoto
        download_success = SSHManager.download_remote_file(remote_path, csv_filename)

        if not download_success:
            # Si no existe el archivo remoto, crea uno local con estructura correcta
            columns = [
                'economic_number', 'departamento', 'autor_principal', 'tipo_participacion', 
                'titulo_libro', 'titulo_capitulo', 'editorial', 'coautores_secundarios', 
                'year', 'pub_date', 'isbn_issn', 'numero_edicion', 'paginas', 
                'paises_distribucion', 'idiomas_disponibles', 'formatos_disponibles', 
                'selected_keywords', 'estado'
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
                'economic_number', 'departamento', 'autor_principal', 'tipo_participacion', 
                'titulo_libro', 'titulo_capitulo', 'editorial', 'coautores_secundarios', 
                'year', 'pub_date', 'isbn_issn', 'numero_edicion', 'paginas', 
                'paises_distribucion', 'idiomas_disponibles', 'formatos_disponibles', 
                'selected_keywords', 'estado'
            ]
            pd.DataFrame(columns=columns).to_csv(csv_filename, index=False)
            return False

        st.success("✅ Sincronización con servidor remoto completada")
        return True

    except Exception as e:
        st.error(f"❌ Error en sincronización: {str(e)}")
        logging.error(f"Sync Error: {str(e)}")
        return False

def save_to_csv(data: dict):
    """Guarda los datos en el CSV local y remoto, eliminando registros con estado 'X'"""
    try:
        economic_number = data['economic_number']
        csv_filename = f"{CONFIG.CSV_PREFIX}{economic_number}.csv"
        
        with st.spinner("Sincronizando datos con el servidor..."):
            if not sync_with_remote(economic_number):
                st.warning("⚠️ Trabajando con copia local debido a problemas de conexión")

        columns = [
            'economic_number', 'departamento', 'autor_principal', 'tipo_participacion', 
            'titulo_libro', 'titulo_capitulo', 'editorial', 'coautores_secundarios', 
            'year', 'pub_date', 'isbn_issn', 'numero_edicion', 'paginas', 
            'paises_distribucion', 'idiomas_disponibles', 'formatos_disponibles', 
            'selected_keywords', 'estado'
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
                # Eliminar registros con estado 'X'
                df_existing = df_existing[df_existing['estado'] != 'X'].copy()
                
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

        # Combinar los datos existentes (sin los 'X') con los nuevos
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)

        # Asegurar que todas las columnas estén presentes
        for col in columns:
            if col not in df_combined.columns:
                df_combined[col] = ""

        # Reordenar columnas
        df_combined = df_combined[columns]

        # Guardar localmente
        df_combined.to_csv(csv_filename, index=False, encoding='utf-8-sig')

        # Intentar subir al servidor remoto
        with st.spinner("Subiendo datos al servidor remoto..."):
            remote_filename = f"{CONFIG.CSV_PREFIX}{economic_number}.csv"
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

def display_author_info(data, investigator_name):
    """Muestra información de autores con formato"""
    st.markdown("**Autores**")
    st.markdown(f"📌 Autor principal: {highlight_author(data['autor_principal'], investigator_name)}", unsafe_allow_html=True)
    if data['coautores_secundarios']:
        st.markdown("👥 Coautores del capítulo:")
        for author in data['coautores_secundarios'].split("; "):
            st.markdown(f"- {highlight_author(author, investigator_name)}", unsafe_allow_html=True)

def display_publication_info(data):
    """Muestra detalles del libro"""
    st.markdown("**Detalles de publicación**")
    st.write(f"📅 Año: {data['year']}")
    st.write(f"**📅 Fecha de publicación:**  \n`{data['pub_date']}`")
    st.write(f"📚 Páginas: {data['paginas']}")
    st.write(f"🔖 ISBN/ISSN: {data['isbn_issn']}")
    st.write(f"🏢 Editorial: {data['editorial']}")
    st.write(f"#️⃣ Edición: {data['numero_edicion']}")
    
    st.markdown("**Distribución**")
    st.write(f"🌍 Países: {data['paises_distribucion']}")
    st.write(f"🌐 Idiomas: {data['idiomas_disponibles']}")
    st.write(f"📖 Formatos: {data['formatos_disponibles']}")

def main():
    st.set_page_config(
        page_title="Captura Capítulos",
        page_icon="📚",
        layout="centered"
    )

    # Mostrar logo si existe
    if Path(CONFIG.LOGO_PATH).exists():
        logo = Image.open(CONFIG.LOGO_PATH)
        st.image(logo, width=200)

    st.title("📚 Captura Capítulos")

    # Validación del número económico
    economic_number = st.text_input("🔢 Número económico del investigador (solo dígitos):").strip()

    if not economic_number:
        st.warning("Por favor ingrese un número económico")
        return

    if not economic_number.isdigit():
        st.error("El número económico debe contener solo dígitos (0-9)")
        return

    # Sincronización inicial con el servidor remoto
    with st.spinner("Conectando con el servidor remoto..."):
        sync_with_remote(economic_number)

    csv_filename = f"{CONFIG.CSV_PREFIX}{economic_number}.csv"

    # Cargar o inicializar el DataFrame
    if Path(csv_filename).exists():
        try:
            capitulos_df = pd.read_csv(csv_filename, encoding='utf-8-sig', dtype={'economic_number': str})
            capitulos_df['economic_number'] = capitulos_df['economic_number'].astype(str).str.strip()

            # Asegurar que el campo 'estado' exista
            if 'estado' not in capitulos_df.columns:
                capitulos_df['estado'] = 'A'
            else:
                # Limpiar valores vacíos/nulos en el campo estado
                capitulos_df['estado'] = capitulos_df['estado'].fillna('A').str.strip().replace('', 'A')
        except Exception as e:
            st.error(f"Error al leer el archivo: {str(e)}")
            capitulos_df = pd.DataFrame(columns=[
                'economic_number', 'departamento', 'autor_principal', 'tipo_participacion',
                'titulo_libro', 'titulo_capitulo', 'editorial', 'coautores_secundarios',
                'year', 'pub_date', 'isbn_issn', 'numero_edicion', 'paginas',
                'paises_distribucion', 'idiomas_disponibles', 'formatos_disponibles',
                'selected_keywords', 'estado'
            ])
    else:
        capitulos_df = pd.DataFrame(columns=[
            'economic_number', 'departamento', 'autor_principal', 'tipo_participacion',
            'titulo_libro', 'titulo_capitulo', 'editorial', 'coautores_secundarios',
            'year', 'pub_date', 'isbn_issn', 'numero_edicion', 'paginas',
            'paises_distribucion', 'idiomas_disponibles', 'formatos_disponibles',
            'selected_keywords', 'estado'
        ])

    # Mostrar registros existentes si los hay
    if not capitulos_df.empty:
        st.subheader(f"📋 Capítulos registrados para {economic_number}")
        st.info("""
        **Instrucciones:**
        - Marque con 'X' los registros que desee dar de baja
        - Todos los demás deben mantenerse con 'A' (Activo)
        """)

        # Crear copia editable solo con las columnas necesarias
        columnas_mostrar = ['titulo_libro', 'titulo_capitulo', 'tipo_participacion', 'year', 'estado']
        edited_df = capitulos_df[columnas_mostrar].copy()

        # Mostrar editor de tabla
        edited_df = st.data_editor(
            edited_df,
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

        # Verificar cambios en los estados
        if not edited_df.equals(capitulos_df[columnas_mostrar]):
            # Actualizar el estado en el DataFrame original
            capitulos_df['estado'] = edited_df['estado']

            # Identificar registros marcados para borrar
            registros_a_borrar = capitulos_df[capitulos_df['estado'] == 'X']

            if not registros_a_borrar.empty:
                st.warning(f"⚠️ Tiene {len(registros_a_borrar)} registro(s) marcado(s) para dar de baja")

                col1, col2 = st.columns(2)
                with col1:
                    if st.button("🗑️ Confirmar baja de registros", type="primary"):
                        # Filtrar solo los registros activos (estado 'A')
                        capitulos_df = capitulos_df[capitulos_df['estado'] == 'A'].copy()

                        # Guardar cambios en el archivo
                        capitulos_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')

                        # Sincronizar con servidor remoto
                        with st.spinner("Guardando cambios..."):
                            remote_filename = f"{CONFIG.CSV_PREFIX}{economic_number}.csv"
                            remote_path = os.path.join(CONFIG.REMOTE['DIR'], remote_filename)
                            upload_success = SSHManager.upload_remote_file(csv_filename, remote_path)

                        if upload_success:
                            st.success("✅ Registros eliminados exitosamente del archivo!")
                            st.balloons()
                            time.sleep(2)
                            st.rerun()
                        else:
                            st.error("❌ Error al sincronizar con el servidor remoto")

                with col2:
                    if st.button("↩️ Cancelar operación"):
                        st.info("Operación cancelada - No se realizaron cambios")
                        st.rerun()

    # Preguntar si desea añadir nuevo registro
    st.divider()
    if st.radio("¿Desea registrar un nuevo capítulo?", ["No", "Sí"], index=0) == "Sí":
        # Formulario para nuevo registro
        st.subheader("📝 Nuevo registro de capítulo")

        with st.form("nuevo_capitulo", clear_on_submit=True):
            departamento = st.selectbox(
                "🏢 Departamento de adscripción:",
                options=DEPARTAMENTOS_INCICH,
                index=0
            )
            autor_principal = st.text_input("👤 Autor principal del libro:")
            tipo_participacion = st.selectbox(
                "🎭 Tipo de participación:",
                options=TIPOS_PARTICIPACION,
                index=0
            )
            titulo_libro = st.text_area("📖 Título del libro:")
            titulo_capitulo = st.text_area("📄 Título del capítulo:")
            editorial = st.text_input("🏢 Editorial:")
            coautores_secundarios = st.text_area("👥 Coautores (separados por ';'):")

            # Detalles de publicación
            st.subheader("📅 Detalles de publicación")
            col1, col2 = st.columns(2)
            with col1:
                year = st.text_input("Año de publicación:")
            with col2:
                pub_date = st.text_input("Fecha exacta [YYYY-MM-DD]:", placeholder="AAAA-MM-DD")

            col3, col4 = st.columns(2)
            with col3:
                isbn_issn = st.text_input("🔖 ISBN/ISSN:")
            with col4:
                numero_edicion = st.text_input("#️⃣ Número de edición:")

            paginas = st.text_input("📚 Número de páginas del capítulo:")

            # Distribución
            st.subheader("🌍 Distribución")
            paises_distribucion = st.multiselect(
                "Países de distribución principales:",
                options=PAISES_PRINCIPALES
            )
            idiomas_disponibles = st.multiselect(
                "Idiomas disponibles:",
                options=IDIOMAS_PRINCIPALES
            )
            formatos_disponibles = st.multiselect(
                "Formatos disponibles:",
                options=FORMATOS_LIBRO
            )

            # Palabras clave
            st.subheader("🔍 Palabras clave")
            selected_categories = st.multiselect(
                f"Seleccione hasta {CONFIG.MAX_KEYWORDS} palabras clave:",
                options=list(KEYWORD_CATEGORIES.keys()),
                max_selections=CONFIG.MAX_KEYWORDS
            )

            if st.form_submit_button("💾 Guardar nuevo registro"):
                nuevo_registro = {
                    'economic_number': economic_number,
                    'departamento': departamento,
                    'autor_principal': autor_principal,
                    'tipo_participacion': tipo_participacion,
                    'titulo_libro': titulo_libro,
                    'titulo_capitulo': titulo_capitulo,
                    'editorial': editorial,
                    'coautores_secundarios': coautores_secundarios,
                    'year': year,
                    'pub_date': pub_date if pub_date else year,
                    'isbn_issn': isbn_issn,
                    'numero_edicion': numero_edicion,
                    'paginas': paginas,
                    'paises_distribucion': ', '.join(paises_distribucion),
                    'idiomas_disponibles': ', '.join(idiomas_disponibles),
                    'formatos_disponibles': ', '.join(formatos_disponibles),
                    'selected_keywords': str(selected_categories),
                    'estado': 'A'
                }

                if save_to_csv(nuevo_registro):
                    st.success("✅ Registro guardado exitosamente!")
                    st.balloons()
                    time.sleep(2)
                    st.rerun()

if __name__ == "__main__":
    main()
