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
    filename='manual.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ====================
# DEPARTAMENTOS INCICH (actualizado desde productividad28.py)
# ====================
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
    "Fisiotepatologíay  Cardiorenal",
    "Inmunología",
    "Instrumentación Electromecánica",
    "Oficina de Apoyo Sistemático para la Investigación Superior (OASIS)",
    "Unidad de Investigación UNAM-INC"
]

# ====================
# OPCIONES SNI Y SII (actualizado desde productividad28.py)
# ====================
SNI_OPCIONES = ["C", "I", "II", "III", "Emérito"]
SII_OPCIONES = ["A", "B", "C", "D", "E", "F", "Emérito"]

# ====================
# OPCIONES NOMBRAMIENTO (actualizado desde productividad28.py)
# ====================
NOMBRAMIENTO_OPCIONES = [
    "Ayudante de investigador",
    "Investigador",
    "Mando medio",
    "Médico",
    "Médico especialista",
    "Técnico"
]

# ====================
# CATEGORÍAS DE KEYWORDS (actualizado desde productividad28.py)
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
        self.REMOTE = {
            'HOST': st.secrets["sftp"]["host"],
            'USER': st.secrets["sftp"]["user"],
            'PASSWORD': st.secrets["sftp"]["password"],
            'PORT': st.secrets["sftp"]["port"],
            'DIR': st.secrets["sftp"]["dir"]
        }
        
        # Prefijos
        self.CSV_PREFIX = st.secrets["prefixes"]["manual"]
        
        # Otros parámetros
        self.TIMEOUT_SECONDS = 30
        self.MAX_KEYWORDS = 3
        self.HIGHLIGHT_COLOR = "#90EE90"
        self.LOGO_PATH = "escudo_COLOR.jpg"

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
                            'economic_number', 'nombramiento', 'sni', 'sii', 'departamento', 'participation_key', 'investigator_name',
                            'corresponding_author', 'coauthors', 'article_title', 'year',
                            'pub_date', 'volume', 'number', 'pages', 'journal_full',
                            'journal_abbrev', 'doi', 'jcr_group', 'pmid', 'selected_keywords',
                            'estado'
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
                'economic_number', 'nombramiento', 'sni', 'sii', 'departamento', 'participation_key', 'investigator_name',
                'corresponding_author', 'coauthors', 'article_title', 'year',
                'pub_date', 'volume', 'number', 'pages', 'journal_full',
                'journal_abbrev', 'doi', 'jcr_group', 'pmid', 'selected_keywords',
                'estado'
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
                'economic_number', 'nombramiento', 'sni', 'sii', 'departamento', 'participation_key', 'investigator_name',
                'corresponding_author', 'coauthors', 'article_title', 'year',
                'pub_date', 'volume', 'number', 'pages', 'journal_full',
                'journal_abbrev', 'doi', 'jcr_group', 'pmid', 'selected_keywords',
                'estado'
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
            'economic_number', 'nombramiento', 'sni', 'sii', 'departamento', 'participation_key', 'investigator_name',
            'corresponding_author', 'coauthors', 'article_title', 'year',
            'pub_date', 'volume', 'number', 'pages', 'journal_full',
            'journal_abbrev', 'doi', 'jcr_group', 'pmid', 'selected_keywords',
            'estado'
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

def main():
    st.set_page_config(
        page_title="Artículos no en PubMed",
        page_icon="📝",
        layout="centered"
    )

    # Mostrar logo si existe
    if Path(CONFIG.LOGO_PATH).exists():
        logo = Image.open(CONFIG.LOGO_PATH)
        st.image(logo, width=200)

    st.title("📝 Artículos no en PubMed")

    # Validación del número económico
    economic_number = st.text_input("🔢 Número económico del investigador (solo dígitos):").strip()

    if not economic_number:
        st.warning("Por favor ingrese un número económico")
        return

    if not economic_number.isdigit():
        st.error("El número económico debe contener solo dígitos (0-9)")
        return

    # Campo de nombramiento
    nombramiento = st.selectbox(
        "👔 Nombramiento:",
        options=NOMBRAMIENTO_OPCIONES,
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

    # Sincronización inicial con el servidor remoto
    with st.spinner("Conectando con el servidor remoto..."):
        sync_with_remote(economic_number)

    csv_filename = f"{CONFIG.CSV_PREFIX}{economic_number}.csv"

    # Cargar o inicializar el DataFrame
    if Path(csv_filename).exists():
        try:
            manual_df = pd.read_csv(csv_filename, encoding='utf-8-sig', dtype={'economic_number': str})
            manual_df['economic_number'] = manual_df['economic_number'].astype(str).str.strip()

            # Asegurar que los campos SNI, SII y nombramiento existan y tengan valores
            if 'sni' not in manual_df.columns:
                manual_df['sni'] = sni
            else:
                manual_df['sni'] = manual_df['sni'].fillna(sni)

            if 'sii' not in manual_df.columns:
                manual_df['sii'] = sii
            else:
                manual_df['sii'] = manual_df['sii'].fillna(sii)

            if 'nombramiento' not in manual_df.columns:
                manual_df['nombramiento'] = nombramiento
            else:
                manual_df['nombramiento'] = manual_df['nombramiento'].fillna(nombramiento)

            # Asegurar que el campo 'estado' exista
            if 'estado' not in manual_df.columns:
                manual_df['estado'] = 'A'
            else:
                # Limpiar valores vacíos/nulos en el campo estado
                manual_df['estado'] = manual_df['estado'].fillna('A').str.strip().replace('', 'A')
        except Exception as e:
            st.error(f"Error al leer el archivo: {str(e)}")
            manual_df = pd.DataFrame(columns=[
                'economic_number', 'nombramiento', 'sni', 'sii', 'departamento', 'participation_key', 'investigator_name',
                'corresponding_author', 'coauthors', 'article_title', 'year',
                'pub_date', 'volume', 'number', 'pages', 'journal_full',
                'journal_abbrev', 'doi', 'jcr_group', 'pmid', 'selected_keywords',
                'estado'
            ])
    else:
        manual_df = pd.DataFrame(columns=[
            'economic_number', 'nombramiento', 'sni', 'sii', 'departamento', 'participation_key', 'investigator_name',
            'corresponding_author', 'coauthors', 'article_title', 'year',
            'pub_date', 'volume', 'number', 'pages', 'journal_full',
            'journal_abbrev', 'doi', 'jcr_group', 'pmid', 'selected_keywords',
            'estado'
        ])

    # Mostrar registros existentes si los hay
    if not manual_df.empty:
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
        columnas_mostrar = ['article_title', 'journal_full', 'estado']
        edited_df = manual_df[columnas_mostrar].copy()

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
        if not edited_df.equals(manual_df[columnas_mostrar]):
            # Actualizar el estado en el DataFrame original
            manual_df['estado'] = edited_df['estado']

            # Identificar registros marcados para borrar
            registros_a_borrar = manual_df[manual_df['estado'] == 'X']

            if not registros_a_borrar.empty:
                st.warning(f"⚠️ Tiene {len(registros_a_borrar)} registro(s) marcado(s) para dar de baja")

            if st.button("🗑️ Confirmar baja de registros", type="primary"):
                # Filtrar solo los registros activos (estado 'A')
                manual_df = manual_df[manual_df['estado'] == 'A'].copy()

                # Guardar cambios en el archivo
                manual_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')

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

    # Preguntar si desea añadir nuevo registro
    st.divider()
    if st.radio("¿Desea añadir un nuevo registro?", ["No", "Sí"], index=0) == "Sí":
        # Formulario para nuevo registro
        st.subheader("📝 Nuevo registro de artículo")

        # Usar session_state para mantener los datos del formulario
        if 'form_data' not in st.session_state:
            st.session_state.form_data = None
            st.session_state.show_confirmation = False

        # Formulario principal con botón de submit
        with st.form("nuevo_articulo"):
            article_title = st.text_area("📄 Título del artículo:", height=100)
            corresponding_author = st.text_input("📌 Autor para correspondencia:")
            coauthors = st.text_area("👥 Coautores (separados por punto y coma ';'):", help="Ejemplo: Autor1; Autor2; Autor3")
            departamento = st.selectbox(
                "🏢 Departamento de adscripción:",
                options=DEPARTAMENTOS_INCICH,
                index=0
            )

            # Detalles de publicación
            st.subheader("📅 Detalles de publicación")
            col1, col2 = st.columns(2)
            with col1:
                year = st.text_input("Año de publicación:")
            with col2:
                pub_date = st.text_input("Fecha exacta [YYYY-MM-DD]:", placeholder="AAAA-MM-DD")

            col3, col4 = st.columns(2)
            with col3:
                volume = st.text_input("📚 Volumen (ej 79(3), el volumen es 79")
            with col4:
                number = st.text_input("# Número (ej 79(3), el número es 3)")

            pages = st.text_input("🔖 Páginas (ej. 123-130):")
            journal_full = st.text_input("🏛️ Nombre completo de la revista:")
            journal_abbrev = st.text_input("🏷️ Abreviatura de la revista:")
            doi = st.text_input("🌐 DOI:")
            pmid = st.text_input("🔍 PMID (opcional):")

            # Grupo JCR
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

            # Palabras clave (mínimo 1, máximo 3)
            st.subheader("🔑 Líneas de Investigación")
            selected_categories = st.multiselect(
                "Seleccione al menos 1 línea de investigación:",
                options=list(KEYWORD_CATEGORIES.keys()),
                max_selections=CONFIG.MAX_KEYWORDS
            )

            # Botón de submit para avanzar a confirmación
            submitted = st.form_submit_button("De clic si ha  respondido todo lo  anterior.")

            if submitted:
                # Validación de campos obligatorios
                if not article_title or not corresponding_author or not year:
                    st.error("Por favor complete los campos obligatorios: Título, Autor para correspondencia y Año")
                    st.stop()

                if not selected_categories:
                    st.error("Por favor seleccione al menos una línea de investigación")
                    st.stop()

                st.session_state.form_data = {
                    'economic_number': economic_number,
                    'nombramiento': nombramiento,
                    'sni': sni,
                    'sii': sii,
                    'departamento': departamento,
                    'corresponding_author': corresponding_author,
                    'coauthors': coauthors,
                    'article_title': article_title,
                    'year': year,
                    'pub_date': pub_date if pub_date else f"{year}-01-01",
                    'volume': volume or '0',
                    'number': number or '0',
                    'pages': pages or '0',
                    'journal_full': journal_full,
                    'journal_abbrev': journal_abbrev,
                    'doi': doi,
                    'jcr_group': jcr_group,
                    'pmid': pmid,
                    'selected_keywords': str(selected_categories)
                }
                st.session_state.show_confirmation = True
                st.rerun()

        # Mostrar sección de confirmación si está activa
        if st.session_state.get('show_confirmation', False) and st.session_state.form_data:
            st.subheader("🔍 Confirmación de Autoría")

            # Construir lista de autores
            authors_list = [st.session_state.form_data['corresponding_author'].strip()]
            if st.session_state.form_data['coauthors']:
                authors_list.extend([author.strip() for author in st.session_state.form_data['coauthors'].split(";") if author.strip()])

            investigator_name = st.selectbox(
                "Seleccione su nombre como aparece en la publicación:",
                options=authors_list,
                key="investigator_select"
            )

            # Calcular participation_key
            if investigator_name == st.session_state.form_data['corresponding_author']:
                participation_key = "CA"
            else:
                author_position = authors_list.index(investigator_name) + 1
                participation_key = f"{author_position}C"

            # Botón de acción final (sin col2)
            if st.button("✅ Guardar registro definitivo", type="primary"):
                # Crear registro completo
                nuevo_registro = {
                    **st.session_state.form_data,
                    'participation_key': participation_key,
                    'investigator_name': investigator_name,
                    'estado': 'A'
                }

                if save_to_csv(nuevo_registro):
                    st.success("✅ Registro guardado exitosamente!")
                    st.balloons()
                    # Limpiar session_state y recargar
                    del st.session_state.form_data
                    st.session_state.show_confirmation = False
                    time.sleep(2)
                    st.rerun()
                else:
                    st.error("❌ Error al guardar el registro")

            # Opción para volver a editar debajo del botón principal
            if st.button("↩️ Volver a editar", key="volver_editar"):
                st.session_state.show_confirmation = False
                st.rerun()


if __name__ == "__main__":
    main()

