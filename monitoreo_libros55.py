import streamlit as st
import pandas as pd
import io
from datetime import datetime
import calendar
import paramiko
import time
import os
import logging
from pathlib import Path
from PIL import Image

# Configuración de logging
logging.basicConfig(
    filename='monitoreo_libros.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ====================
# CATEGORÍAS DE KEYWORDS PARA LIBROS
# ====================
KEYWORD_CATEGORIES = {
    "Accidente Cerebrovascular": ["accidente cerebrovascular", "acv", "ictus", "stroke"],
    "Alzheimer": ["alzheimer", "demencia", "enfermedad neurodegenerativa"],
    # ... (resto de categorías de keywords se mantienen igual)
}

# ====================
# CONFIGURACIÓN INICIAL
# ====================
class Config:
    def __init__(self):
        # Configuración SFTP
        self.REMOTE_LIBROS_FILE = "pro_libros_total.csv"  # Nombre completo del archivo remoto
        self.REMOTE_GENERADOR_PATH = f"{st.secrets['sftp']['dir']}/{st.secrets['prefixes']['generadorlibros']}"
        self.TIMEOUT_SECONDS = 30
        
        self.REMOTE = {
            'HOST': st.secrets["sftp"]["host"],
            'USER': st.secrets["sftp"]["user"],
            'PASSWORD': st.secrets["sftp"]["password"],
            'PORT': st.secrets["sftp"]["port"],
            'DIR': st.secrets["sftp"]["dir"]
        }
        
        # Configuración de estilo
        self.HIGHLIGHT_COLOR = "#90EE90"
        self.LOGO_PATH = "escudo_COLOR.jpg"
        self.COLUMN_WIDTH = "200px"  # Ancho fijo para todas las columnas

CONFIG = Config()

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
                        logging.error(f"Archivo remoto no encontrado: {remote_path}")
                        return False
                        
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

def ejecutar_generador_remoto():
    """Ejecuta el script generadorlibros.sh en el servidor remoto"""
    ssh = None
    try:
        with st.spinner("🔄 Ejecutando generadorlibros.sh en servidor remoto..."):
            # Establecer conexión SSH
            ssh = SSHManager.get_connection()
            if not ssh:
                return False

            # 1. Verificar que el script existe
            sftp = ssh.open_sftp()
            try:
                sftp.stat(CONFIG.REMOTE_GENERADOR_PATH)
                logging.info(f"Script encontrado en: {CONFIG.REMOTE_GENERADOR_PATH}")
            except FileNotFoundError:
                st.error(f"❌ Error: No se encontró el script en {CONFIG.REMOTE_GENERADOR_PATH}")
                logging.error(f"Script no encontrado: {CONFIG.REMOTE_GENERADOR_PATH}")
                return False
            finally:
                sftp.close()

            # 2. Ejecutar el script en el directorio correcto
            comando = f"cd {CONFIG.REMOTE['DIR']} && bash {CONFIG.REMOTE_GENERADOR_PATH}"
            logging.info(f"Ejecutando comando: {comando}")
            
            stdin, stdout, stderr = ssh.exec_command(comando)
            exit_status = stdout.channel.recv_exit_status()
            output = stdout.read().decode('utf-8').strip()
            error = stderr.read().decode('utf-8').strip()

            # 3. Verificar resultados
            if exit_status != 0:
                error_msg = f"Código {exit_status}\nOutput: {output}\nError: {error}"
                st.error(f"❌ Error en la ejecución: {error_msg}")
                logging.error(f"Error ejecutando generadorlibros.sh: {error_msg}")
                return False

            logging.info("Script ejecutado correctamente")
            
            # 4. Verificar que el archivo se creó en la ubicación correcta
            sftp = ssh.open_sftp()
            output_path = os.path.join(CONFIG.REMOTE['DIR'], CONFIG.REMOTE_LIBROS_FILE)
            try:
                sftp.stat(output_path)
                file_size = sftp.stat(output_path).st_size
                logging.info(f"Archivo creado en: {output_path} (Tamaño: {file_size} bytes)")
                st.success("✅ generadorlibros.sh ejecutado correctamente en el servidor")
                return True
                
            except FileNotFoundError:
                error_msg = f"No se encontró el archivo de salida en {output_path}"
                st.error(f"❌ Error: {error_msg}")
                logging.error(error_msg)
                return False
            finally:
                sftp.close()

    except Exception as e:
        error_msg = f"Error inesperado: {str(e)}"
        st.error(f"❌ {error_msg}")
        logging.error(f"Error en ejecutar_generador_remoto: {error_msg}")
        return False
    finally:
        if ssh:
            ssh.close()

def sync_libros_file():
    """Sincroniza el archivo libros_total.csv desde el servidor remoto"""
    try:
        remote_path = os.path.join(CONFIG.REMOTE['DIR'], CONFIG.REMOTE_LIBROS_FILE)
        local_path = "libros_total.csv"
        
        with st.spinner("🔄 Sincronizando archivo libros_total.csv desde el servidor..."):
            if SSHManager.download_remote_file(remote_path, local_path):
                st.success("✅ Archivo libros_total.csv sincronizado correctamente")
                return True
            else:
                st.error("❌ No se pudo descargar el archivo libros_total.csv del servidor")
                return False
    except Exception as e:
        st.error(f"❌ Error en sincronización: {str(e)}")
        logging.error(f"Sync Error: {str(e)}")
        return False

def highlight_author(author: str, investigator_name: str) -> str:
    """Resalta el nombre del investigador principal"""
    if investigator_name and investigator_name.lower() == author.lower():
        return f"<span style='background-color: {CONFIG.HIGHLIGHT_COLOR};'>{author}</span>"
    return author

def generar_tabla_resumen(unique_libros, filtered_df):
    """Genera una tabla consolidada con todos los totales"""
    datos_resumen = []
    
    # 1. Total libros únicos (ya calculado)
    total_libros = len(unique_libros)
    datos_resumen.append(("Libros únicos", total_libros))
    
    # 2. Editoriales
    total_editoriales = unique_libros['editorial'].nunique()
    datos_resumen.append(("Editoriales distintas", total_editoriales))
    
    # 3. Tipos de participación
    total_participaciones = unique_libros['tipo_participacion'].nunique()
    datos_resumen.append(("Tipos de participación distintos", total_participaciones))
    
    # 4. Líneas de investigación
    try:
        all_keywords = []
        for keywords in unique_libros['selected_keywords']:
            if pd.notna(keywords):
                keywords_str = str(keywords).strip()
                if keywords_str.startswith('[') and keywords_str.endswith(']'):
                    keywords_str = keywords_str[1:-1]
                    import re
                    keyword_list = re.split(r",\s*(?=(?:[^']*'[^']*')*[^']*$)", keywords_str)
                    keyword_list = [k.strip().strip("'\"") for k in keyword_list if k.strip()]
                    all_keywords.extend(keyword_list)
                else:
                    keyword_list = [k.strip() for k in keywords_str.split(",") if k.strip()]
                    all_keywords.extend(keyword_list)
        total_keywords = len(set(all_keywords)) if all_keywords else 0
        datos_resumen.append(("Líneas de investigación distintas", total_keywords))
    except:
        datos_resumen.append(("Líneas de investigación distintas", "N/D"))
    
    # 5. Departamentos (si existe)
    if 'departamento' in unique_libros.columns:
        total_deptos = unique_libros['departamento'].nunique()
        datos_resumen.append(("Departamentos distintos", total_deptos))
    
    # 6. Distribución temporal (meses)
    total_meses = unique_libros['pub_date'].dt.to_period('M').nunique()
    datos_resumen.append(("Meses con publicaciones", total_meses))
    
    # 7. Nivel SNI (si existe)
    if 'sni' in unique_libros.columns:
        total_sni = unique_libros['sni'].nunique()
        datos_resumen.append(("Niveles SNI distintos", total_sni))
    
    # 8. Nivel SII (si existe)
    if 'sii' in unique_libros.columns:
        total_sii = unique_libros['sii'].nunique()
        datos_resumen.append(("Niveles SII distintos", total_sii))
    
    # 9. Nombramientos (si existe)
    if 'nombramiento' in unique_libros.columns:
        total_nombramientos = unique_libros['nombramiento'].nunique()
        datos_resumen.append(("Tipos de nombramiento distintos", total_nombramientos))
    
    # 10. Países de distribución (si existe)
    if 'paises_distribucion' in unique_libros.columns:
        try:
            all_countries = []
            for countries in unique_libros['paises_distribucion']:
                if pd.notna(countries):
                    cleaned = str(countries).strip().split(", ")
                    all_countries.extend([c.strip() for c in cleaned if c.strip()])
            total_paises = len(set(all_countries)) if all_countries else 0
            datos_resumen.append(("Países de distribución distintos", total_paises))
        except:
            datos_resumen.append(("Países de distribución distintos", "N/D"))
    
    # 11. Idiomas (si existe)
    if 'idiomas_disponibles' in unique_libros.columns:
        total_idiomas = unique_libros['idiomas_disponibles'].nunique()
        datos_resumen.append(("Idiomas distintos", total_idiomas))
    
    # 12. Formatos (si existe)
    if 'formatos_disponibles' in unique_libros.columns:
        total_formatos = unique_libros['formatos_disponibles'].nunique()
        datos_resumen.append(("Formatos distintos", total_formatos))
    
    # Crear DataFrame
    resumen_df = pd.DataFrame(datos_resumen, columns=['Categoría', 'Total'])
    
    return resumen_df

def aplicar_estilo_tabla(df):
    """Aplica estilo CSS para uniformizar el ancho de columnas"""
    styles = []
    for col in df.columns:
        styles.append({
            'selector': f'th.col_heading.col{df.columns.get_loc(col)}',
            'props': [('width', CONFIG.COLUMN_WIDTH)]
        })
        styles.append({
            'selector': f'td.col{df.columns.get_loc(col)}',
            'props': [('width', CONFIG.COLUMN_WIDTH)]
        })
    return df.style.set_table_styles(styles)

def mostrar_tabla_uniforme(df, titulo, ayuda=None, max_rows=10):
    """Muestra una tabla con columnas de ancho uniforme"""
    st.markdown(f"**{titulo}**")
    if ayuda:
        st.caption(ayuda)
    
    # Aplicar estilo CSS para uniformizar el ancho de columnas
    st.markdown(
        f"""
        <style>
            th, td {{
                width: {CONFIG.COLUMN_WIDTH} !important;
                min-width: {CONFIG.COLUMN_WIDTH} !important;
                max-width: {CONFIG.COLUMN_WIDTH} !important;
            }}
        </style>
        """,
        unsafe_allow_html=True
    )
    
    st.dataframe(df.head(max_rows), hide_index=True)

# ====================
# FUNCIONES DE MÉTRICAS PARA LIBROS (AGREGAR AL INICIO DEL ARCHIVO)
# ====================
def indice_calidad_editorial(editorial):
    """Calcula el Índice de Calidad Editorial (ICE) basado en el prestigio de la editorial"""
    if pd.isna(editorial):
        return 0.3

    editorial = str(editorial).strip()

    # Definición de categorías de editoriales
    editoriales_tier1 = ['Springer', 'Elsevier', 'Wiley', 'Nature', 'Oxford University Press']
    editoriales_tier2 = ['Taylor & Francis', 'Cambridge University Press', 'Academic Press', 'Bentham Science Publishers']
    editoriales_tier3 = ['Acta Biochimica Polonica', 'CRC Press']

    if editorial in editoriales_tier1:
        return 1.0
    elif editorial in editoriales_tier2:
        return 0.7
    elif editorial in editoriales_tier3:
        return 0.5
    else:
        return 0.3  # Editoriales no clasificadas

def coeficiente_internacionalizacion(paises, idiomas):
    """Calcula el Coeficiente de Internacionalización (CI)"""
    if pd.isna(paises) or pd.isna(idiomas):
        return 0.0

    try:
        # Procesar países (eliminar valores vacíos)
        paises_list = [p.strip() for p in str(paises).split(",") if p.strip()]
        # Procesar idiomas (eliminar valores vacíos)
        idiomas_list = [i.strip() for i in str(idiomas).split(",") if i.strip()]

        score_paises = min(len(paises_list), 3) / 3  # Normalizado a 0-1 (máx. 3 países)
        score_idiomas = min(len(idiomas_list), 2) / 2  # Normalizado a 0-1 (máx. 2 idiomas)
        return 0.6 * score_paises + 0.4 * score_idiomas
    except:
        return 0.0

def indice_relevancia_tematica(keywords):
    """Calcula el Índice de Relevancia Temática (IRT) para cardiología"""
    if pd.isna(keywords):
        return 0.0

    keywords_cardio = [
        "cardíaco", "miocardio", "arritmia", "isquemia",
        "hipertensión", "ECG", "insuficiencia cardíaca",
        "coronario", "válvula", "aterosclerosis", "angina"
    ]

    try:
        if isinstance(keywords, str):
            # Limpiar y separar keywords
            keywords_str = keywords.strip()
            if keywords_str.startswith('[') and keywords_str.endswith(']'):
                keywords_str = keywords_str[1:-1]
                kw_list = [k.strip().strip("'\"") for k in keywords_str.split(",") if k.strip()]
            else:
                kw_list = [k.strip() for k in keywords_str.split(",") if k.strip()]

            # Calcular matches con términos de cardiología
            matches = sum(1 for kw in kw_list if any(cardio_kw in kw.lower() for cardio_kw in keywords_cardio))
            return matches / len(kw_list) if kw_list else 0.0
        return 0.0
    except:
        return 0.0

# ====================
# FUNCIÓN MAIN COMPLETA (CON LAS CORRECCIONES)
# ====================

def main():
    st.set_page_config(
        page_title="Análisis de Libros",
        page_icon="📚",
        layout="wide"
    )

    # Añadir logo en la parte superior
    if Path(CONFIG.LOGO_PATH).exists():
        st.image(CONFIG.LOGO_PATH, width=200)

    st.title("Análisis de Libros")

    # Paso 1: Ejecutar generador remoto para actualizar datos
    if not ejecutar_generador_remoto():
        st.warning("⚠️ Continuando con datos existentes (pueden no estar actualizados)")

    # Paso 2: Sincronizar archivo libros_total.csv
    if not sync_libros_file():
        st.warning("⚠️ Trabajando con copia local de libros_total.csv debido a problemas de conexión")

    # Verificar si el archivo local existe
    if not Path("libros_total.csv").exists():
        st.error("No se encontró el archivo libros_total.csv")
        return

    try:
        # Leer y procesar el archivo
        df = pd.read_csv("libros_total.csv", header=0, encoding='utf-8')
        df.columns = df.columns.str.strip()

        # Verificar campos importantes
        required_columns = ['autor_principal', 'titulo_libro', 'pub_date', 'estado',
                          'editorial', 'paises_distribucion', 'idiomas_disponibles', 'selected_keywords']
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            st.warning(f"El archivo libros_total.csv no contiene los campos requeridos: {', '.join(missing_columns)}")
            return

        # Convertir y validar fechas
        df['pub_date'] = pd.to_datetime(df['pub_date'], errors='coerce')
        df = df[(df['estado'] == 'A') & (df['pub_date'].notna())]

        if df.empty:
            st.warning("No hay libros válidos para analizar")
            return

        st.success(f"Datos cargados correctamente. Registros activos: {len(df)}")

        # Obtener rangos de fechas disponibles
        min_date = df['pub_date'].min()
        max_date = df['pub_date'].max()

        # Selector de rango mes-año
        st.header("📅 Selección de Periodo")
        col1, col2 = st.columns(2)

        with col1:
            start_year = st.selectbox("Año inicio", range(min_date.year, max_date.year+1), index=0)
            start_month = st.selectbox("Mes inicio", range(1, 13), index=min_date.month-1,
                                    format_func=lambda x: datetime(1900, x, 1).strftime('%B'))

        with col2:
            end_year = st.selectbox("Año término", range(min_date.year, max_date.year+1),
                                 index=len(range(min_date.year, max_date.year+1))-1)
            end_month = st.selectbox("Mes término", range(1, 13), index=max_date.month-1,
                                  format_func=lambda x: datetime(1900, x, 1).strftime('%B'))

        # Calcular fechas de inicio y fin
        start_day = 1
        end_day = calendar.monthrange(end_year, end_month)[1]
        date_start = datetime(start_year, start_month, start_day)
        date_end = datetime(end_year, end_month, end_day)

        # Filtrar dataframe
        filtered_df = df[(df['pub_date'] >= pd.to_datetime(date_start)) &
                       (df['pub_date'] <= pd.to_datetime(date_end))]

        # Obtener libros únicos
        unique_libros = filtered_df.drop_duplicates(subset=['titulo_libro'])

        st.markdown(f"**Periodo seleccionado:** {date_start.strftime('%d/%m/%Y')} - {date_end.strftime('%d/%m/%Y')}")
        st.markdown(f"**Registros encontrados:** {len(filtered_df)}")
        st.markdown(f"**Libros únicos:** {len(unique_libros)}")

        if len(filtered_df) != len(unique_libros):
            st.warning(f"⚠️ **Nota:** Se detectaron {len(filtered_df) - len(unique_libros)} registros duplicados del mismo libro.")

        if filtered_df.empty:
            st.warning("No hay libros en el periodo seleccionado")
            return

        # =============================================
        # TABLA DE PRODUCTIVIDAD POR INVESTIGADOR (MANTENIDA SIN CAMBIOS)
        # =============================================
        st.header("🔍 Productividad por investigador")
        investigator_stats = filtered_df.groupby(['autor_principal', 'economic_number']).agg(
            Libros_Unicos=('titulo_libro', lambda x: len(set(x))),
            Participaciones=('tipo_participacion', lambda x: ', '.join(sorted(set(x))))
        ).reset_index()
        investigator_stats = investigator_stats.sort_values('Libros_Unicos', ascending=False)
        investigator_stats.columns = ['Investigador', 'Número económico', 'Libros únicos', 'Tipo de participación']

        # Mostrar tabla principal
        mostrar_tabla_uniforme(investigator_stats, "Productividad por investigador")

        # Detalle expandible por investigador (CON DESCARGAS DE PDF)
        for index, row in investigator_stats.iterrows():
            with st.expander(f"{row['Investigador']} - {row['Libros únicos']} libros"):
                investigator_libros = filtered_df[filtered_df['autor_principal'] == row['Investigador']]
                unique_libros_investigator = investigator_libros.drop_duplicates(subset=['titulo_libro'])

                # Mostrar tabla de libros
                display_columns = ['titulo_libro', 'editorial', 'pub_date', 'isbn_issn']
                if 'sni' in unique_libros_investigator.columns and 'sii' in unique_libros_investigator.columns:
                    display_columns.extend(['sni', 'sii'])
                if 'nombramiento' in unique_libros_investigator.columns:
                    display_columns.append('nombramiento')

                st.write(f"Libros de {row['Investigador']}:")
                mostrar_tabla_uniforme(unique_libros_investigator[display_columns], "")

                # SECCIÓN DE PORTADAS PDF (MANTENIDA)
                st.subheader("📄 Portadas disponibles")
                economic_number = row['Número económico']
                remote_pdfs = []

                # Buscar PDFs en el servidor
                ssh = SSHManager.get_connection()
                if ssh:
                    try:
                        with ssh.open_sftp() as sftp:
                            try:
                                remote_files = sftp.listdir(CONFIG.REMOTE['DIR'])
                                remote_pdfs = [f for f in remote_files if f.endswith(f".{economic_number}.pdf")]
                                remote_pdfs.sort(reverse=True)
                            except Exception as e:
                                st.warning(f"No se pudieron listar los archivos PDF: {str(e)}")
                    except Exception as e:
                        st.warning(f"Error al acceder a SFTP: {str(e)}")
                    finally:
                        ssh.close()

                # Mostrar opciones de descarga
                if remote_pdfs:
                    st.info(f"Se encontraron {len(remote_pdfs)} portadas para este investigador")
                    selected_pdf = st.selectbox(
                        "Seleccione una portada para ver:",
                        remote_pdfs,
                        key=f"pdf_selector_{economic_number}_{index}"
                    )

                    if selected_pdf:
                        temp_pdf_path = f"temp_{selected_pdf}"
                        remote_pdf_path = os.path.join(CONFIG.REMOTE['DIR'], selected_pdf)

                        if SSHManager.download_remote_file(remote_pdf_path, temp_pdf_path):
                            with open(temp_pdf_path, "rb") as f:
                                pdf_bytes = f.read()

                            st.download_button(
                                label="Descargar esta portada",
                                data=pdf_bytes,
                                file_name=selected_pdf,
                                mime="application/pdf",
                                key=f"download_pdf_{economic_number}_{index}"
                            )

                            try:
                                os.remove(temp_pdf_path)
                            except:
                                pass
                        else:
                            st.error("No se pudo descargar el PDF seleccionado")
                else:
                    st.warning("No se encontraron portadas PDF para este investigador")

                # Descargar CSV (MANTENIDO)
                csv = unique_libros_investigator.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Descargar producción de libros en CSV",
                    data=csv,
                    file_name=f"libros_{row['Investigador'].replace(' ', '_')}.csv",
                    mime='text/csv',
                    key=f"download_csv_{economic_number}_{index}"
                )

        # =============================================
        # SECCIÓN DE MÉTRICAS DE CALIDAD EDITORIAL (MEJORADA)
        # =============================================
        st.header("📊 Métricas de Calidad Editorial")

        # Explicación detallada de cada métrica
        with st.expander("🔍 Explicación Detallada de las Métricas", expanded=True):
            st.markdown("""
            ### Índice de Calidad Editorial (ICE)
            **Fórmula:**
            Clasificación de editoriales en 4 niveles con valores de 0.3 a 1.0

            **Criterios:**
            - **1.0**: Editoriales líderes (Springer, Elsevier, Wiley, Oxford University Press)
            - **0.7**: Editoriales especializadas reconocidas (Taylor & Francis, Cambridge University Press)
            - **0.5**: Editoriales académicas (Bentham Science, Acta Biochimica Polonica)
            - **0.3**: Otras editoriales no clasificadas

            **Propósito:**
            Evaluar el prestigio y reconocimiento de la editorial donde se publicó el libro.
            """)

            st.markdown("""
            ### Coeficiente de Internacionalización (CI)
            **Fórmula:**
            CI = (0.6 × [n° países/3]) + (0.4 × [n° idiomas/2])

            **Donde:**
            - **Países:** Número de países de distribución (máximo 3 para normalización)
            - **Idiomas:** Número de idiomas disponibles (máximo 2 para normalización)

            **Propósito:**
            Medir el alcance geográfico y lingüístico de la publicación.
            """)

            st.markdown("""
            ### Índice de Relevancia Temática (IRT)
            **Fórmula:**
            IRT = (N° palabras clave de cardiología) / (Total palabras clave)

            **Términos considerados como cardiología:**
            cardíaco, miocardio, arritmia, isquemia, hipertensión, ECG, insuficiencia cardíaca, coronario, válvula, aterosclerosis, angina

            **Propósito:**
            Evaluar qué tan relacionado está el contenido del libro con el área de cardiología.
            """)

            st.markdown("""
            ### Puntaje Integrado (PI)
            **Fórmula:**
            PI = (0.4 × ICE) + (0.3 × CI) + (0.3 × IRT)

            **Interpretación:**
            - **0.8-1.0**: Excelente calidad e impacto
            - **0.6-0.79**: Buena calidad
            - **0.4-0.59**: Calidad aceptable
            - **<0.4**: Baja calidad relativa

            **Propósito:**
            Proporcionar una evaluación integral combinando los tres aspectos clave.
            """)

        # Calcular métricas para cada libro único
        with st.spinner("Calculando métricas de calidad..."):
            unique_libros['ICE'] = unique_libros['editorial'].apply(indice_calidad_editorial)
            unique_libros['CI'] = unique_libros.apply(
                lambda x: coeficiente_internacionalizacion(x['paises_distribucion'], x['idiomas_disponibles']),
                axis=1
            )
            unique_libros['IRT'] = unique_libros['selected_keywords'].apply(indice_relevancia_tematica)
            unique_libros['PI'] = 0.4 * unique_libros['ICE'] + 0.3 * unique_libros['CI'] + 0.3 * unique_libros['IRT']

        # Mostrar tabla de resultados por investigador
        st.subheader("Métricas por Investigador")

        # Agrupar por investigador y calcular promedios
        metrics_by_investigator = unique_libros.groupby('autor_principal').agg({
            'ICE': 'mean',
            'CI': 'mean',
            'IRT': 'mean',
            'PI': 'mean',
            'titulo_libro': 'count'
        }).reset_index()

        metrics_by_investigator.columns = [
            'Investigador',
            'ICE Promedio',
            'CI Promedio',
            'IRT Promedio',
            'PI Promedio',
            'Libros Evaluados'
        ]

        # Ordenar por PI descendente y formatear
        metrics_by_investigator = metrics_by_investigator.sort_values('PI Promedio', ascending=False)
        metrics_by_investigator['ICE Promedio'] = metrics_by_investigator['ICE Promedio'].round(2)
        metrics_by_investigator['CI Promedio'] = metrics_by_investigator['CI Promedio'].round(2)
        metrics_by_investigator['IRT Promedio'] = metrics_by_investigator['IRT Promedio'].round(2)
        metrics_by_investigator['PI Promedio'] = metrics_by_investigator['PI Promedio'].round(2)

        mostrar_tabla_uniforme(metrics_by_investigator, "Resumen de Métricas por Investigador")

        # Mostrar tabla completa de libros con métricas
        st.subheader("Resultados Detallados por Libro")
        metricas_df = unique_libros[[
            'titulo_libro', 'autor_principal', 'editorial',
            'ICE', 'CI', 'IRT', 'PI'
        ]].sort_values('PI', ascending=False)

        metricas_df.columns = [
            'Título', 'Autor Principal', 'Editorial',
            'Calidad Editorial (ICE)', 'Internacionalización (CI)',
            'Relevancia Temática (IRT)', 'Puntaje Integrado (PI)'
        ]

        # Formatear valores numéricos
        metricas_df = metricas_df.round(2)
        mostrar_tabla_uniforme(metricas_df, "")

        # =============================================
        # TOP 5 LIBROS POR CALIDAD
        # =============================================
        st.header("🏆 Libros Destacados")

        top_libros = unique_libros.nlargest(5, 'PI')[[
            'titulo_libro', 'autor_principal', 'editorial', 'PI', 'ICE', 'CI', 'IRT'
        ]]
        top_libros.columns = [
            'Título', 'Autor', 'Editorial',
            'Puntaje Integrado', 'Calidad Editorial',
            'Internacionalización', 'Relevancia Temática'
        ]

        # Mostrar tabla con explicación
        st.markdown("""
        **Criterios de selección:**
        Los libros destacados son aquellos con mayor Puntaje Integrado (PI), que combina:
        - Calidad editorial (40%)
        - Internacionalización (30%)
        - Relevancia temática (30%)
        """)

        mostrar_tabla_uniforme(top_libros.round(2), "Top 5 libros por Calidad Integral")

        # =============================================
        # SECCIÓN DE DESCARGAS GLOBALES (MANTENIDA)
        # =============================================
        st.header("📥 Exportar Resultados")

        col1, col2 = st.columns(2)

        with col1:
            # Descargar métricas completas
            csv_metricas = metricas_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Descargar métricas de calidad (CSV)",
                data=csv_metricas,
                file_name="metricas_calidad_libros.csv",
                mime='text/csv'
            )

        with col2:
            # Descargar datos completos
            if Path("libros_total.csv").exists():
                with open("libros_total.csv", "rb") as file:
                    st.download_button(
                        label="Descargar dataset completo",
                        data=file,
                        file_name="libros_total.csv",
                        mime="text/csv"
                    )

    except Exception as e:
        st.error(f"Error al procesar el archivo: {str(e)}")
        logging.error(f"Error en main: {str(e)}")

if __name__ == "__main__":
    main()

