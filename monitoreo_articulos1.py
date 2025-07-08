import streamlit as st
import pandas as pd
import io
from datetime import datetime
import calendar
import paramiko
import time
import os
import logging
import zipfile
from pathlib import Path
from PIL import Image

# Configuración de logging
logging.basicConfig(
    filename='monitoreo_articulos.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ====================
# CATEGORÍAS DE KEYWORDS PARA ARTÍCULOS
# ====================
KEYWORD_CATEGORIES = {
    "Accidente Cerebrovascular": ["accidente cerebrovascular", "acv", "ictus", "stroke"],
    "Alzheimer": ["alzheimer", "demencia", "enfermedad neurodegenerativa"],
    # ... (agrega más categorías según sea necesario)
}

# ====================
# CONFIGURACIÓN INICIAL
# ====================
class Config:
    def __init__(self):
        # Configuración SFTP
        self.REMOTE_ARTICULOS_FILE = "pro_productos_total.csv"  # Nombre completo del archivo remoto
        self.REMOTE_GENERADOR_PATH = f"{st.secrets['sftp']['dir']}/{st.secrets['prefixes']['generadorarticulos']}"
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
    """Ejecuta el script generadorarticulos.sh en el servidor remoto"""
    ssh = None
    try:
        with st.spinner("🔄 Ejecutando generadorarticulos.sh en servidor remoto..."):
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
                logging.error(f"Error ejecutando generadorarticulos.sh: {error_msg}")
                return False

            logging.info("Script ejecutado correctamente")
            
            # 4. Verificar que el archivo se creó en la ubicación correcta
            sftp = ssh.open_sftp()
            output_path = os.path.join(CONFIG.REMOTE['DIR'], CONFIG.REMOTE_ARTICULOS_FILE)
            try:
                sftp.stat(output_path)
                file_size = sftp.stat(output_path).st_size
                logging.info(f"Archivo creado en: {output_path} (Tamaño: {file_size} bytes)")
                st.success("✅ generadorarticulos.sh ejecutado correctamente en el servidor")
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

def sync_articulos_file():
    """Sincroniza el archivo pro_productos_total.csv desde el servidor remoto"""
    try:
        remote_path = os.path.join(CONFIG.REMOTE['DIR'], CONFIG.REMOTE_ARTICULOS_FILE)
        local_path = "articulos_total.csv"
        
        with st.spinner("🔄 Sincronizando archivo articulos_total.csv desde el servidor..."):
            if SSHManager.download_remote_file(remote_path, local_path):
                st.success("✅ Archivo articulos_total.csv sincronizado correctamente")
                return True
            else:
                st.error("❌ No se pudo descargar el archivo articulos_total.csv del servidor")
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

def generar_tabla_resumen(unique_articulos, filtered_df):
    """Genera una tabla consolidada con todos los totales"""
    datos_resumen = []
    
    # 1. Total artículos únicos
    total_articulos = len(unique_articulos)
    datos_resumen.append(("Artículos únicos", total_articulos))
    
    # 2. Revistas
    total_revistas = unique_articulos['journal_full'].nunique()
    datos_resumen.append(("Revistas distintas", total_revistas))
    
    # 3. Autores
    autores_unicos = set()
    for autores in unique_articulos['coauthors'].dropna():
        autores_list = [a.strip() for a in autores.split(";") if a.strip()]
        autores_unicos.update(autores_list)
    datos_resumen.append(("Autores distintos", len(autores_unicos)))
    
    # 4. Líneas de investigación
    try:
        all_keywords = []
        for keywords in unique_articulos['selected_keywords']:
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
    
    # 5. Departamentos
    if 'departamento' in unique_articulos.columns:
        total_deptos = unique_articulos['departamento'].nunique()
        datos_resumen.append(("Departamentos distintos", total_deptos))
    
    # 6. Distribución temporal (meses)
    total_meses = unique_articulos['pub_date'].dt.to_period('M').nunique()
    datos_resumen.append(("Meses con publicaciones", total_meses))
    
    # 7. Nivel SNI
    if 'sni' in unique_articulos.columns:
        total_sni = unique_articulos['sni'].nunique()
        datos_resumen.append(("Niveles SNI distintos", total_sni))
    
    # 8. Nivel SII
    if 'sii' in unique_articulos.columns:
        total_sii = unique_articulos['sii'].nunique()
        datos_resumen.append(("Niveles SII distintos", total_sii))
    
    # 9. Nombramientos
    if 'nombramiento' in unique_articulos.columns:
        total_nombramientos = unique_articulos['nombramiento'].nunique()
        datos_resumen.append(("Tipos de nombramiento distintos", total_nombramientos))
    
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
# FUNCIONES DE MÉTRICAS PARA ARTÍCULOS
# ====================
def indice_calidad_revista(journal_abbrev, jcr_group):
    """Calcula el Índice de Calidad de Revista (ICR) basado en el prestigio de la revista"""
    if pd.isna(journal_abbrev):
        return 0.3

    journal_abbrev = str(journal_abbrev).strip()

    # Definición de categorías de revistas
    revistas_tier1 = ['Nature', 'Science', 'Cell', 'Lancet', 'NEJM']
    revistas_tier2 = ['JAMA', 'BMJ', 'Circulation', 'JACC']
    revistas_tier3 = ['PLoS One', 'Scientific Reports']

    # Si tenemos información de JCR, usamos eso primero
    if pd.notna(jcr_group):
        try:
            jcr_num = float(jcr_group.split()[-1])
            if jcr_num >= 4.0:
                return 1.0
            elif jcr_num >= 2.0:
                return 0.7
            elif jcr_num >= 1.0:
                return 0.5
        except:
            pass

    # Si no hay JCR o no se pudo parsear, usamos la lista de revistas
    if journal_abbrev in revistas_tier1:
        return 1.0
    elif journal_abbrev in revistas_tier2:
        return 0.7
    elif journal_abbrev in revistas_tier3:
        return 0.5
    else:
        return 0.3  # Revistas no clasificadas

def coeficiente_colaboracion(coauthors):
    """Calcula el Coeficiente de Colaboración (CC)"""
    if pd.isna(coauthors):
        return 0.0

    try:
        autores = [a.strip() for a in str(coauthors).split(";") if a.strip()]
        num_autores = len(autores)
        if num_autores == 1:
            return 0.0
        elif num_autores <= 3:
            return 0.5
        elif num_autores <= 6:
            return 0.7
        else:
            return 1.0
    except:
        return 0.0

def indice_relevancia_tematica(keywords):
    """Calcula el Índice de Relevancia Temática (IRT)"""
    if pd.isna(keywords):
        return 0.0

    try:
        if isinstance(keywords, str):
            # Limpiar y separar keywords
            keywords_str = keywords.strip()
            if keywords_str.startswith('[') and keywords_str.endswith(']'):
                keywords_str = keywords_str[1:-1]
                kw_list = [k.strip().strip("'\"") for k in keywords_str.split(",") if k.strip()]
            else:
                kw_list = [k.strip() for k in keywords_str.split(",") if k.strip()]

            # Calcular matches con categorías de keywords
            matches = 0
            for kw in kw_list:
                for category, terms in KEYWORD_CATEGORIES.items():
                    if any(term in kw.lower() for term in terms):
                        matches += 1
                        break
            
            return matches / len(kw_list) if kw_list else 0.0
        return 0.0
    except:
        return 0.0

# ====================
# FUNCIÓN MAIN COMPLETA
# ====================

def main():
    st.set_page_config(
        page_title="Análisis de Artículos Científicos",
        page_icon="📄",
        layout="wide"
    )

    # Añadir logo en la parte superior
    if Path(CONFIG.LOGO_PATH).exists():
        st.image(CONFIG.LOGO_PATH, width=200)

    st.title("Análisis de Artículos Científicos")

    # Paso 1: Ejecutar generador remoto para actualizar datos
    if not ejecutar_generador_remoto():
        st.warning("⚠️ Continuando con datos existentes (pueden no estar actualizados)")

    # Paso 2: Sincronizar archivo articulos_total.csv
    if not sync_articulos_file():
        st.warning("⚠️ Trabajando con copia local de articulos_total.csv debido a problemas de conexión")

    # Verificar si el archivo local existe
    if not Path("articulos_total.csv").exists():
        st.error("No se encontró el archivo articulos_total.csv")
        return

    try:
        # Leer y procesar el archivo
        df = pd.read_csv("articulos_total.csv", header=0, encoding='utf-8')
        df.columns = df.columns.str.strip()

        # Verificar campos importantes
        required_columns = ['economic_number', 'nombramiento', 'sni', 'sii', 'departamento', 
                           'participation_key', 'investigator_name', 'corresponding_author', 
                           'coauthors', 'article_title', 'year', 'pub_date', 'volume', 
                           'number', 'pages', 'journal_full', 'journal_abbrev', 'doi', 
                           'jcr_group', 'pmid', 'selected_keywords', 'pdf_filename', 'estado']
        
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            st.warning(f"El archivo articulos_total.csv no contiene los campos requeridos: {', '.join(missing_columns)}")
            return

        # Convertir y validar fechas
        df['pub_date'] = pd.to_datetime(df['pub_date'], errors='coerce')
        df = df[(df['estado'] == 'A') & (df['pub_date'].notna())].copy()

        if df.empty:
            st.warning("No hay artículos válidos para analizar")
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
                       (df['pub_date'] <= pd.to_datetime(date_end))].copy()

        # Obtener artículos únicos (por título)
        unique_articulos = filtered_df.drop_duplicates(subset=['article_title']).copy()

        st.markdown(f"**Periodo seleccionado:** {date_start.strftime('%d/%m/%Y')} - {date_end.strftime('%d/%m/%Y')}")
        st.markdown(f"**Registros encontrados:** {len(filtered_df)}")
        st.markdown(f"**Artículos únicos:** {len(unique_articulos)}")

        if len(filtered_df) != len(unique_articulos):
            st.warning(f"⚠️ **Nota:** Se detectaron {len(filtered_df) - len(unique_articulos)} registros duplicados del mismo artículo.")

        if filtered_df.empty:
            st.warning("No hay artículos en el periodo seleccionado")
            return

        # =============================================
        # TABLA DE PRODUCTIVIDAD POR INVESTIGADOR
        # =============================================
        st.header("🔍 Productividad por investigador")
        investigator_stats = filtered_df.groupby(['investigator_name', 'economic_number']).agg(
            Articulos_Unicos=('article_title', lambda x: len(set(x))),
            Participaciones=('participation_key', lambda x: ', '.join(sorted(set(x))))
        ).reset_index()
        investigator_stats = investigator_stats.sort_values('Articulos_Unicos', ascending=False)
        investigator_stats.columns = ['Investigador', 'Número económico', 'Artículos únicos', 'Tipo de participación']

        mostrar_tabla_uniforme(investigator_stats, "Productividad por investigador")

        # Detalle expandible por investigador
        for index, row in investigator_stats.iterrows():
            with st.expander(f"{row['Investigador']} - {row['Artículos únicos']} artículos"):
                investigator_articulos = filtered_df[filtered_df['investigator_name'] == row['Investigador']]
                unique_articulos_investigator = investigator_articulos.drop_duplicates(subset=['article_title'])

                display_columns = ['article_title', 'journal_abbrev', 'pub_date', 'doi']
                if 'sni' in unique_articulos_investigator.columns and 'sii' in unique_articulos_investigator.columns:
                    display_columns.extend(['sni', 'sii'])
                if 'nombramiento' in unique_articulos_investigator.columns:
                    display_columns.append('nombramiento')

                st.write(f"Artículos de {row['Investigador']}:")
                mostrar_tabla_uniforme(unique_articulos_investigator[display_columns], "")

                # Sección de PDFs
                st.subheader("📄 Artículos disponibles")
                pdf_files = unique_articulos_investigator['pdf_filename'].dropna().unique()

                if len(pdf_files) > 0:
                    st.info(f"Se encontraron {len(pdf_files)} artículos PDF para este investigador")
                    selected_pdf = st.selectbox(
                        "Seleccione un artículo para ver:",
                        pdf_files,
                        key=f"pdf_selector_{row['Número económico']}_{index}"
                    )

                    if selected_pdf:
                        temp_pdf_path = f"temp_{selected_pdf}"
                        remote_pdf_path = os.path.join(CONFIG.REMOTE['DIR'], selected_pdf)

                        if SSHManager.download_remote_file(remote_pdf_path, temp_pdf_path):
                            with open(temp_pdf_path, "rb") as f:
                                pdf_bytes = f.read()

                            st.download_button(
                                label="Descargar este artículo",
                                data=pdf_bytes,
                                file_name=selected_pdf,
                                mime="application/pdf",
                                key=f"download_pdf_{row['Número económico']}_{index}"
                            )

                            try:
                                os.remove(temp_pdf_path)
                            except:
                                pass
                        else:
                            st.error("No se pudo descargar el PDF seleccionado")
                else:
                    st.warning("No se encontraron artículos PDF para este investigador")

                csv = unique_articulos_investigator.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Descargar producción de artículos en CSV",
                    data=csv,
                    file_name=f"articulos_{row['Investigador'].replace(' ', '_')}.csv",
                    mime='text/csv',
                    key=f"download_csv_{row['Número económico']}_{index}"
                )

        # =============================================
        # SECCIÓN DE MÉTRICAS DE CALIDAD
        # =============================================
        st.header("📊 Métricas de Calidad")

        # Calcular métricas para cada artículo único
        with st.spinner("Calculando métricas de calidad..."):
            unique_articulos = unique_articulos.assign(
                ICR=unique_articulos.apply(lambda x: indice_calidad_revista(x['journal_abbrev'], x['jcr_group']), axis=1),
                CC=unique_articulos['coauthors'].apply(coeficiente_colaboracion),
                IRT=unique_articulos['selected_keywords'].apply(indice_relevancia_tematica)
            )
            unique_articulos = unique_articulos.assign(
                PI=0.5 * unique_articulos['ICR'] + 0.3 * unique_articulos['CC'] + 0.2 * unique_articulos['IRT']
            )

        # Mostrar tabla de resultados por investigador
        st.subheader("Métricas por Investigador")
        metrics_by_investigator = unique_articulos.groupby('investigator_name').agg({
            'ICR': 'mean',
            'CC': 'mean',
            'IRT': 'mean',
            'PI': 'mean',
            'article_title': 'count'
        }).reset_index()

        metrics_by_investigator.columns = [
            'Investigador',
            'Calidad Revista (ICR)',
            'Colaboración (CC)',
            'Relevancia Temática (IRT)',
            'Puntaje Integrado (PI)',
            'Artículos Evaluados'
        ]

        metrics_by_investigator = metrics_by_investigator.sort_values('Puntaje Integrado (PI)', ascending=False)
        metrics_by_investigator = metrics_by_investigator.round(2)

        mostrar_tabla_uniforme(metrics_by_investigator, "Resumen de Métricas por Investigador")

        # Botón para explicación de métricas
        with st.expander("ℹ️ Explicación de las Métricas", expanded=False):
            st.markdown("""
            ### Índice de Calidad de Revista (ICR)
            **Fórmula:**
            Clasificación de revistas en 4 niveles con valores de 0.3 a 1.0
            - **1.0**: Revistas líderes (Nature, Science, Cell, etc.)
            - **0.7**: Revistas especializadas reconocidas (JAMA, BMJ, etc.)
            - **0.5**: Revistas académicas estándar
            - **0.3**: Otras revistas

            **Propósito:** Evaluar el prestigio de la revista donde se publicó.
            """)

            st.markdown("""
            ### Coeficiente de Colaboración (CC)
            **Fórmula:**
            - 0.0: Artículo individual
            - 0.5: 2-3 autores
            - 0.7: 4-6 autores
            - 1.0: Más de 6 autores

            **Propósito:** Medir grado de colaboración en la investigación.
            """)

            st.markdown("""
            ### Índice de Relevancia Temática (IRT)
            **Fórmula:**
            IRT = (N° palabras clave relevantes) / (Total palabras clave)

            **Propósito:** Evaluar relación con áreas de investigación prioritarias.
            """)

            st.markdown("""
            ### Puntaje Integrado (PI)
            **Fórmula:**
            PI = (0.5 × ICR) + (0.3 × CC) + (0.2 × IRT)

            **Interpretación:**
            0.8-1.0: Excelente | 0.6-0.79: Bueno | 0.4-0.59: Aceptable | <0.4: Bajo
            """)

        # Mostrar tabla completa de artículos con métricas
        st.subheader("Resultados Detallados por Artículo")
        metricas_df = unique_articulos[[
            'article_title', 'investigator_name', 'journal_abbrev',
            'ICR', 'CC', 'IRT', 'PI'
        ]].sort_values('PI', ascending=False)

        metricas_df.columns = [
            'Título', 'Autor Principal', 'Revista',
            'Calidad Revista (ICR)', 'Colaboración (CC)',
            'Relevancia Temática (IRT)', 'Puntaje Integrado (PI)'
        ]

        metricas_df = metricas_df.round(2)
        mostrar_tabla_uniforme(metricas_df, "")

        # =============================================
        # TOP 5 ARTÍCULOS POR CALIDAD
        # =============================================
        st.header("🏆 Artículos Destacados")
        top_articulos = unique_articulos.nlargest(5, 'PI')[[
            'article_title', 'investigator_name', 'journal_abbrev', 'PI', 'ICR', 'CC', 'IRT'
        ]]
        top_articulos.columns = [
            'Título', 'Autor', 'Revista',
            'Puntaje Integrado', 'Calidad Revista',
            'Colaboración', 'Relevancia Temática'
        ]

        mostrar_tabla_uniforme(top_articulos.round(2), "Top 5 artículos por Calidad Integral")

        # Botón para criterios de selección
        with st.expander("ℹ️ Criterios de Selección", expanded=False):
            st.markdown("""
            **Artículos destacados** se seleccionan por mayor Puntaje Integrado (PI) que combina:
            - 50% Calidad de revista (ICR)
            - 30% Colaboración (CC)
            - 20% Relevancia temática (IRT)

            Los artículos con PI ≥ 0.8 tienen calidad excepcional en los tres aspectos evaluados.
            """)

        # =============================================
        # SECCIÓN DE DESCARGAS GLOBALES
        # =============================================
        st.header("📥 Exportar Resultados")
        col1, col2, col3 = st.columns(3)

        with col1:
            csv_metricas = metricas_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Descargar métricas de calidad (CSV)",
                data=csv_metricas,
                file_name="metricas_calidad_articulos.csv",
                mime='text/csv'
            )

        with col2:
            if Path("articulos_total.csv").exists():
                with open("articulos_total.csv", "rb") as file:
                    st.download_button(
                        label="Descargar dataset completo",
                        data=file,
                        file_name="articulos_total.csv",
                        mime="text/csv"
                    )

        with col3:
            # Botón para descargar todos los PDFs con prefijo ART o MAN
            if st.button("Descargar todos los PDFs (ART/MAN)"):
                with st.spinner("Buscando archivos PDF en el servidor..."):
                    ssh = SSHManager.get_connection()
                    if ssh:
                        try:
                            with ssh.open_sftp() as sftp:
                                sftp.chdir(CONFIG.REMOTE['DIR'])
                                pdf_files = []
                                for filename in sftp.listdir():
                                    if (filename.startswith('ART') or filename.startswith('MAN')) and filename.lower().endswith('.pdf'):
                                        pdf_files.append(filename)

                                if not pdf_files:
                                    st.warning("No se encontraron archivos PDF con prefijo ART o MAN")
                                else:
                                    st.info(f"Se encontraron {len(pdf_files)} archivos PDF")

                                    # Crear un archivo ZIP con todos los PDFs
                                    zip_buffer = io.BytesIO()
                                    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                                        for pdf_file in pdf_files:
                                            temp_path = f"temp_{pdf_file}"
                                            remote_path = os.path.join(CONFIG.REMOTE['DIR'], pdf_file)
                                            if SSHManager.download_remote_file(remote_path, temp_path):
                                                zip_file.write(temp_path, pdf_file)
                                                os.remove(temp_path)

                                    zip_buffer.seek(0)
                                    st.download_button(
                                        label="Descargar todos los PDFs (ZIP)",
                                        data=zip_buffer,
                                        file_name="pdfs_articulos.zip",
                                        mime="application/zip",
                                        key="download_all_pdfs"
                                    )
                        except Exception as e:
                            st.error(f"Error al acceder a los archivos PDF: {str(e)}")
                            logging.error(f"Error al descargar PDFs: {str(e)}")
                        finally:
                            ssh.close()
                    else:
                        st.error("No se pudo establecer conexión con el servidor")

    except Exception as e:
        st.error(f"Error al procesar el archivo: {str(e)}")
        logging.error(f"Error en main: {str(e)}")

if __name__ == "__main__":
    main()
