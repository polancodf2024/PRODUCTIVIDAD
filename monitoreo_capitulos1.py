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
    filename='monitoreo_capitulos.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ====================
# CATEGORÍAS DE KEYWORDS PARA CAPÍTULOS
# ====================
KEYWORD_CATEGORIES = {
    "Hipertensión": ["hipertensión", "presión arterial", "tensión arterial"],
    "Cardiología": ["cardíaco", "miocardio", "arritmia", "isquemia"],
    # ... (agregar más categorías relevantes para capítulos)
}

# ====================
# CONFIGURACIÓN INICIAL
# ====================
class Config:
    def __init__(self):
        # Configuración SFTP
        self.REMOTE_CAPITULOS_FILE = "pro_capitulos_total.csv"  # Nombre del archivo remoto
        self.REMOTE_GENERADOR_PATH = f"{st.secrets['sftp']['dir']}/{st.secrets['prefixes']['generadorcapitulos']}"
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
        self.COLUMN_WIDTH = "200px"  # Ancho fijo para columnas

CONFIG = Config()

# ==================
# CLASE SSH MEJORADA (se mantiene igual)
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
    """Ejecuta el script generadorcapitulos.sh en el servidor remoto"""
    ssh = None
    try:
        with st.spinner("🔄 Ejecutando generadorcapitulos.sh en servidor remoto..."):
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
                logging.error(f"Error ejecutando generadorcapitulos.sh: {error_msg}")
                return False

            logging.info("Script ejecutado correctamente")
            
            # 4. Verificar que el archivo se creó en la ubicación correcta
            sftp = ssh.open_sftp()
            output_path = os.path.join(CONFIG.REMOTE['DIR'], CONFIG.REMOTE_CAPITULOS_FILE)
            try:
                sftp.stat(output_path)
                file_size = sftp.stat(output_path).st_size
                logging.info(f"Archivo creado en: {output_path} (Tamaño: {file_size} bytes)")
                st.success("✅ generadorcapitulos.sh ejecutado correctamente en el servidor")
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

def sync_capitulos_file():
    """Sincroniza el archivo capitulos_total.csv desde el servidor remoto"""
    try:
        remote_path = os.path.join(CONFIG.REMOTE['DIR'], CONFIG.REMOTE_CAPITULOS_FILE)
        local_path = "capitulos_total.csv"
        
        with st.spinner("🔄 Sincronizando archivo capitulos_total.csv desde el servidor..."):
            if SSHManager.download_remote_file(remote_path, local_path):
                st.success("✅ Archivo capitulos_total.csv sincronizado correctamente")
                return True
            else:
                st.error("❌ No se pudo descargar el archivo capitulos_total.csv del servidor")
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

def generar_tabla_resumen(unique_capitulos, filtered_df):
    """Genera una tabla consolidada con todos los totales"""
    datos_resumen = []
    
    # 1. Total capítulos únicos (ya calculado)
    total_capitulos = len(unique_capitulos)
    datos_resumen.append(("Capítulos únicos", total_capitulos))
    
    # 2. Editoriales
    total_editoriales = unique_capitulos['editorial'].nunique()
    datos_resumen.append(("Editoriales distintas", total_editoriales))
    
    # 3. Tipos de participación
    total_participaciones = unique_capitulos['tipo_participacion'].nunique()
    datos_resumen.append(("Tipos de participación distintos", total_participaciones))
    
    # 4. Líneas de investigación
    try:
        all_keywords = []
        for keywords in unique_capitulos['selected_keywords']:
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
    if 'departamento' in unique_capitulos.columns:
        total_deptos = unique_capitulos['departamento'].nunique()
        datos_resumen.append(("Departamentos distintos", total_deptos))
    
    # 6. Distribución temporal (meses)
    total_meses = unique_capitulos['pub_date'].dt.to_period('M').nunique()
    datos_resumen.append(("Meses con publicaciones", total_meses))
    
    # 7. Nivel SNI
    if 'sni' in unique_capitulos.columns:
        total_sni = unique_capitulos['sni'].nunique()
        datos_resumen.append(("Niveles SNI distintos", total_sni))
    
    # 8. Nivel SII
    if 'sii' in unique_capitulos.columns:
        total_sii = unique_capitulos['sii'].nunique()
        datos_resumen.append(("Niveles SII distintos", total_sii))
    
    # 9. Nombramientos
    if 'nombramiento' in unique_capitulos.columns:
        total_nombramientos = unique_capitulos['nombramiento'].nunique()
        datos_resumen.append(("Tipos de nombramiento distintos", total_nombramientos))
    
    # 10. Idiomas
    if 'idiomas_disponibles' in unique_capitulos.columns:
        total_idiomas = unique_capitulos['idiomas_disponibles'].nunique()
        datos_resumen.append(("Idiomas distintos", total_idiomas))
    
    # 11. Coautores
    if 'coautores_secundarios' in unique_capitulos.columns:
        total_coautores = unique_capitulos['coautores_secundarios'].nunique()
        datos_resumen.append(("Coautores distintos", total_coautores))
    
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
# FUNCIONES DE MÉTRICAS PARA CAPÍTULOS
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

def coeficiente_colaboracion(coautores):
    """Calcula el Coeficiente de Colaboración (CC)"""
    if pd.isna(coautores):
        return 0.0

    try:
        # Procesar coautores (eliminar valores vacíos)
        coautores_list = [c.strip() for c in str(coautores).split(";") if c.strip()]
        return min(len(coautores_list), 5) / 5  # Normalizado a 0-1 (máx. 5 coautores)
    except:
        return 0.0

def indice_relevancia_tematica(keywords):
    """Calcula el Índice de Relevancia Temática (IRT) para capítulos"""
    if pd.isna(keywords):
        return 0.0

    keywords_estrategicas = [
        "hipertensión", "cardíaco", "miocardio", "arritmia",
        "isquemia", "ECG", "insuficiencia cardíaca",
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

            # Calcular matches con términos estratégicos
            matches = sum(1 for kw in kw_list if any(estrategico_kw in kw.lower() for estrategico_kw in keywords_estrategicas))
            return matches / len(kw_list) if kw_list else 0.0
        return 0.0
    except:
        return 0.0

# ====================
# FUNCIÓN MAIN MODIFICADA PARA CAPÍTULOS
# ====================
def main():
    st.set_page_config(
        page_title="Análisis de Capítulos",
        page_icon="📚",
        layout="wide"
    )

    # Añadir logo en la parte superior
    if Path(CONFIG.LOGO_PATH).exists():
        st.image(CONFIG.LOGO_PATH, width=200)

    st.title("Análisis de Capítulos de Libros")

    # Paso 1: Ejecutar generador remoto para actualizar datos
    if not ejecutar_generador_remoto():
        st.warning("⚠️ Continuando con datos existentes (pueden no estar actualizados)")

    # Paso 2: Sincronizar archivo capitulos_total.csv
    if not sync_capitulos_file():
        st.warning("⚠️ Trabajando con copia local de capitulos_total.csv debido a problemas de conexión")

    # Verificar si el archivo local existe
    if not Path("capitulos_total.csv").exists():
        st.error("No se encontró el archivo capitulos_total.csv")
        return

    try:
        # Leer y procesar el archivo
        df = pd.read_csv("capitulos_total.csv", header=0, encoding='utf-8')
        df.columns = df.columns.str.strip()

        # Verificar campos importantes
        required_columns = ['economic_number', 'nombramiento', 'sni', 'sii', 'departamento', 
                          'autor_principal', 'tipo_participacion', 'titulo_libro', 'titulo_capitulo',
                          'editorial', 'coautores_secundarios', 'year', 'pub_date', 'isbn_issn',
                          'numero_edicion', 'paginas', 'idiomas_disponibles', 'selected_keywords',
                          'pdf_filename', 'estado']
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            st.warning(f"El archivo capitulos_total.csv no contiene los campos requeridos: {', '.join(missing_columns)}")
            return

        # Convertir y validar fechas
        df['pub_date'] = pd.to_datetime(df['pub_date'], errors='coerce')
        df = df[(df['estado'] == 'A') & (df['pub_date'].notna())].copy()

        if df.empty:
            st.warning("No hay capítulos válidos para analizar")
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

        # Obtener capítulos únicos
        unique_capitulos = filtered_df.drop_duplicates(subset=['titulo_capitulo']).copy()

        st.markdown(f"**Periodo seleccionado:** {date_start.strftime('%d/%m/%Y')} - {date_end.strftime('%d/%m/%Y')}")
        st.markdown(f"**Registros encontrados:** {len(filtered_df)}")
        st.markdown(f"**Capítulos únicos:** {len(unique_capitulos)}")

        if len(filtered_df) != len(unique_capitulos):
            st.warning(f"⚠️ **Nota:** Se detectaron {len(filtered_df) - len(unique_capitulos)} registros duplicados del mismo capítulo.")

        if filtered_df.empty:
            st.warning("No hay capítulos en el periodo seleccionado")
            return

        # =============================================
        # TABLA DE PRODUCTIVIDAD POR INVESTIGADOR
        # =============================================
        st.header("🔍 Productividad por investigador")
        investigator_stats = filtered_df.groupby(['autor_principal', 'economic_number', 'nombramiento', 'sni', 'sii', 'departamento']).agg(
            Capitulos_Unicos=('titulo_capitulo', lambda x: len(set(x))),
            Participaciones=('tipo_participacion', lambda x: ', '.join(sorted(set(x))))
        ).reset_index()
        investigator_stats = investigator_stats.sort_values('Capitulos_Unicos', ascending=False)
        investigator_stats.columns = ['Investigador', 'Número económico', 'Nombramiento', 'SNI', 'SII', 'Departamento', 'Capítulos únicos', 'Tipo de participación']

        mostrar_tabla_uniforme(investigator_stats, "Productividad por investigador")

        # Detalle expandible por investigador
        for index, row in investigator_stats.iterrows():
            with st.expander(f"{row['Investigador']} - {row['Capítulos únicos']} capítulos"):
                investigator_capitulos = filtered_df[filtered_df['autor_principal'] == row['Investigador']]
                unique_capitulos_investigator = investigator_capitulos.drop_duplicates(subset=['titulo_capitulo'])

                display_columns = ['titulo_capitulo', 'titulo_libro', 'editorial', 'pub_date', 'isbn_issn']
                if 'paginas' in unique_capitulos_investigator.columns:
                    display_columns.append('paginas')
                if 'idiomas_disponibles' in unique_capitulos_investigator.columns:
                    display_columns.append('idiomas_disponibles')

                st.write(f"Capítulos de {row['Investigador']}:")
                mostrar_tabla_uniforme(unique_capitulos_investigator[display_columns], "")

                # Sección de PDFs
                st.subheader("📄 Capítulos disponibles")
                pdf_files = unique_capitulos_investigator['pdf_filename'].dropna().unique()

                if len(pdf_files) > 0:
                    st.info(f"Se encontraron {len(pdf_files)} capítulos en PDF para este investigador")
                    selected_pdf = st.selectbox(
                        "Seleccione un capítulo para ver:",
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
                                label="Descargar este capítulo",
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
                    st.warning("No se encontraron capítulos en PDF para este investigador")

                csv = unique_capitulos_investigator.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Descargar producción de capítulos en CSV",
                    data=csv,
                    file_name=f"capitulos_{row['Investigador'].replace(' ', '_')}.csv",
                    mime='text/csv',
                    key=f"download_csv_{row['Número económico']}_{index}"
                )

        # =============================================
        # SECCIÓN DE MÉTRICAS DE CALIDAD
        # =============================================
        st.header("📊 Métricas de Calidad de Capítulos")

        # Calcular métricas para cada capítulo único
        with st.spinner("Calculando métricas de calidad..."):
            unique_capitulos = unique_capitulos.assign(
                ICE=unique_capitulos['editorial'].apply(indice_calidad_editorial),
                CC=unique_capitulos['coautores_secundarios'].apply(coeficiente_colaboracion),
                IRT=unique_capitulos['selected_keywords'].apply(indice_relevancia_tematica)
            )
            unique_capitulos = unique_capitulos.assign(
                PI=0.5 * unique_capitulos['ICE'] + 0.3 * unique_capitulos['CC'] + 0.2 * unique_capitulos['IRT']
            )

        # Mostrar tabla de resultados por investigador
        st.subheader("Métricas por Investigador")
        metrics_by_investigator = unique_capitulos.groupby('autor_principal').agg({
            'ICE': 'mean',
            'CC': 'mean',
            'IRT': 'mean',
            'PI': 'mean',
            'titulo_capitulo': 'count'
        }).reset_index()

        metrics_by_investigator.columns = [
            'Investigador',
            'ICE Promedio',
            'CC Promedio',
            'IRT Promedio',
            'PI Promedio',
            'Capítulos Evaluados'
        ]

        metrics_by_investigator = metrics_by_investigator.sort_values('PI Promedio', ascending=False)
        metrics_by_investigator = metrics_by_investigator.round(2)

        mostrar_tabla_uniforme(metrics_by_investigator, "Resumen de Métricas por Investigador")

        # Botón para explicación de métricas
        with st.expander("ℹ️ Explicación de las Métricas", expanded=False):
            st.markdown("""
            ### Índice de Calidad Editorial (ICE)
            **Fórmula:**
            Clasificación de editoriales en 4 niveles con valores de 0.3 a 1.0
            - **1.0**: Editoriales líderes (Springer, Elsevier, Wiley)
            - **0.7**: Editoriales especializadas reconocidas
            - **0.5**: Editoriales académicas
            - **0.3**: Otras editoriales

            **Propósito:** Evaluar el prestigio de la editorial donde se publicó el capítulo.
            """)

            st.markdown("""
            ### Coeficiente de Colaboración (CC)
            **Fórmula:**
            CC = (N° coautores) / 5  # Normalizado a 0-1 (máx. 5 coautores)

            **Propósito:** Medir el grado de colaboración en el capítulo.
            """)

            st.markdown("""
            ### Índice de Relevancia Temática (IRT)
            **Fórmula:**
            IRT = (N° palabras clave estratégicas) / (Total palabras clave)

            **Propósito:** Evaluar relación con áreas estratégicas de investigación.
            """)

            st.markdown("""
            ### Puntaje Integrado (PI)
            **Fórmula:**
            PI = (0.5 × ICE) + (0.3 × CC) + (0.2 × IRT)

            **Interpretación:**
            0.8-1.0: Excelente | 0.6-0.79: Bueno | 0.4-0.59: Aceptable | <0.4: Bajo
            """)

        # Mostrar tabla completa de capítulos con métricas
        st.subheader("Resultados Detallados por Capítulo")
        metricas_df = unique_capitulos[[
            'titulo_capitulo', 'titulo_libro', 'autor_principal', 'editorial',
            'ICE', 'CC', 'IRT', 'PI'
        ]].sort_values('PI', ascending=False)

        metricas_df.columns = [
            'Capítulo', 'Libro', 'Autor Principal', 'Editorial',
            'Calidad Editorial (ICE)', 'Colaboración (CC)',
            'Relevancia Temática (IRT)', 'Puntaje Integrado (PI)'
        ]

        metricas_df = metricas_df.round(2)
        mostrar_tabla_uniforme(metricas_df, "")

        # =============================================
        # TOP 5 CAPÍTULOS POR CALIDAD
        # =============================================
        st.header("🏆 Capítulos Destacados")
        top_capitulos = unique_capitulos.nlargest(5, 'PI')[[
            'titulo_capitulo', 'titulo_libro', 'autor_principal', 'editorial', 'PI', 'ICE', 'CC', 'IRT'
        ]]
        top_capitulos.columns = [
            'Capítulo', 'Libro', 'Autor', 'Editorial',
            'Puntaje Integrado', 'Calidad Editorial',
            'Colaboración', 'Relevancia Temática'
        ]

        mostrar_tabla_uniforme(top_capitulos.round(2), "Top 5 capítulos por Calidad Integral")

        # Botón para criterios de selección
        with st.expander("ℹ️ Criterios de Selección", expanded=False):
            st.markdown("""
            **Capítulos destacados** se seleccionan por mayor Puntaje Integrado (PI) que combina:
            - 50% Calidad editorial (ICE)
            - 30% Colaboración (CC)
            - 20% Relevancia temática (IRT)

            Los capítulos con PI ≥ 0.8 tienen calidad excepcional en los tres aspectos evaluados.
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
                file_name="metricas_calidad_capitulos.csv",
                mime='text/csv'
            )

        with col2:
            if Path("capitulos_total.csv").exists():
                with open("capitulos_total.csv", "rb") as file:
                    st.download_button(
                        label="Descargar dataset completo",
                        data=file,
                        file_name="capitulos_total.csv",
                        mime="text/csv"
                    )

        with col3:
            # Botón para descargar todos los PDFs con prefijo CAP
            if st.button("Descargar todos los PDFs (CAP)"):
                with st.spinner("Buscando archivos PDF en el servidor..."):
                    ssh = SSHManager.get_connection()
                    if ssh:
                        try:
                            with ssh.open_sftp() as sftp:
                                sftp.chdir(CONFIG.REMOTE['DIR'])
                                pdf_files = []
                                for filename in sftp.listdir():
                                    if (filename.startswith('CAP')) and filename.lower().endswith('.pdf'):
                                        pdf_files.append(filename)

                                if not pdf_files:
                                    st.warning("No se encontraron archivos PDF con prefijo CAP")
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
                                        file_name="pdfs_capitulos.zip",
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
