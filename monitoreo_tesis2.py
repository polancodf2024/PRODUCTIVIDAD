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
    filename='monitoreo_tesis.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ====================
# CATEGORÍAS DE KEYWORDS PARA TESIS
# ====================
KEYWORD_CATEGORIES = {
    "Biología Celular": ["celular", "molecular", "energía", "metabolismo"],
    "Genética": ["genética", "ADN", "ARN", "genómica"],
    # ... (agregar más categorías relevantes para tesis)
}

# ====================
# CONFIGURACIÓN INICIAL
# ====================
class Config:
    def __init__(self):
        # Configuración SFTP
        self.REMOTE_TESIS_FILE = "pro_tesis_total.csv"  # Nombre del archivo remoto
        self.REMOTE_GENERADOR_PATH = f"{st.secrets['sftp']['dir']}/{st.secrets['prefixes']['generadortesis']}"
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
    """Ejecuta el script generadortesis.sh en el servidor remoto"""
    ssh = None
    try:
        with st.spinner("🔄 Ejecutando generadortesis.sh en servidor remoto..."):
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
                logging.error(f"Error ejecutando generadortesis.sh: {error_msg}")
                return False

            logging.info("Script ejecutado correctamente")
            
            # 4. Verificar que el archivo se creó en la ubicación correcta
            sftp = ssh.open_sftp()
            output_path = os.path.join(CONFIG.REMOTE['DIR'], CONFIG.REMOTE_TESIS_FILE)
            try:
                sftp.stat(output_path)
                file_size = sftp.stat(output_path).st_size
                logging.info(f"Archivo creado en: {output_path} (Tamaño: {file_size} bytes)")
                st.success("✅ generadortesis.sh ejecutado correctamente en el servidor")
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

def sync_tesis_file():
    """Sincroniza el archivo tesis_total.csv desde el servidor remoto"""
    try:
        remote_path = os.path.join(CONFIG.REMOTE['DIR'], CONFIG.REMOTE_TESIS_FILE)
        local_path = "tesis_total.csv"
        
        with st.spinner("🔄 Sincronizando archivo tesis_total.csv desde el servidor..."):
            if SSHManager.download_remote_file(remote_path, local_path):
                st.success("✅ Archivo tesis_total.csv sincronizado correctamente")
                return True
            else:
                st.error("❌ No se pudo descargar el archivo tesis_total.csv del servidor")
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

def generar_tabla_resumen(unique_tesis, filtered_df):
    """Genera una tabla consolidada con todos los totales"""
    datos_resumen = []
    
    # 1. Total tesis únicas
    total_tesis = len(unique_tesis)
    datos_resumen.append(("Tesis únicas", total_tesis))
    
    # 2. Departamentos
    total_deptos = unique_tesis['departamento'].nunique()
    datos_resumen.append(("Departamentos distintos", total_deptos))
    
    # 3. Tipos de tesis
    total_tipos = unique_tesis['tipo_tesis'].nunique()
    datos_resumen.append(("Tipos de tesis distintos", total_tipos))
    
    # 4. Líneas de investigación
    try:
        all_keywords = []
        for keywords in unique_tesis['selected_keywords']:
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
    
    # 5. Idiomas
    if 'idioma' in unique_tesis.columns:
        total_idiomas = unique_tesis['idioma'].nunique()
        datos_resumen.append(("Idiomas distintos", total_idiomas))
    
    # 6. Distribución temporal (años)
    total_anios = unique_tesis['year'].nunique()
    datos_resumen.append(("Años con tesis", total_anios))
    
    # 7. Nivel SNI
    if 'sni' in unique_tesis.columns:
        total_sni = unique_tesis['sni'].nunique()
        datos_resumen.append(("Niveles SNI distintos", total_sni))
    
    # 8. Nivel SII
    if 'sii' in unique_tesis.columns:
        total_sii = unique_tesis['sii'].nunique()
        datos_resumen.append(("Niveles SII distintos", total_sii))
    
    # 9. Nombramientos
    if 'nombramiento' in unique_tesis.columns:
        total_nombramientos = unique_tesis['nombramiento'].nunique()
        datos_resumen.append(("Tipos de nombramiento distintos", total_nombramientos))
    
    # 10. Directores
    if 'directores' in unique_tesis.columns:
        total_directores = unique_tesis['directores'].nunique()
        datos_resumen.append(("Directores distintos", total_directores))
    
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
# FUNCIONES DE MÉTRICAS PARA TESIS
# ====================
def indice_calidad_tesis(tipo_tesis):
    """Calcula el Índice de Calidad de Tesis (ICT) basado en el tipo de tesis"""
    if pd.isna(tipo_tesis):
        return 0.3

    tipo_tesis = str(tipo_tesis).strip().lower()

    # Definición de categorías de tesis
    if "doctorado" in tipo_tesis:
        return 1.0
    elif "maestría" in tipo_tesis:
        return 0.7
    elif "licenciatura" in tipo_tesis:
        return 0.5
    else:
        return 0.3  # Otros tipos

def coeficiente_colaboracion(directores, coautores):
    """Calcula el Coeficiente de Colaboración (CC)"""
    try:
        # Procesar directores
        directores_list = [d.strip() for d in str(directores).split(",") if d.strip()] if pd.notna(directores) else []
        # Procesar coautores
        coautores_list = [c.strip() for c in str(coautores).split(";") if c.strip()] if pd.notna(coautores) else []
        
        total_colaboradores = len(directores_list) + len(coautores_list)
        return min(total_colaboradores / 5, 1.0)  # Normalizado a 0-1 (máx. 5 colaboradores)
    except:
        return 0.0

def indice_relevancia_tematica_tesis(keywords):
    """Calcula el Índice de Relevancia Temática (IRT) para tesis"""
    if pd.isna(keywords):
        return 0.0

    keywords_estrategicas = [
        "sistemas biológicos", "celular", "molecular", "energía",
        "genómica", "biotecnología", "investigación aplicada"
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
# FUNCIÓN MAIN MODIFICADA PARA TESIS
# ====================
def main():
    st.set_page_config(
        page_title="Análisis de Tesis",
        page_icon="📚",
        layout="wide"
    )

    # Añadir logo en la parte superior
    if Path(CONFIG.LOGO_PATH).exists():
        st.image(CONFIG.LOGO_PATH, width=200)

    st.title("Análisis de Tesis")

    # Paso 1: Ejecutar generador remoto para actualizar datos
    if not ejecutar_generador_remoto():
        st.warning("⚠️ Continuando con datos existentes (pueden no estar actualizados)")

    # Paso 2: Sincronizar archivo tesis_total.csv
    if not sync_tesis_file():
        st.warning("⚠️ Trabajando con copia local de tesis_total.csv debido a problemas de conexión")

    # Verificar si el archivo local existe
    if not Path("tesis_total.csv").exists():
        st.error("No se encontró el archivo tesis_total.csv")
        return

    try:
        # Leer y procesar el archivo
        df = pd.read_csv("tesis_total.csv", header=0, encoding='utf-8')
        df.columns = df.columns.str.strip()

        # Verificar campos importantes
        required_columns = ['economic_number', 'nombramiento', 'sni', 'sii', 'departamento', 
                          'titulo_tesis', 'tipo_tesis', 'year', 'pub_date', 'directores', 
                          'paginas', 'idioma', 'estudiante', 'coautores', 'selected_keywords', 
                          'pdf_filename', 'estado']
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            st.warning(f"El archivo tesis_total.csv no contiene los campos requeridos: {', '.join(missing_columns)}")
            return

        # Convertir y validar fechas
        df['pub_date'] = pd.to_datetime(df['pub_date'], errors='coerce')
        df = df[(df['estado'] == 'A') & (df['pub_date'].notna())].copy()

        if df.empty:
            st.warning("No hay tesis válidas para analizar")
            return

        st.success(f"Datos cargados correctamente. Registros activos: {len(df)}")

        # Obtener rangos de años disponibles
        min_year = df['year'].min()
        max_year = df['year'].max()

        # Selector de rango de años
        st.header("📅 Selección de Periodo")
        col1, col2 = st.columns(2)

        with col1:
            start_year = st.selectbox("Año inicio", range(int(min_year), int(max_year)+1), index=0)
        
        with col2:
            end_year = st.selectbox("Año término", range(int(min_year), int(max_year)+1),
                                 index=len(range(int(min_year), int(max_year)+1))-1)

        # Filtrar dataframe
        filtered_df = df[(df['year'] >= start_year) & (df['year'] <= end_year)].copy()

        # Obtener tesis únicas
        unique_tesis = filtered_df.drop_duplicates(subset=['titulo_tesis']).copy()

        st.markdown(f"**Periodo seleccionado:** {start_year} - {end_year}")
        st.markdown(f"**Registros encontrados:** {len(filtered_df)}")
        st.markdown(f"**Tesis únicas:** {len(unique_tesis)}")

        if len(filtered_df) != len(unique_tesis):
            st.warning(f"⚠️ **Nota:** Se detectaron {len(filtered_df) - len(unique_tesis)} registros duplicados de la misma tesis.")

        if filtered_df.empty:
            st.warning("No hay tesis en el periodo seleccionado")
            return

        # =============================================
        # TABLA DE PRODUCTIVIDAD POR INVESTIGADOR
        # =============================================
        st.header("🔍 Productividad por investigador")
        investigator_stats = filtered_df.groupby(['economic_number', 'nombramiento', 'sni', 'sii', 'departamento']).agg(
            Tesis_Dirigidas=('titulo_tesis', lambda x: len(set(x))),
            Tipos_Tesis=('tipo_tesis', lambda x: ', '.join(sorted(set(x))))
        ).reset_index()
        investigator_stats = investigator_stats.sort_values('Tesis_Dirigidas', ascending=False)
        investigator_stats.columns = ['Número económico', 'Nombramiento', 'SNI', 'SII', 'Departamento', 'Tesis dirigidas', 'Tipos de tesis']

        mostrar_tabla_uniforme(investigator_stats, "Productividad por investigador")

        # Detalle expandible por investigador
        for index, row in investigator_stats.iterrows():
            with st.expander(f"{row['Número económico']} - {row['Tesis dirigidas']} tesis"):
                investigator_tesis = filtered_df[filtered_df['economic_number'] == row['Número económico']]
                unique_tesis_investigator = investigator_tesis.drop_duplicates(subset=['titulo_tesis'])

                display_columns = ['titulo_tesis', 'tipo_tesis', 'year', 'directores', 'estudiante']
                if 'paginas' in unique_tesis_investigator.columns:
                    display_columns.append('paginas')
                if 'idioma' in unique_tesis_investigator.columns:
                    display_columns.append('idioma')

                st.write(f"Tesis dirigidas por {row['Número económico']}:")
                mostrar_tabla_uniforme(unique_tesis_investigator[display_columns], "")

                # Sección de PDFs
                st.subheader("📄 Tesis disponibles")
                pdf_files = unique_tesis_investigator['pdf_filename'].dropna().unique()

                if len(pdf_files) > 0:
                    st.info(f"Se encontraron {len(pdf_files)} tesis en PDF para este investigador")
                    selected_pdf = st.selectbox(
                        "Seleccione una tesis para ver:",
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
                                label="Descargar esta tesis",
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
                    st.warning("No se encontraron tesis en PDF para este investigador")

                csv = unique_tesis_investigator.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Descargar producción de tesis en CSV",
                    data=csv,
                    file_name=f"tesis_{row['Número económico']}.csv",
                    mime='text/csv',
                    key=f"download_csv_{row['Número económico']}_{index}"
                )

        # =============================================
        # SECCIÓN DE MÉTRICAS DE CALIDAD
        # =============================================
        st.header("📊 Métricas de Calidad de Tesis")

        # Calcular métricas para cada tesis única
        with st.spinner("Calculando métricas de calidad..."):
            unique_tesis = unique_tesis.assign(
                ICT=unique_tesis['tipo_tesis'].apply(indice_calidad_tesis),
                CC=unique_tesis.apply(lambda x: coeficiente_colaboracion(x['directores'], x['coautores']), axis=1),
                IRT=unique_tesis['selected_keywords'].apply(indice_relevancia_tematica_tesis)
            )
            unique_tesis = unique_tesis.assign(
                PI=0.5 * unique_tesis['ICT'] + 0.3 * unique_tesis['CC'] + 0.2 * unique_tesis['IRT']
            )

        # Mostrar tabla de resultados por investigador
        st.subheader("Métricas por Investigador")
        metrics_by_investigator = unique_tesis.groupby('economic_number').agg({
            'ICT': 'mean',
            'CC': 'mean',
            'IRT': 'mean',
            'PI': 'mean',
            'titulo_tesis': 'count'
        }).reset_index()

        metrics_by_investigator.columns = [
            'Número económico',
            'ICT Promedio',
            'CC Promedio',
            'IRT Promedio',
            'PI Promedio',
            'Tesis Evaluadas'
        ]

        metrics_by_investigator = metrics_by_investigator.sort_values('PI Promedio', ascending=False)
        metrics_by_investigator = metrics_by_investigator.round(2)

        mostrar_tabla_uniforme(metrics_by_investigator, "Resumen de Métricas por Investigador")

        # Botón para explicación de métricas
        with st.expander("ℹ️ Explicación de las Métricas", expanded=False):
            st.markdown("""
            ### Índice de Calidad de Tesis (ICT)
            **Fórmula:**
            Clasificación de tesis por nivel académico:
            - **1.0**: Doctorado
            - **0.7**: Maestría
            - **0.5**: Licenciatura
            - **0.3**: Otros tipos

            **Propósito:** Evaluar el nivel académico de la tesis.
            """)

            st.markdown("""
            ### Coeficiente de Colaboración (CC)
            **Fórmula:**
            CC = (N° directores + coautores) / 5  # Normalizado a 0-1 (máx. 5 colaboradores)

            **Propósito:** Medir el grado de colaboración en la tesis.
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
            PI = (0.5 × ICT) + (0.3 × CC) + (0.2 × IRT)

            **Interpretación:**
            0.8-1.0: Excelente | 0.6-0.79: Bueno | 0.4-0.59: Aceptable | <0.4: Bajo
            """)

        # Mostrar tabla completa de tesis con métricas
        st.subheader("Resultados Detallados por Tesis")
        metricas_df = unique_tesis[[
            'titulo_tesis', 'economic_number', 'tipo_tesis',
            'ICT', 'CC', 'IRT', 'PI'
        ]].sort_values('PI', ascending=False)

        metricas_df.columns = [
            'Título', 'Número económico', 'Tipo de tesis',
            'Calidad (ICT)', 'Colaboración (CC)',
            'Relevancia (IRT)', 'Puntaje Integrado (PI)'
        ]

        metricas_df = metricas_df.round(2)
        mostrar_tabla_uniforme(metricas_df, "")

        # =============================================
        # TOP 5 TESIS POR CALIDAD
        # =============================================
        st.header("🏆 Tesis Destacadas")
        top_tesis = unique_tesis.nlargest(5, 'PI')[[
            'titulo_tesis', 'economic_number', 'tipo_tesis', 'PI', 'ICT', 'CC', 'IRT'
        ]]
        top_tesis.columns = [
            'Título', 'Número económico', 'Tipo de tesis',
            'Puntaje Integrado', 'Calidad',
            'Colaboración', 'Relevancia'
        ]

        mostrar_tabla_uniforme(top_tesis.round(2), "Top 5 tesis por Calidad Integral")

        # Botón para criterios de selección
        with st.expander("ℹ️ Criterios de Selección", expanded=False):
            st.markdown("""
            **Tesis destacadas** se seleccionan por mayor Puntaje Integrado (PI) que combina:
            - 50% Calidad académica (ICT)
            - 30% Colaboración (CC)
            - 20% Relevancia temática (IRT)

            Las tesis con PI ≥ 0.8 tienen calidad excepcional en los tres aspectos evaluados.
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
                file_name="metricas_calidad_tesis.csv",
                mime='text/csv'
            )

        with col2:
            if Path("tesis_total.csv").exists():
                with open("tesis_total.csv", "rb") as file:
                    st.download_button(
                        label="Descargar dataset completo",
                        data=file,
                        file_name="tesis_total.csv",
                        mime="text/csv"
                    )

        with col3:
            # Botón para descargar todos los PDFs con prefijo TES
            if st.button("Descargar todos los PDFs (TES)"):
                with st.spinner("Buscando archivos PDF en el servidor..."):
                    ssh = SSHManager.get_connection()
                    if ssh:
                        try:
                            with ssh.open_sftp() as sftp:
                                sftp.chdir(CONFIG.REMOTE['DIR'])
                                pdf_files = []
                                for filename in sftp.listdir():
                                    if (filename.startswith('TES')) and filename.lower().endswith('.pdf'):
                                        pdf_files.append(filename)

                                if not pdf_files:
                                    st.warning("No se encontraron archivos PDF con prefijo TES")
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
                                        file_name="pdfs_tesis.zip",
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
