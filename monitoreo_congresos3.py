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
    filename='monitoreo_congresos.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ====================
# CATEGORÍAS DE KEYWORDS PARA CONGRESOS
# ====================
KEYWORD_CATEGORIES = {
    "Cardiología": ["cardio", "corazón", "miocardio", "arritmia", "isquemia", "hipertensión", "ECG", "insuficiencia cardíaca"],
    "Neurología": ["neuro", "cerebro", "ictus", "alzheimer", "parkinson", "demencia"],
    # ... (puedes agregar más categorías relevantes)
}

# ====================
# CONFIGURACIÓN INICIAL
# ====================
class Config:
    def __init__(self):
        # Configuración SFTP
        self.REMOTE_CONGRESOS_FILE = "pro_congresos_total.csv"  # Nombre completo del archivo remoto
        self.REMOTE_GENERADOR_PATH = f"{st.secrets['sftp']['dir']}/{st.secrets['prefixes']['generadorcongresos']}"
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
    """Ejecuta el script generadorcongresos.sh en el servidor remoto"""
    ssh = None
    try:
        with st.spinner("🔄 Ejecutando generadorcongresos.sh en servidor remoto..."):
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
                logging.error(f"Error ejecutando generadorcongresos.sh: {error_msg}")
                return False

            logging.info("Script ejecutado correctamente")
            
            # 4. Verificar que el archivo se creó en la ubicación correcta
            sftp = ssh.open_sftp()
            output_path = os.path.join(CONFIG.REMOTE['DIR'], CONFIG.REMOTE_CONGRESOS_FILE)
            try:
                sftp.stat(output_path)
                file_size = sftp.stat(output_path).st_size
                logging.info(f"Archivo creado en: {output_path} (Tamaño: {file_size} bytes)")
                st.success("✅ generadorcongresos.sh ejecutado correctamente en el servidor")
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

def sync_congresos_file():
    """Sincroniza el archivo pro_congresos_total.csv desde el servidor remoto"""
    try:
        remote_path = os.path.join(CONFIG.REMOTE['DIR'], CONFIG.REMOTE_CONGRESOS_FILE)
        local_path = "pro_congresos_total.csv"
        
        with st.spinner("🔄 Sincronizando archivo pro_congresos_total.csv desde el servidor..."):
            if SSHManager.download_remote_file(remote_path, local_path):
                st.success("✅ Archivo pro_congresos_total.csv sincronizado correctamente")
                return True
            else:
                st.error("❌ No se pudo descargar el archivo pro_congresos_total.csv del servidor")
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

def generar_tabla_resumen(unique_congresos, filtered_df):
    """Genera una tabla consolidada con todos los totales"""
    datos_resumen = []
    
    # 1. Total presentaciones únicas
    total_presentaciones = len(unique_congresos)
    datos_resumen.append(("Presentaciones únicas", total_presentaciones))
    
    # 2. Congresos distintos
    total_congresos = unique_congresos['titulo_congreso'].nunique()
    datos_resumen.append(("Congresos distintos", total_congresos))
    
    # 3. Tipos de congreso
    total_tipos = unique_congresos['tipo_congreso'].nunique()
    datos_resumen.append(("Tipos de congreso distintos", total_tipos))
    
    # 4. Países
    total_paises = unique_congresos['pais'].nunique()
    datos_resumen.append(("Países distintos", total_paises))
    
    # 5. Líneas de investigación
    try:
        all_keywords = []
        for keywords in unique_congresos['linea_investigacion']:
            if pd.notna(keywords):
                keywords_str = str(keywords).strip()
                keyword_list = [k.strip() for k in keywords_str.split(",") if k.strip()]
                all_keywords.extend(keyword_list)
        total_keywords = len(set(all_keywords)) if all_keywords else 0
        datos_resumen.append(("Líneas de investigación distintas", total_keywords))
    except:
        datos_resumen.append(("Líneas de investigación distintas", "N/D"))
    
    # 6. Departamentos
    if 'departamento' in unique_congresos.columns:
        total_deptos = unique_congresos['departamento'].nunique()
        datos_resumen.append(("Departamentos distintos", total_deptos))
    
    # 7. Años con participación
    total_años = unique_congresos['año_congreso'].nunique()
    datos_resumen.append(("Años con participación", total_años))
    
    # 8. Nivel SNI
    if 'sni' in unique_congresos.columns:
        total_sni = unique_congresos['sni'].nunique()
        datos_resumen.append(("Niveles SNI distintos", total_sni))
    
    # 9. Nivel SII
    if 'sii' in unique_congresos.columns:
        total_sii = unique_congresos['sii'].nunique()
        datos_resumen.append(("Niveles SII distintos", total_sii))
    
    # 10. Nombramientos
    if 'nombramiento' in unique_congresos.columns:
        total_nombramientos = unique_congresos['nombramiento'].nunique()
        datos_resumen.append(("Tipos de nombramiento distintos", total_nombramientos))
    
    # 11. Roles
    if 'rol' in unique_congresos.columns:
        total_roles = unique_congresos['rol'].nunique()
        datos_resumen.append(("Roles distintos", total_roles))
    
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
# FUNCIONES DE MÉTRICAS PARA CONGRESOS
# ====================
def indice_prestigio_congreso(titulo_congreso, tipo_congreso):
    """Calcula el Índice de Prestigio del Congreso (IPC)"""
    if pd.isna(titulo_congreso) or pd.isna(tipo_congreso):
        return 0.3

    titulo_congreso = str(titulo_congreso).lower()
    tipo_congreso = str(tipo_congreso).lower()

    # Congresos internacionales conocidos
    congresos_tier1 = [
        'american heart', 'european society of cardiology', 
        'world congress of cardiology', 'international congress'
    ]
    
    # Congresos nacionales importantes
    congresos_tier2 = [
        'congreso nacional', 'sociedad mexicana', 
        'reunión anual', 'simposio nacional'
    ]

    # Asignar puntaje según tipo y nombre del congreso
    if tipo_congreso == 'internacional':
        if any(keyword in titulo_congreso for keyword in congresos_tier1):
            return 1.0
        return 0.7
    elif tipo_congreso == 'nacional':
        if any(keyword in titulo_congreso for keyword in congresos_tier2):
            return 0.6
        return 0.4
    else:
        return 0.3  # Congresos locales o no clasificados

def coeficiente_internacionalizacion(pais, tipo_congreso):
    """Calcula el Coeficiente de Internacionalización (CI) para congresos"""
    if pd.isna(pais) or pd.isna(tipo_congreso):
        return 0.0

    tipo_congreso = str(tipo_congreso).lower()
    pais = str(pais).strip()

    # Países con mayor prestigio en investigación
    paises_tier1 = ['Estados Unidos', 'Reino Unido', 'Alemania', 'Japón', 'Canadá']
    paises_tier2 = ['Francia', 'Italia', 'España', 'Australia', 'Suiza']

    if tipo_congreso == 'internacional':
        if pais in paises_tier1:
            return 1.0
        elif pais in paises_tier2:
            return 0.8
        else:
            return 0.6
    else:
        return 0.3  # Congresos nacionales o locales

def indice_relevancia_tematica(linea_investigacion):
    """Calcula el Índice de Relevancia Temática (IRT) para cardiología"""
    if pd.isna(linea_investigacion):
        return 0.0

    keywords_cardio = [
        "cardíaco", "miocardio", "arritmia", "isquemia",
        "hipertensión", "ECG", "insuficiencia cardíaca",
        "coronario", "válvula", "aterosclerosis", "angina"
    ]

    try:
        if isinstance(linea_investigacion, str):
            linea_investigacion = linea_investigacion.lower()
            matches = sum(1 for kw in keywords_cardio if kw in linea_investigacion)
            return min(matches / len(keywords_cardio), 1.0)  # Normalizado a 0-1
        return 0.0
    except:
        return 0.0

def main():
    st.set_page_config(
        page_title="Análisis de Congresos",
        page_icon="📊",
        layout="wide"
    )

    # Añadir logo en la parte superior
    if Path(CONFIG.LOGO_PATH).exists():
        st.image(CONFIG.LOGO_PATH, width=200)

    st.title("Análisis de Participación en Congresos")

    # Paso 1: Ejecutar generador remoto para actualizar datos
    if not ejecutar_generador_remoto():
        st.warning("⚠️ Continuando con datos existentes (pueden no estar actualizados)")

    # Paso 2: Sincronizar archivo pro_congresos_total.csv
    if not sync_congresos_file():
        st.warning("⚠️ Trabajando con copia local de pro_congresos_total.csv debido a problemas de conexión")

    # Verificar si el archivo local existe
    if not Path("pro_congresos_total.csv").exists():
        st.error("No se encontró el archivo pro_congresos_total.csv")
        return

    try:
        # Leer y procesar el archivo
        df = pd.read_csv("pro_congresos_total.csv", header=0, encoding='utf-8')
        df.columns = df.columns.str.strip()

        # Verificar campos importantes
        required_columns = ['economic_number', 'titulo_presentacion', 'titulo_congreso', 
                          'tipo_congreso', 'pais', 'año_congreso', 'fecha_exacta_congreso',
                          'rol', 'linea_investigacion', 'pdf_filename', 'estado']
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            st.warning(f"El archivo pro_congresos_total.csv no contiene los campos requeridos: {', '.join(missing_columns)}")
            return

        # Convertir y validar fechas
        df['fecha_exacta_congreso'] = pd.to_datetime(df['fecha_exacta_congreso'], errors='coerce')
        df = df[df['estado'] == 'A'].copy()

        if df.empty:
            st.warning("No hay congresos válidos para analizar")
            return

        st.success(f"Datos cargados correctamente. Registros activos: {len(df)}")

        # Obtener rangos de fechas disponibles
        min_date = df['fecha_exacta_congreso'].min()
        max_date = df['fecha_exacta_congreso'].max()

        # Selector de rango de fechas
        st.header("📅 Selección de Periodo")
        col1, col2 = st.columns(2)

        with col1:
            start_date = st.date_input("Fecha inicio", min_date)

        with col2:
            end_date = st.date_input("Fecha término", max_date)

        # Filtrar dataframe
        filtered_df = df[(df['fecha_exacta_congreso'] >= pd.to_datetime(start_date)) &
                       (df['fecha_exacta_congreso'] <= pd.to_datetime(end_date))].copy()

        # Obtener presentaciones únicas (basado en título de presentación)
        unique_congresos = filtered_df.drop_duplicates(subset=['titulo_presentacion']).copy()

        st.markdown(f"**Periodo seleccionado:** {start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')}")
        st.markdown(f"**Registros encontrados:** {len(filtered_df)}")
        st.markdown(f"**Presentaciones únicas:** {len(unique_congresos)}")

        if filtered_df.empty:
            st.warning("No hay congresos en el periodo seleccionado")
            return

        # =============================================
        # TABLA DE PRODUCTIVIDAD POR INVESTIGADOR
        # =============================================
        st.header("🔍 Productividad por investigador")
        investigator_stats = filtered_df.groupby(['economic_number']).agg(
            Presentaciones_Unicas=('titulo_presentacion', lambda x: len(set(x))),
            Congresos_Distintos=('titulo_congreso', 'nunique'),
            Paises_Visitados=('pais', 'nunique'),
            Roles=('rol', lambda x: ', '.join(sorted(set(x))))
        ).reset_index()
        
        # Agregar información de nombramiento, SNI, SII si existe
        if 'nombramiento' in df.columns and 'sni' in df.columns and 'sii' in df.columns:
            investigator_info = df[['economic_number', 'nombramiento', 'sni', 'sii']].drop_duplicates()
            investigator_stats = pd.merge(investigator_stats, investigator_info, on='economic_number', how='left')
        
        investigator_stats = investigator_stats.sort_values('Presentaciones_Unicas', ascending=False)
        investigator_stats.columns = ['Número económico', 'Presentaciones únicas', 'Congresos distintos', 
                                    'Países visitados', 'Roles', 'Nombramiento', 'SNI', 'SII']

        mostrar_tabla_uniforme(investigator_stats, "Productividad por investigador")

        # Detalle expandible por investigador
        for index, row in investigator_stats.iterrows():
            with st.expander(f"{row['Número económico']} - {row['Presentaciones únicas']} presentaciones"):
                investigator_congresos = filtered_df[filtered_df['economic_number'] == row['Número económico']]
                unique_congresos_investigator = investigator_congresos.drop_duplicates(subset=['titulo_presentacion'])

                display_columns = ['titulo_presentacion', 'titulo_congreso', 'tipo_congreso', 
                                 'pais', 'fecha_exacta_congreso', 'rol']
                if 'sni' in unique_congresos_investigator.columns and 'sii' in unique_congresos_investigator.columns:
                    display_columns.extend(['sni', 'sii'])
                if 'nombramiento' in unique_congresos_investigator.columns:
                    display_columns.append('nombramiento')

                st.write(f"Presentaciones de {row['Número económico']}:")
                mostrar_tabla_uniforme(unique_congresos_investigator[display_columns], "")

                # Sección de archivos PDF
                st.subheader("📄 Archivos disponibles")
                pdf_files = unique_congresos_investigator['pdf_filename'].dropna().unique()

                if len(pdf_files) > 0:
                    st.info(f"Se encontraron {len(pdf_files)} archivos para este investigador")
                    selected_pdf = st.selectbox(
                        "Seleccione un archivo para ver:",
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
                                label="Descargar este archivo",
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
                    st.warning("No se encontraron archivos PDF para este investigador")

                csv = unique_congresos_investigator.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Descargar participación en congresos (CSV)",
                    data=csv,
                    file_name=f"congresos_{row['Número económico']}.csv",
                    mime='text/csv',
                    key=f"download_csv_{row['Número económico']}_{index}"
                )

        # =============================================
        # SECCIÓN DE MÉTRICAS DE CALIDAD
        # =============================================
        st.header("📊 Métricas de Calidad de Participación")

        # Calcular métricas para cada presentación única
        with st.spinner("Calculando métricas de calidad..."):
            unique_congresos = unique_congresos.assign(
                IPC=unique_congresos.apply(lambda x: indice_prestigio_congreso(x['titulo_congreso'], x['tipo_congreso']), axis=1),
                CI=unique_congresos.apply(lambda x: coeficiente_internacionalizacion(x['pais'], x['tipo_congreso']), axis=1),
                IRT=unique_congresos['linea_investigacion'].apply(indice_relevancia_tematica)
            )
            unique_congresos = unique_congresos.assign(
                PI=0.5 * unique_congresos['IPC'] + 0.3 * unique_congresos['CI'] + 0.2 * unique_congresos['IRT']
            )

        # Mostrar tabla de resultados por investigador
        st.subheader("Métricas por Investigador")
        metrics_by_investigator = unique_congresos.groupby('economic_number').agg({
            'IPC': 'mean',
            'CI': 'mean',
            'IRT': 'mean',
            'PI': 'mean',
            'titulo_presentacion': 'count'
        }).reset_index()

        metrics_by_investigator.columns = [
            'Número económico',
            'Prestigio Congreso (IPC) Promedio',
            'Internacionalización (CI) Promedio',
            'Relevancia Temática (IRT) Promedio',
            'Puntaje Integrado (PI) Promedio',
            'Presentaciones Evaluadas'
        ]

        metrics_by_investigator = metrics_by_investigator.sort_values('Puntaje Integrado (PI) Promedio', ascending=False)
        metrics_by_investigator = metrics_by_investigator.round(2)

        mostrar_tabla_uniforme(metrics_by_investigator, "Resumen de Métricas por Investigador")

        # Botón para explicación de métricas
        with st.expander("ℹ️ Explicación de las Métricas", expanded=False):
            st.markdown("""
            ### Índice de Prestigio del Congreso (IPC)
            **Fórmula:**
            Clasificación de congresos en 4 niveles con valores de 0.3 a 1.0
            - **1.0**: Congresos internacionales de alto prestigio
            - **0.7**: Congresos internacionales generales
            - **0.6**: Congresos nacionales importantes
            - **0.4**: Congresos nacionales generales
            - **0.3**: Otros eventos

            **Propósito:** Evaluar el prestigio del congreso donde se participó.
            """)

            st.markdown("""
            ### Coeficiente de Internacionalización (CI)
            **Fórmula:**
            - Internacional: 0.6-1.0 (según país)
            - Nacional: 0.3

            **Propósito:** Medir alcance geográfico de la participación.
            """)

            st.markdown("""
            ### Índice de Relevancia Temática (IRT)
            **Fórmula:**
            IRT = (N° términos de cardiología) / (Total términos relevantes)

            **Propósito:** Evaluar relación con área de cardiología.
            """)

            st.markdown("""
            ### Puntaje Integrado (PI)
            **Fórmula:**
            PI = (0.5 × IPC) + (0.3 × CI) + (0.2 × IRT)

            **Interpretación:**
            0.8-1.0: Excelente | 0.6-0.79: Bueno | 0.4-0.59: Aceptable | <0.4: Bajo
            """)

        # Mostrar tabla completa de presentaciones con métricas
        st.subheader("Resultados Detallados por Presentación")
        metricas_df = unique_congresos[[
            'titulo_presentacion', 'economic_number', 'titulo_congreso', 'tipo_congreso',
            'IPC', 'CI', 'IRT', 'PI'
        ]].sort_values('PI', ascending=False)

        metricas_df.columns = [
            'Título Presentación', 'Número económico', 'Congreso', 'Tipo Congreso',
            'Prestigio Congreso (IPC)', 'Internacionalización (CI)',
            'Relevancia Temática (IRT)', 'Puntaje Integrado (PI)'
        ]

        metricas_df = metricas_df.round(2)
        mostrar_tabla_uniforme(metricas_df, "")

        # =============================================
        # TOP 5 PRESENTACIONES POR CALIDAD
        # =============================================
        st.header("🏆 Presentaciones Destacadas")
        top_presentaciones = unique_congresos.nlargest(5, 'PI')[[
            'titulo_presentacion', 'economic_number', 'titulo_congreso', 'PI', 'IPC', 'CI', 'IRT'
        ]]
        top_presentaciones.columns = [
            'Título Presentación', 'Número económico', 'Congreso',
            'Puntaje Integrado', 'Prestigio Congreso',
            'Internacionalización', 'Relevancia Temática'
        ]

        mostrar_tabla_uniforme(top_presentaciones.round(2), "Top 5 presentaciones por Calidad Integral")

        # Botón para criterios de selección
        with st.expander("ℹ️ Criterios de Selección", expanded=False):
            st.markdown("""
            **Presentaciones destacadas** se seleccionan por mayor Puntaje Integrado (PI) que combina:
            - 50% Prestigio del congreso (IPC)
            - 30% Internacionalización (CI)
            - 20% Relevancia temática (IRT)

            Las presentaciones con PI ≥ 0.8 tienen calidad excepcional en los tres aspectos evaluados.
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
                file_name="metricas_calidad_congresos.csv",
                mime='text/csv'
            )

        with col2:
            if Path("pro_congresos_total.csv").exists():
                with open("pro_congresos_total.csv", "rb") as file:
                    st.download_button(
                        label="Descargar dataset completo",
                        data=file,
                        file_name="pro_congresos_total.csv",
                        mime="text/csv"
                    )

        with col3:
            # Botón para descargar todos los PDFs con prefijo CON
            if st.button("Descargar todos los PDFs (CON)"):
                with st.spinner("Buscando archivos PDF en el servidor..."):
                    ssh = SSHManager.get_connection()
                    if ssh:
                        try:
                            with ssh.open_sftp() as sftp:
                                sftp.chdir(CONFIG.REMOTE['DIR'])
                                pdf_files = []
                                for filename in sftp.listdir():
                                    if (filename.startswith('CON')) and filename.lower().endswith('.pdf'):
                                        pdf_files.append(filename)

                                if not pdf_files:
                                    st.warning("No se encontraron archivos PDF con prefijo CON")
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
                                        file_name="pdfs_congresos.zip",
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
