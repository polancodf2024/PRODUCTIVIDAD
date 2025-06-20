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
        # Leer y procesar el archivo con los nuevos campos sni y sii (VERSIÓN CORREGIDA)
        df = pd.read_csv("libros_total.csv", header=0, encoding='utf-8')
        df.columns = df.columns.str.strip()  # Limpiar espacios en nombres de columnas

        # Verificación de columnas (para diagnóstico)
        logging.info(f"Columnas detectadas: {df.columns.tolist()}")

        # Verificar que los campos importantes existen
        required_columns = ['autor_principal', 'titulo_libro', 'pub_date', 'estado', 'selected_keywords']
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

        # Selector de rango mes-año con ayuda
        st.header("📅 Selección de Periodo")
        col1, col2 = st.columns(2)

        with col1:
            start_year = st.selectbox("Año inicio",
                                   range(min_date.year, max_date.year+1),
                                   index=0,
                                   help="Selecciona el año inicial para el análisis.")
            start_month = st.selectbox("Mes inicio",
                                    range(1, 13),
                                    index=min_date.month-1,
                                    format_func=lambda x: datetime(1900, x, 1).strftime('%B'),
                                    help="Selecciona el mes inicial para el análisis.")

        with col2:
            end_year = st.selectbox("Año término",
                                  range(min_date.year, max_date.year+1),
                                  index=len(range(min_date.year, max_date.year+1))-1,
                                  help="Selecciona el año final para el análisis.")
            end_month = st.selectbox("Mes término",
                                   range(1, 13),
                                   index=max_date.month-1,
                                   format_func=lambda x: datetime(1900, x, 1).strftime('%B'),
                                   help="Selecciona el mes final para el análisis.")

        # Calcular fechas de inicio y fin
        start_day = 1
        end_day = calendar.monthrange(end_year, end_month)[1]

        date_start = datetime(start_year, start_month, start_day)
        date_end = datetime(end_year, end_month, end_day)

        # Filtrar dataframe
        filtered_df = df[(df['pub_date'] >= pd.to_datetime(date_start)) &
                       (df['pub_date'] <= pd.to_datetime(date_end))]

        # Obtener libros únicos para estadísticas precisas
        unique_libros = filtered_df.drop_duplicates(subset=['titulo_libro'])

        st.markdown(f"**Periodo seleccionado:** {date_start.strftime('%d/%m/%Y')} - {date_end.strftime('%d/%m/%Y')}",
                   help="Rango de fechas seleccionado para el análisis.")
        st.markdown(f"**Registros encontrados:** {len(filtered_df)}",
                   help="Total de registros en el periodo, incluyendo posibles duplicados del mismo libro.")
        st.markdown(f"**Libros únicos:** {len(unique_libros)}",
                   help="Cantidad de libros distintos, eliminando duplicados.")

        if len(filtered_df) != len(unique_libros):
            st.warning(f"⚠️ **Nota:** Se detectaron {len(filtered_df) - len(unique_libros)} registros duplicados del mismo libro.")

        if filtered_df.empty:
            st.warning("No hay libros en el periodo seleccionado")
            return

        # Análisis consolidado en tablas
        st.header("📊 Estadísticas Consolidadas",
                help="Métricas generales basadas en los filtros aplicados.")

        # Tabla 1: Productividad por investigador (LIBROS ÚNICOS) con participación
        st.subheader("🔍 Productividad por investigador",
                   help="Muestra cuántos libros únicos tiene cada investigador y su tipo de participación.")

        # Crear dataframe con información de participación
        investigator_stats = filtered_df.groupby('autor_principal').agg(
            Libros_Unicos=('titulo_libro', lambda x: len(set(x))),
            Participaciones=('tipo_participacion', lambda x: ', '.join(sorted(set(x))))
        ).reset_index()

        investigator_stats = investigator_stats.sort_values('Libros_Unicos', ascending=False)
        investigator_stats.columns = ['Investigador', 'Libros únicos', 'Tipo de participación']

        # Mostrar tabla con enlaces clickeables
        for index, row in investigator_stats.iterrows():
            # Crear un expander para cada investigador
            with st.expander(f"{row['Investigador']} - {row['Libros únicos']} libros"):
                # Filtrar los libros del investigador
                investigator_libros = filtered_df[filtered_df['autor_principal'] == row['Investigador']]
                unique_libros_investigator = investigator_libros.drop_duplicates(subset=['titulo_libro'])

                # Mostrar los libros (incluyendo los nuevos campos si existen)
                display_columns = ['titulo_libro', 'editorial', 'pub_date', 'isbn_issn']
                if 'sni' in unique_libros_investigator.columns and 'sii' in unique_libros_investigator.columns:
                    display_columns.extend(['sni', 'sii'])
                if 'nombramiento' in unique_libros_investigator.columns:
                    display_columns.append('nombramiento')

                st.write(f"Libros de {row['Investigador']}:")
                mostrar_tabla_uniforme(unique_libros_investigator[display_columns], "")

                # Opción para descargar en CSV
                csv = unique_libros_investigator.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Descargar producción de libros en CSV",
                    data=csv,
                    file_name=f"libros_{row['Investigador'].replace(' ', '_')}.csv",
                    mime='text/csv',
                    key=f"download_{index}"
                )

        # Tabla 2: Editoriales más utilizadas (LIBROS ÚNICOS)
        editorial_stats = unique_libros.groupby('editorial').agg(
            Total_Libros=('editorial', 'size')
        ).reset_index()
        editorial_stats = editorial_stats.sort_values('Total_Libros', ascending=False)
        editorial_stats.columns = ['Editorial', 'Libros únicos']
        mostrar_tabla_uniforme(
            editorial_stats,
            "🏢 Editoriales más utilizadas",
            "Listado de editoriales ordenadas por cantidad de libros publicados"
        )

        # Tabla 3: Tipos de participación más comunes (LIBROS ÚNICOS)
        participacion_stats = unique_libros['tipo_participacion'].value_counts().reset_index()
        participacion_stats.columns = ['Tipo de participación', 'Libros únicos']
        mostrar_tabla_uniforme(
            participacion_stats,
            "🎭 Participación de los autores",
            "Distribución de los tipos de participación en los libros"
        )

        # Tabla 4: Enfoques más frecuentes (LIBROS ÚNICOS)
        try:
            all_keywords = []
            for keywords in unique_libros['selected_keywords']:
                if pd.notna(keywords):
                    # Procesamiento mejorado de palabras clave
                    keywords_str = str(keywords).strip()
                    if keywords_str.startswith('[') and keywords_str.endswith(']'):
                        # Es una lista en formato de cadena
                        keywords_str = keywords_str[1:-1]  # Eliminar corchetes
                        # Dividir por comas que no estén dentro de comillas
                        import re
                        keyword_list = re.split(r",\s*(?=(?:[^']*'[^']*')*[^']*$)", keywords_str)
                        keyword_list = [k.strip().strip("'\"") for k in keyword_list if k.strip()]
                        all_keywords.extend(keyword_list)
                    else:
                        # Es una cadena simple, dividir por comas
                        keyword_list = [k.strip() for k in keywords_str.split(",") if k.strip()]
                        all_keywords.extend(keyword_list)

            keyword_stats = pd.Series(all_keywords).value_counts().reset_index()
            keyword_stats.columns = ['Enfoque', 'Frecuencia']
            mostrar_tabla_uniforme(
                keyword_stats,
                "🧪 Líneas de investigación mas frecuentes",
                "Líneas de investigación más utilizadas en los libros, indicando las áreas de investigación predominantes"
            )
        except Exception as e:
            st.warning(f"No se pudieron procesar las palabras clave: {str(e)}")
            logging.error(f"Error procesando palabras clave: {str(e)}")

        # Tabla 5: Distribución por departamentos (LIBROS ÚNICOS)
        if 'departamento' in unique_libros.columns:
            depto_stats = unique_libros['departamento'].value_counts().reset_index()
            depto_stats.columns = ['Departamento', 'Libros únicos']
            mostrar_tabla_uniforme(
                depto_stats,
                "🏛️ Distribución por departamento de adscripción",
                "Clasificación de libros según el departamento de adscripción del autor principal"
            )
        else:
            st.warning("El campo 'departamento' no está disponible en los datos")

        # Tabla 6: Distribución temporal (LIBROS ÚNICOS)
        time_stats = unique_libros['pub_date'].dt.to_period('M').astype(str).value_counts().sort_index().reset_index()
        time_stats.columns = ['Mes-Año', 'Libros únicos']
        mostrar_tabla_uniforme(
            time_stats,
            "🕰️ Distribución mensual",
            "Evolución mensual de la producción de libros en el periodo seleccionado"
        )

        # Tabla 7: Distribución por nivel SNI (LIBROS ÚNICOS)
        if 'sni' in unique_libros.columns:
            sni_stats = unique_libros['sni'].value_counts().reset_index()
            sni_stats.columns = ['Nivel SNI', 'Libros únicos']
            mostrar_tabla_uniforme(
                sni_stats,
                "📊 Distribución por nivel SNI",
                "Clasificación de libros según el nivel del Sistema Nacional de Investigadores (SNI) de los autores"
            )
        else:
            st.warning("El campo 'sni' no está disponible en los datos")

        # Tabla 8: Distribución por nivel SII (LIBROS ÚNICOS)
        if 'sii' in unique_libros.columns:
            sii_stats = unique_libros['sii'].value_counts().reset_index()
            sii_stats.columns = ['Nivel SII', 'Libros únicos']
            mostrar_tabla_uniforme(
                sii_stats,
                "📈 Distribución por nivel SII",
                "Clasificación de libros según el nivel del Sistema Institucional de Investigación (SII) de los autores"
            )
        else:
            st.warning("El campo 'sii' no está disponible en los datos")

        # Tabla 9: Distribución por nombramiento (NUEVA TABLA)
        if 'nombramiento' in unique_libros.columns:
            nombramiento_stats = unique_libros['nombramiento'].value_counts().reset_index()
            nombramiento_stats.columns = ['Tipo de Nombramiento', 'Libros únicos']
            mostrar_tabla_uniforme(
                nombramiento_stats,
                "👔 Distribución por nombramiento del autor",
                "Clasificación de libros según el tipo de nombramiento del autor principal"
            )
        else:
            st.warning("El campo 'nombramiento' no está disponible en los datos")

        # Tabla 10: Distribución por países de distribución (LIBROS ÚNICOS)
        if 'paises_distribucion' in unique_libros.columns:
            try:
                all_countries = []
                for countries in unique_libros['paises_distribucion']:
                    if pd.notna(countries):
                        cleaned = str(countries).strip().split(", ")
                        all_countries.extend([c.strip() for c in cleaned if c.strip()])

                country_stats = pd.Series(all_countries).value_counts().reset_index()
                country_stats.columns = ['País', 'Frecuencia']
                mostrar_tabla_uniforme(
                    country_stats,
                    "🌍 Distribución por países",
                    "Países donde se distribuyen los libros publicados"
                )
            except:
                st.warning("No se pudieron procesar los países de distribución")

        # Tabla 11: Distribución por idioma (LIBROS ÚNICOS)
        if 'idiomas_disponibles' in unique_libros.columns:
            idioma_stats = unique_libros['idiomas_disponibles'].value_counts().reset_index()
            idioma_stats.columns = ['Idioma', 'Libros únicos']
            mostrar_tabla_uniforme(
                idioma_stats,
                "🌐 Distribución por idioma",
                "Idiomas en los que están publicados los libros"
            )
        else:
            st.warning("El campo 'idiomas_disponibles' no está disponible en los datos")

        # Tabla 12: Distribución por formato (LIBROS ÚNICOS)
        if 'formatos_disponibles' in unique_libros.columns:
            formato_stats = unique_libros['formatos_disponibles'].value_counts().reset_index()
            formato_stats.columns = ['Formato', 'Libros únicos']
            mostrar_tabla_uniforme(
                formato_stats,
                "📖 Distribución por tipo de formato",
                "Formatos disponibles para los libros publicados"
            )
        else:
            st.warning("El campo 'formatos_disponibles' no está disponible en los datos")

        # ==========================================
        # TABLA RESUMEN CONSOLIDADA
        # ==========================================
        st.header("📋 Resumen Consolidado de Totales")
        resumen_df = generar_tabla_resumen(unique_libros, filtered_df)
        mostrar_tabla_uniforme(resumen_df, "Resumen General")

        # ==========================================
        # SECCIÓN: DESCARGAR ARCHIVO COMPLETO
        # ==========================================
        st.header("📥 Descargar Datos Completos")

        # Opción para descargar el archivo pro_libros_total.csv
        if Path("libros_total.csv").exists():
            with open("libros_total.csv", "rb") as file:
                btn = st.download_button(
                    label="Descargar archivo pro_libros_total.csv completo",
                    data=file,
                    file_name="pro_libros_total.csv",
                    mime="text/csv",
                    help="Descarga el archivo CSV completo con todos los datos de libros"
                )
            if btn:
                st.success("Descarga iniciada")
        else:
            st.warning("El archivo libros_total.csv no está disponible para descargar")

    except Exception as e:
        st.error(f"Error al procesar el archivo: {str(e)}")
        logging.error(f"Error en main: {str(e)}")

if __name__ == "__main__":
    main()
