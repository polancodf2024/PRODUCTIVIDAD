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
    filename='monitoreo_capitulos.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ====================
# CATEGORÍAS DE KEYWORDS PARA CAPÍTULOS
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
        self.REMOTE_CAPITULOS_FILE = "pro_capitulos_total.csv"  # Nombre completo del archivo remoto
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
    
    # 5. Departamentos (si existe)
    if 'departamento' in unique_capitulos.columns:
        total_deptos = unique_capitulos['departamento'].nunique()
        datos_resumen.append(("Departamentos distintos", total_deptos))
    
    # 6. Distribución temporal (meses)
    total_meses = unique_capitulos['pub_date'].dt.to_period('M').nunique()
    datos_resumen.append(("Meses con publicaciones", total_meses))
    
    # 7. Nivel SNI (si existe)
    if 'sni' in unique_capitulos.columns:
        total_sni = unique_capitulos['sni'].nunique()
        datos_resumen.append(("Niveles SNI distintos", total_sni))
    
    # 8. Nivel SII (si existe)
    if 'sii' in unique_capitulos.columns:
        total_sii = unique_capitulos['sii'].nunique()
        datos_resumen.append(("Niveles SII distintos", total_sii))
    
    # 9. Nombramientos (si existe)
    if 'nombramiento' in unique_capitulos.columns:
        total_nombramientos = unique_capitulos['nombramiento'].nunique()
        datos_resumen.append(("Tipos de nombramiento distintos", total_nombramientos))
    
    # 10. Países de distribución (si existe)
    if 'paises_distribucion' in unique_capitulos.columns:
        try:
            all_countries = []
            for countries in unique_capitulos['paises_distribucion']:
                if pd.notna(countries):
                    cleaned = str(countries).strip().split(", ")
                    all_countries.extend([c.strip() for c in cleaned if c.strip()])
            total_paises = len(set(all_countries)) if all_countries else 0
            datos_resumen.append(("Países de distribución distintos", total_paises))
        except:
            datos_resumen.append(("Países de distribución distintos", "N/D"))
    
    # 11. Idiomas (si existe)
    if 'idiomas_disponibles' in unique_capitulos.columns:
        total_idiomas = unique_capitulos['idiomas_disponibles'].nunique()
        datos_resumen.append(("Idiomas distintos", total_idiomas))
    
    # 12. Formatos (si existe)
    if 'formatos_disponibles' in unique_capitulos.columns:
        total_formatos = unique_capitulos['formatos_disponibles'].nunique()
        datos_resumen.append(("Formatos distintos", total_formatos))
    
    # 13. Libros distintos
    if 'titulo_libro' in unique_capitulos.columns:
        total_libros = unique_capitulos['titulo_libro'].nunique()
        datos_resumen.append(("Libros distintos", total_libros))
    
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
        page_title="Análisis de Capítulos de Libros",
        page_icon="📖",
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
        required_columns = ['autor_principal', 'titulo_libro', 'titulo_capitulo', 'pub_date', 'estado', 'selected_keywords', 'economic_number']
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            st.warning(f"El archivo capitulos_total.csv no contiene los campos requeridos: {', '.join(missing_columns)}")
            return

        # Convertir y validar fechas
        df['pub_date'] = pd.to_datetime(df['pub_date'], errors='coerce')
        df = df[(df['estado'] == 'A') & (df['pub_date'].notna())]

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
                       (df['pub_date'] <= pd.to_datetime(date_end))]

        # Obtener capítulos únicos (basados en título de capítulo y libro)
        unique_capitulos = filtered_df.drop_duplicates(subset=['titulo_capitulo', 'titulo_libro'])

        st.markdown(f"**Periodo seleccionado:** {date_start.strftime('%d/%m/%Y')} - {date_end.strftime('%d/%m/%Y')}")
        st.markdown(f"**Registros encontrados:** {len(filtered_df)}")
        st.markdown(f"**Capítulos únicos:** {len(unique_capitulos)}")
        st.markdown(f"**Libros distintos:** {unique_capitulos['titulo_libro'].nunique()}")

        if filtered_df.empty:
            st.warning("No hay capítulos en el periodo seleccionado")
            return

        # Análisis consolidado en tablas
        st.header("📊 Estadísticas Consolidadas")

        # Tabla 1: Productividad por investigador
        st.subheader("🔍 Productividad por investigador")
        investigator_stats = filtered_df.groupby(['autor_principal', 'economic_number']).agg(
            Capítulos_Unicos=('titulo_capitulo', lambda x: len(set(x))),
            Libros_Unicos=('titulo_libro', lambda x: len(set(x))),
            Participaciones=('tipo_participacion', lambda x: ', '.join(sorted(set(x))))
        ).reset_index()
        investigator_stats = investigator_stats.sort_values('Capítulos_Unicos', ascending=False)
        investigator_stats.columns = ['Investigador', 'Número económico', 'Capítulos únicos', 'Libros distintos', 'Tipo de participación']

        for index, row in investigator_stats.iterrows():
            with st.expander(f"{row['Investigador']} - {row['Capítulos únicos']} capítulos en {row['Libros distintos']} libros"):
                investigator_capitulos = filtered_df[filtered_df['autor_principal'] == row['Investigador']]
                unique_capitulos_investigator = investigator_capitulos.drop_duplicates(subset=['titulo_capitulo', 'titulo_libro'])

                display_columns = ['titulo_libro', 'titulo_capitulo', 'editorial', 'pub_date', 'isbn_issn']
                if 'sni' in unique_capitulos_investigator.columns and 'sii' in unique_capitulos_investigator.columns:
                    display_columns.extend(['sni', 'sii'])
                if 'nombramiento' in unique_capitulos_investigator.columns:
                    display_columns.append('nombramiento')

                st.write(f"Capítulos de {row['Investigador']}:")
                mostrar_tabla_uniforme(unique_capitulos_investigator[display_columns], "")

                # SECCIÓN DE PORTADAS PDF
                st.subheader("📄 Portadas disponibles")
                economic_number = row['Número económico']
                remote_pdfs = []

                ssh = SSHManager.get_connection()
                if ssh:
                    try:
                        with ssh.open_sftp() as sftp:
                            try:
                                remote_files = sftp.listdir(CONFIG.REMOTE['DIR'])
                                remote_pdfs = [f for f in remote_files if f.endswith(f".{economic_number}.pdf")]
                                remote_pdfs.sort(reverse=True)  # Ordenar de más reciente a más antiguo
                            except Exception as e:
                                st.warning(f"No se pudieron listar los archivos PDF: {str(e)}")
                    except Exception as e:
                        st.warning(f"Error al acceder a SFTP: {str(e)}")
                    finally:
                        ssh.close()

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
                                # Solución para PyPDF2: Mostrar solo el botón de descarga si no está instalado
                                 st.warning("Seleccione el archivo PDF que quiere revisar y bájelo a su dispositivo.")
                            except Exception as e:
                                st.warning(f"No se pudo mostrar vista previa: {str(e)}")

                            try:
                                os.remove(temp_pdf_path)
                            except:
                                pass
                        else:
                            st.error("No se pudo descargar el PDF seleccionado")
                else:
                    st.warning("No se encontraron portadas PDF para este investigador")

                # Descargar CSV
                csv = unique_capitulos_investigator.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Descargar producción de capítulos en CSV",
                    data=csv,
                    file_name=f"capitulos_{row['Investigador'].replace(' ', '_')}.csv",
                    mime='text/csv',
                    key=f"download_csv_{economic_number}_{index}"
                )

        # =============================================
        # TABLAS DE ESTADÍSTICAS (sección restaurada)
        # =============================================

        # Tabla 2: Editoriales más utilizadas
        st.subheader("🏢 Editoriales más utilizadas")
        editorial_stats = unique_capitulos.groupby('editorial').agg(
            Total_Capitulos=('editorial', 'size'),
            Total_Libros=('titulo_libro', 'nunique')
        ).reset_index()
        editorial_stats = editorial_stats.sort_values('Total_Capitulos', ascending=False)
        editorial_stats.columns = ['Editorial', 'Capítulos únicos', 'Libros distintos']
        mostrar_tabla_uniforme(editorial_stats, "")

        # Tabla 3: Tipos de participación
        st.subheader("🎭 Participación de los autores")
        participacion_stats = unique_capitulos['tipo_participacion'].value_counts().reset_index()
        participacion_stats.columns = ['Tipo de participación', 'Capítulos únicos']
        mostrar_tabla_uniforme(participacion_stats, "")

        # Tabla 4: Líneas de investigación
        st.subheader("🧪 Líneas de investigación mas frecuentes")
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

            keyword_stats = pd.Series(all_keywords).value_counts().reset_index()
            keyword_stats.columns = ['Enfoque', 'Frecuencia']
            mostrar_tabla_uniforme(keyword_stats, "")
        except Exception as e:
            st.warning(f"No se pudieron procesar las palabras clave: {str(e)}")

        # Tabla 5: Distribución por departamentos
        if 'departamento' in unique_capitulos.columns:
            st.subheader("🏛️ Distribución por departamento de adscripción")
            depto_stats = unique_capitulos['departamento'].value_counts().reset_index()
            depto_stats.columns = ['Departamento', 'Capítulos únicos']
            mostrar_tabla_uniforme(depto_stats, "")
        else:
            st.warning("El campo 'departamento' no está disponible en los datos")

        # Tabla 6: Distribución temporal
        st.subheader("🕰️ Distribución mensual")
        time_stats = unique_capitulos['pub_date'].dt.to_period('M').astype(str).value_counts().sort_index().reset_index()
        time_stats.columns = ['Mes-Año', 'Capítulos únicos']
        mostrar_tabla_uniforme(time_stats, "")

        # Tabla 7: Distribución por nivel SNI
        if 'sni' in unique_capitulos.columns:
            st.subheader("📊 Distribución por nivel SNI")
            sni_stats = unique_capitulos['sni'].value_counts().reset_index()
            sni_stats.columns = ['Nivel SNI', 'Capítulos únicos']
            mostrar_tabla_uniforme(sni_stats, "")
        else:
            st.warning("El campo 'sni' no está disponible en los datos")

        # Tabla 8: Distribución por nivel SII
        if 'sii' in unique_capitulos.columns:
            st.subheader("📈 Distribución por nivel SII")
            sii_stats = unique_capitulos['sii'].value_counts().reset_index()
            sii_stats.columns = ['Nivel SII', 'Capítulos únicos']
            mostrar_tabla_uniforme(sii_stats, "")
        else:
            st.warning("El campo 'sii' no está disponible en los datos")

        # Tabla 9: Distribución por nombramiento
        if 'nombramiento' in unique_capitulos.columns:
            st.subheader("👔 Distribución por nombramiento del autor")
            nombramiento_stats = unique_capitulos['nombramiento'].value_counts().reset_index()
            nombramiento_stats.columns = ['Tipo de Nombramiento', 'Capítulos únicos']
            mostrar_tabla_uniforme(nombramiento_stats, "")
        else:
            st.warning("El campo 'nombramiento' no está disponible en los datos")

        # Tabla 10: Distribución por países
        if 'paises_distribucion' in unique_capitulos.columns:
            st.subheader("🌍 Distribución por países")
            try:
                all_countries = []
                for countries in unique_capitulos['paises_distribucion']:
                    if pd.notna(countries):
                        cleaned = str(countries).strip().split(", ")
                        all_countries.extend([c.strip() for c in cleaned if c.strip()])

                country_stats = pd.Series(all_countries).value_counts().reset_index()
                country_stats.columns = ['País', 'Frecuencia']
                mostrar_tabla_uniforme(country_stats, "")
            except:
                st.warning("No se pudieron procesar los países de distribución")

        # Tabla 11: Distribución por idioma
        if 'idiomas_disponibles' in unique_capitulos.columns:
            st.subheader("🌐 Distribución por idioma")
            idioma_stats = unique_capitulos['idiomas_disponibles'].value_counts().reset_index()
            idioma_stats.columns = ['Idioma', 'Capítulos únicos']
            mostrar_tabla_uniforme(idioma_stats, "")
        else:
            st.warning("El campo 'idiomas_disponibles' no está disponible en los datos")

        # Tabla 12: Distribución por formato
        if 'formatos_disponibles' in unique_capitulos.columns:
            st.subheader("📖 Distribución por tipo de formato")
            formato_stats = unique_capitulos['formatos_disponibles'].value_counts().reset_index()
            formato_stats.columns = ['Formato', 'Capítulos únicos']
            mostrar_tabla_uniforme(formato_stats, "")
        else:
            st.warning("El campo 'formatos_disponibles' no está disponible en los datos")

        # Tabla 13: Libros con más capítulos
        st.subheader("📚 Libros con más capítulos")
        libros_stats = unique_capitulos.groupby('titulo_libro').agg(
            Total_Capitulos=('titulo_libro', 'size'),
            Autores_Unicos=('autor_principal', 'nunique')
        ).reset_index()
        libros_stats = libros_stats.sort_values('Total_Capitulos', ascending=False)
        libros_stats.columns = ['Título del Libro', 'Capítulos únicos', 'Autores únicos']
        mostrar_tabla_uniforme(libros_stats, "")

        # Tabla Resumen Consolidada
        st.header("📋 Resumen Consolidado de Totales")
        resumen_df = generar_tabla_resumen(unique_capitulos, filtered_df)
        mostrar_tabla_uniforme(resumen_df, "")

        # Sección de descarga
        st.header("📥 Descargar Datos Completos")
        if Path("capitulos_total.csv").exists():
            with open("capitulos_total.csv", "rb") as file:
                btn = st.download_button(
                    label="Descargar archivo pro_capitulos_total.csv completo",
                    data=file,
                    file_name="pro_capitulos_total.csv",
                    mime="text/csv"
                )
            if btn:
                st.success("Descarga iniciada")
        else:
            st.warning("El archivo capitulos_total.csv no está disponible para descargar")

    except Exception as e:
        st.error(f"Error al procesar el archivo: {str(e)}")
        logging.error(f"Error en main: {str(e)}")

if __name__ == "__main__":
    main()
