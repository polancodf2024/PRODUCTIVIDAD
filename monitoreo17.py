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

# Configuración de logging mejorada
logging.basicConfig(
    filename='monitoreo.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

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
        self.CSV_PRODUCTOS_PREFIX = "productos_"  # Prefijo para archivos CSV locales
        self.REMOTE_PRODUCTOS_PREFIX = st.secrets["prefixes"]["productos"].rstrip('_')  # Elimina _ final si existe
        self.TIMEOUT_SECONDS = 30
        self.LOGO_PATH = "escudo_COLOR.jpg"

        self.REMOTE = {
            'HOST': st.secrets["sftp"]["host"],
            'USER': st.secrets["sftp"]["user"],
            'PASSWORD': st.secrets["sftp"]["password"],
            'PORT': st.secrets["sftp"]["port"],
            'DIR': st.secrets["sftp"]["dir"]
        }

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
        st.info(f"🔍 Buscando archivo remoto")
        
        for attempt in range(SSHManager.MAX_RETRIES):
            ssh = SSHManager.get_connection()
            if not ssh:
                return False
                
            try:
                with ssh.open_sftp() as sftp:
                    try:
                        file_info = sftp.stat(remote_path)
                        st.info(f"📄 Archivo encontrado.")
                    except FileNotFoundError:
                        st.error(f"❌ Archivo no encontrado")
                        # Crear archivo local con estructura correcta
                        columns = [
                            'economic_number', 'departamento', 'participation_key', 'investigator_name',
                            'corresponding_author', 'coauthors', 'article_title', 'year',
                            'pub_date', 'volume', 'number', 'pages', 'journal_full',
                            'journal_abbrev', 'doi', 'jcr_group', 'pmid', 'selected_keywords',
                            'estado'
                        ]
                        pd.DataFrame(columns=columns).to_csv(local_path, index=False)
                        logging.info(f"Archivo remoto no encontrado")
                        return True
                        
                    sftp.get(remote_path, local_path)
                    
                    if SSHManager.verify_file_integrity(local_path, remote_path, sftp):
                        logging.info(f"Archivo descargado correctamente")
                        st.success(f"✅ Archivo descargado correctamente.")
                        return True
                    else:
                        logging.warning(f"Error de integridad en descarga, reintentando... (intento {attempt + 1})")
                        st.warning(f"⚠️ Error de integridad en descarga, reintentando... (intento {attempt + 1})")
                        if attempt < SSHManager.MAX_RETRIES - 1:
                            time.sleep(SSHManager.RETRY_DELAY)
                        else:
                            raise Exception("Fallo en verificación de integridad después de múltiples intentos")
                            
            except Exception as e:
                logging.error(f"Error en descarga (intento {attempt + 1}): {str(e)}")
                st.error(f"❌ Error en descarga (intento {attempt + 1}): {str(e)}")
                if attempt == SSHManager.MAX_RETRIES - 1:
                    st.error(f"Error descargando archivo remoto después de {SSHManager.MAX_RETRIES} intentos: {str(e)}")
                    return False
                    
            finally:
                ssh.close()

# ====================
# FUNCIONES DE SINCRONIZACIÓN
# ====================
def sync_with_remote():
    """Sincroniza el archivo productos local con el remoto"""
    try:
        st.info("🔄 Sincronizando con el servidor remoto...")
        csv_filename = "productos.csv"
        remote_filename = f"{CONFIG.REMOTE_PRODUCTOS_PREFIX}.csv"
        remote_path = os.path.join(CONFIG.REMOTE['DIR'], remote_filename)
        
        st.info(f"📂 Ruta completa del archivo remoto encontrada")

        # Intenta descargar el archivo remoto
        download_success = SSHManager.download_remote_file(remote_path, csv_filename)

        if not download_success:
            # Si no existe el archivo remoto, crea uno local con estructura correcta
            columns = [
                'economic_number', 'departamento', 'participation_key', 'investigator_name',
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
                'economic_number', 'departamento', 'participation_key', 'investigator_name',
                'corresponding_author', 'coauthors', 'article_title', 'year',
                'pub_date', 'volume', 'number', 'pages', 'journal_full',
                'journal_abbrev', 'doi', 'jcr_group', 'pmid', 'selected_keywords',
                'estado'
            ]
            pd.DataFrame(columns=columns).to_csv(csv_filename, index=False)
            return False

        st.success("✅ Sincronización con servidor remoto completada")
        st.info(f"📊 Datos descargados: {len(df)} registros")
        return True

    except Exception as e:
        st.error(f"❌ Error en sincronización: {str(e)}")
        logging.error(f"Sync Error: {str(e)}")
        return False

def load_data():
    """Carga los datos desde el archivo local o remoto"""
    csv_filename = "productos.csv"
    
    # Primero intenta sincronizar con el remoto
    sync_with_remote()
    
    # Luego carga el archivo local
    if Path(csv_filename).exists():
        try:
            df = pd.read_csv(csv_filename, encoding='utf-8-sig', dtype={'economic_number': str})
            df['economic_number'] = df['economic_number'].astype(str).str.strip()
            
            # Asegurar que el campo 'estado' exista
            if 'estado' not in df.columns:
                df['estado'] = 'A'
            else:
                # Limpiar valores vacíos/nulos en el campo estado
                df['estado'] = df['estado'].fillna('A').str.strip().replace('', 'A')
                
            return df
        except Exception as e:
            st.error(f"Error al leer el archivo: {str(e)}")
            logging.error(f"Error loading data: {str(e)}")
            return pd.DataFrame()
    else:
        return pd.DataFrame()

# ====================
# FUNCIÓN PRINCIPAL
# ====================
def main():
    # Añadir logo en la parte superior
    if Path(CONFIG.LOGO_PATH).exists():
        st.image(CONFIG.LOGO_PATH, width=200)
    else:
        st.warning(f"Logo no encontrado")

    st.title("Análisis de Manuscritos")

    # Sincronizar y cargar datos
    df = load_data()

    # Verificar si el DataFrame está vacío
    if df.empty:
        st.warning("No hay datos disponibles para analizar")
        return

    # Convertir y validar fechas
    df['pub_date'] = pd.to_datetime(df['pub_date'], errors='coerce')
    df = df[(df['estado'] == 'A') & (df['pub_date'].notna())]

    if df.empty:
        st.warning("No hay publicaciones válidas para analizar")
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

    # Obtener artículos únicos para estadísticas precisas
    unique_articles = filtered_df.drop_duplicates(subset=['article_title'])

    st.markdown(f"**Periodo seleccionado:** {date_start.strftime('%d/%m/%Y')} - {date_end.strftime('%d/%m/%Y')}",
               help="Rango de fechas seleccionado para el análisis.")
    st.markdown(f"**Registros encontrados:** {len(filtered_df)}",
               help="Total de registros en el periodo, incluyendo posibles duplicados del mismo artículo.")
    st.markdown(f"**Artículos únicos:** {len(unique_articles)}",
               help="Cantidad de artículos científicos distintos, eliminando duplicados.")

    if len(filtered_df) != len(unique_articles):
        st.warning(f"⚠️ **Nota:** Se detectaron {len(filtered_df) - len(unique_articles)} manuscritos duplicados del mismo artículo. ")

    if filtered_df.empty:
        st.warning("No hay publicaciones en el periodo seleccionado")
        return

    # Análisis consolidado en tablas
    st.header("📊 Estadísticas Consolidadas",
            help="Métricas generales basadas en los filtros aplicados.")

    # Tabla 1: Productividad por investigador
    st.subheader("🔍 Productividad por Investigador",
               help="Muestra cuántos artículos únicos tiene cada investigador y su posición de autoría.")

    investigator_stats = filtered_df.groupby('investigator_name').agg(
        Articulos_Unicos=('article_title', lambda x: len(set(x))),
        Participaciones=('participation_key', lambda x: ', '.join(sorted(set(x))))
    ).reset_index()

    investigator_stats = investigator_stats.sort_values('Articulos_Unicos', ascending=False)
    investigator_stats.columns = ['Investigador', 'Artículos únicos', 'Posiciones de autoría']

    # Añadir fila de totales
    total_row = pd.DataFrame({
        'Investigador': ['TOTAL'],
        'Artículos únicos': [investigator_stats['Artículos únicos'].sum()],
        'Posiciones de autoría': ['']
    })
    investigator_stats = pd.concat([investigator_stats.head(10), total_row], ignore_index=True)

    # Mostrar tabla con enlaces clickeables
    for index, row in investigator_stats.iterrows():
        if row['Investigador'] != 'TOTAL':
            with st.expander(f"{row['Investigador']} - {row['Artículos únicos']} artículos - Posiciones: {row['Posiciones de autoría']}"):
                investigator_articles = filtered_df[filtered_df['investigator_name'] == row['Investigador']]
                unique_articles_investigator = investigator_articles.drop_duplicates(subset=['article_title'])

                st.write(f"Artículos de {row['Investigador']}:")
                st.dataframe(unique_articles_investigator[['article_title', 'journal_full', 'pub_date', 'jcr_group', 'participation_key']])

                csv = unique_articles_investigator.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Descargar producción científica en CSV",
                    data=csv,
                    file_name=f"produccion_{row['Investigador'].replace(' ', '_')}.csv",
                    mime='text/csv',
                    key=f"download_{index}"
                )

    # Tabla 2: Revistas más publicadas
    st.subheader("📚 Revistas más Utilizadas",
               help="Listado de revistas científicas ordenadas por cantidad de artículos publicados.")
    journal_stats = unique_articles.groupby('journal_full').agg(
        Total_Articulos=('journal_full', 'size'),
        Grupo_JCR=('jcr_group', lambda x: x.mode()[0] if not x.mode().empty else 'No disponible')
    ).reset_index()
    journal_stats = journal_stats.sort_values('Total_Articulos', ascending=False)
    journal_stats.columns = ['Revista', 'Artículos únicos', 'Grupo JCR más frecuente']

    # Añadir fila de totales
    total_row = pd.DataFrame({
        'Revista': ['TOTAL'],
        'Artículos únicos': [journal_stats['Artículos únicos'].sum()],
        'Grupo JCR más frecuente': ['']
    })
    journal_stats = pd.concat([journal_stats.head(10), total_row], ignore_index=True)
    st.dataframe(journal_stats, hide_index=True)

    # Tabla 3: Disciplinas más comunes
    st.subheader("🧪 Enfoques más Frecuentes",
               help="Palabras clave más utilizadas en los artículos.")
    try:
        all_keywords = []
        for keywords in unique_articles['selected_keywords']:
            cleaned = str(keywords).strip("[]'").replace("'", "").split(", ")
            all_keywords.extend([k.strip() for k in cleaned if k.strip()])

        keyword_stats = pd.Series(all_keywords).value_counts().reset_index()
        keyword_stats.columns = ['Disciplina', 'Frecuencia']

        # Añadir fila de totales
        total_row = pd.DataFrame({
            'Disciplina': ['TOTAL'],
            'Frecuencia': [keyword_stats['Frecuencia'].sum()]
        })
        keyword_stats = pd.concat([keyword_stats.head(10), total_row], ignore_index=True)
        st.dataframe(keyword_stats, hide_index=True)
    except:
        st.warning("No se pudieron procesar las disciplinas")

    # Tabla 4: Distribución por grupos JCR
    st.subheader("🏆 Distribución por Índice de Impacto",
               help="Clasificación de artículos según el factor de impacto de las revistas.")
    jcr_stats = unique_articles['jcr_group'].value_counts().reset_index()
    jcr_stats.columns = ['Grupo JCR', 'Artículos únicos']

    # Añadir fila de totales
    total_row = pd.DataFrame({
        'Grupo JCR': ['TOTAL'],
        'Artículos únicos': [jcr_stats['Artículos únicos'].sum()]
    })
    jcr_stats = pd.concat([jcr_stats, total_row], ignore_index=True)
    st.dataframe(jcr_stats, hide_index=True)

    # Tabla 5: Distribución temporal
    st.subheader("🕰️ Distribución Mensual",
                help="Evolución mensual de la producción científica.")
    time_stats = unique_articles['pub_date'].dt.strftime('%Y-%m').value_counts().sort_index().reset_index()
    time_stats.columns = ['Mes-Año', 'Artículos únicos']

    # Añadir fila de totales
    total_row = pd.DataFrame({
        'Mes-Año': ['TOTAL'],
        'Artículos únicos': [time_stats['Artículos únicos'].sum()]
    })
    time_stats = pd.concat([time_stats, total_row], ignore_index=True)
    st.dataframe(time_stats, hide_index=True)

if __name__ == "__main__":
    main()
