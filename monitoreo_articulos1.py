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

# Configuración de logging
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
        # Configuración SFTP
        self.REMOTE_PRODUCTOS_FILE = "pro_productos_total.csv"  # Nombre completo del archivo remoto
        self.TIMEOUT_SECONDS = 30
        
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

def sync_productos_file():
    """Sincroniza el archivo productos_total.csv desde el servidor remoto"""
    try:
        remote_path = os.path.join(CONFIG.REMOTE['DIR'], CONFIG.REMOTE_PRODUCTOS_FILE)
        local_path = "productos_total.csv"
        
        with st.spinner("🔄 Sincronizando archivo productos_total.csv desde el servidor..."):
            if SSHManager.download_remote_file(remote_path, local_path):
                st.success("✅ Archivo productos_total.csv sincronizado correctamente")
                return True
            else:
                st.error("❌ No se pudo descargar el archivo productos_total.csv del servidor")
                return False
    except Exception as e:
        st.error(f"❌ Error en sincronización: {str(e)}")
        logging.error(f"Sync Error: {str(e)}")
        return False

def main():
    # Añadir logo en la parte superior
    st.image("escudo_COLOR.jpg", width=200)

    st.title("Análisis de Artículos")

    # Sincronizar archivo productos_total.csv al inicio
    if not sync_productos_file():
        st.warning("⚠️ Trabajando con copia local de productos_total.csv debido a problemas de conexión")

    # Verificar si el archivo local existe
    if not Path("productos_total.csv").exists():
        st.error("No se encontró el archivo productos_total.csv")
        return

    try:
        # Leer y procesar el archivo con los nuevos campos sni y sii
        df = pd.read_csv("productos_total.csv", header=0, encoding='utf-8')
        df.columns = df.columns.str.strip()  # Limpiar espacios en nombres de columnas

        # Verificación de columnas (para diagnóstico)
        logging.info(f"Columnas detectadas: {df.columns.tolist()}")

        # Verificar que los campos clave existen
        if 'nombramiento' not in df.columns:
            st.warning("El archivo no contiene el campo 'nombramiento'")
        if 'sni' not in df.columns or 'sii' not in df.columns:
            st.warning("El archivo productos_total.csv no contiene los campos 'sni' y/o 'sii'")

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
        st.markdown(f"**Artículos encontrados:** {len(filtered_df)}",
                   help="Total de registros en el periodo, incluyendo posibles duplicados del mismo artículo.")
        st.markdown(f"**Artículos únicos:** {len(unique_articles)}",
                   help="Cantidad de artículos científicos distintos, eliminando duplicados.")

        duplicates_count = len(filtered_df) - len(unique_articles)

        if duplicates_count > 0:
            if len(unique_articles) == 1:
                st.warning(f"⚠️ **Nota:** Se detectó {duplicates_count} artículo duplicado.")
            else:
                st.warning(f"⚠️ **Nota:** Se detectaron {duplicates_count} artículos duplicados.")

        if filtered_df.empty:
            st.warning("No hay publicaciones en el periodo seleccionado")
            return

        # Análisis consolidado en tablas
        st.header("📊 Estadísticas Consolidadas",
                help="Métricas generales basadas en los filtros aplicados.")

        # Tabla 1: Productividad por investigador (ARTÍCULOS ÚNICOS) con participación
        st.subheader("🔍 Productividad por investigador",
                   help="Muestra cuántos artículos únicos tiene cada investigador y su posición de autoría.")

        investigator_stats = filtered_df.groupby('investigator_name').agg(
            Articulos_Unicos=('article_title', lambda x: len(set(x))),
            Participaciones=('participation_key', lambda x: ', '.join(sorted(set(x))))
        ).reset_index()

        investigator_stats = investigator_stats.sort_values('Articulos_Unicos', ascending=False)
        investigator_stats.columns = ['Investigador', 'Artículos únicos', 'Posición de autoría']

        # Añadir fila de totales
        total_row = pd.DataFrame({
            'Investigador': ['TOTAL'],
            'Artículos únicos': [investigator_stats['Artículos únicos'].sum()],
            'Posición de autoría': ['']
        })
        investigator_stats = pd.concat([investigator_stats.head(10), total_row], ignore_index=True)

        # Mostrar tabla con enlaces clickeables - VERSIÓN MODIFICADA
        for index, row in investigator_stats.iterrows():
            if row['Investigador'] != 'TOTAL':
                with st.expander(f"{row['Investigador']} - {row['Artículos únicos']} artículos"):
                    investigator_articles = filtered_df[filtered_df['investigator_name'] == row['Investigador']]
                    unique_articles_investigator = investigator_articles.drop_duplicates(subset=['article_title'])

                    # Mostrar todos los campos disponibles excepto columnas internas
                    excluded_columns = ['estado']  # Solo excluir columna estado
                    display_columns = [col for col in unique_articles_investigator.columns
                                     if col not in excluded_columns]

                    # Configuración especial para columnas
                    column_config = {
                        "pub_date": st.column_config.DateColumn("Fecha publicación", format="DD/MM/YYYY"),
                        "doi": st.column_config.LinkColumn("DOI"),
                        "pmid": st.column_config.NumberColumn("PMID"),
                        "participation_key": st.column_config.TextColumn("Participación"),
                        "economic_number": st.column_config.TextColumn("Número económico"),
                        "nombramiento": st.column_config.TextColumn("Nombramiento"),
                        "sni": st.column_config.TextColumn("SNI"),
                        "sii": st.column_config.TextColumn("SII")
                    }

                    st.dataframe(
                        unique_articles_investigator[display_columns],
                        column_config=column_config,
                        use_container_width=True,
                        hide_index=True
                    )

                    # Botón de descarga
                    csv = unique_articles_investigator.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Descargar producción científica completa",
                        data=csv,
                        file_name=f"produccion_{row['Investigador'].replace(' ', '_')}.csv",
                        mime='text/csv',
                        key=f"download_{index}"
                    )

        # Tabla 2: Revistas más publicadas
        st.subheader("📚 Revistas más utilizadas",
                   help="Listado de revistas científicas ordenadas por cantidad de artículos publicados.")
        journal_stats = unique_articles.groupby('journal_full').agg(
            Total_Articulos=('journal_full', 'size'),
            Grupo_JCR=('jcr_group', lambda x: x.mode()[0] if not x.mode().empty else 'No disponible')
        ).reset_index()
        journal_stats = journal_stats.sort_values('Total_Articulos', ascending=False)
        journal_stats.columns = ['Revista', 'Artículos únicos', 'Grupo JCR más frecuente']

        total_row = pd.DataFrame({
            'Revista': ['TOTAL'],
            'Artículos únicos': [journal_stats['Artículos únicos'].sum()],
            'Grupo JCR más frecuente': ['']
        })
        journal_stats = pd.concat([journal_stats.head(10), total_row], ignore_index=True)
        st.dataframe(journal_stats, hide_index=True)

        # Tabla 3: Disciplinas más comunes
        st.subheader("🧪 Líneas de investigación más frecuentes",
                   help="Líneas de investigación más utilizadas en los artículos.")
        try:
            all_keywords = []
            for keywords in unique_articles['selected_keywords']:
                cleaned = str(keywords).strip("[]'").replace("'", "").split(", ")
                all_keywords.extend([k.strip() for k in cleaned if k.strip()])

            keyword_stats = pd.Series(all_keywords).value_counts().reset_index()
            keyword_stats.columns = ['Disciplina', 'Frecuencia']

            total_row = pd.DataFrame({
                'Disciplina': ['TOTAL'],
                'Frecuencia': [keyword_stats['Frecuencia'].sum()]
            })
            keyword_stats = pd.concat([keyword_stats.head(10), total_row], ignore_index=True)
            st.dataframe(keyword_stats, hide_index=True)
        except:
            st.warning("No se pudieron procesar las disciplinas")

        # Tabla 4: Distribución por grupos JCR
        st.subheader("🏆 Distribución por índice de impacto",
                   help="Clasificación de artículos según el factor de impacto de las revistas.")
        jcr_stats = unique_articles['jcr_group'].value_counts().reset_index()
        jcr_stats.columns = ['Grupo JCR', 'Artículos únicos']

        total_row = pd.DataFrame({
            'Grupo JCR': ['TOTAL'],
            'Artículos únicos': [jcr_stats['Artículos únicos'].sum()]
        })
        jcr_stats = pd.concat([jcr_stats, total_row], ignore_index=True)
        st.dataframe(jcr_stats, hide_index=True)

        # Tabla 5: Distribución temporal
        st.subheader("🕰️ Distribución mensual de artículos",
                    help="Evolución mensual de la producción científica.")
        time_stats = unique_articles['pub_date'].dt.strftime('%Y-%m').value_counts().sort_index().reset_index()
        time_stats.columns = ['Mes-Año', 'Artículos únicos']

        total_row = pd.DataFrame({
            'Mes-Año': ['TOTAL'],
            'Artículos únicos': [time_stats['Artículos únicos'].sum()]
        })
        time_stats = pd.concat([time_stats, total_row], ignore_index=True)
        st.dataframe(time_stats, hide_index=True)

        # Tabla 6: Distribución por nivel SNI
        if 'sni' in unique_articles.columns:
            st.subheader("📊 Distribución por nivel SNI",
                        help="Clasificación según el nivel del Sistema Nacional de Investigadores.")
            sni_stats = unique_articles['sni'].value_counts().reset_index()
            sni_stats.columns = ['Nivel SNI', 'Artículos únicos']

            total_row = pd.DataFrame({
                'Nivel SNI': ['TOTAL'],
                'Artículos únicos': [sni_stats['Artículos únicos'].sum()]
            })
            sni_stats = pd.concat([sni_stats, total_row], ignore_index=True)
            st.dataframe(sni_stats, hide_index=True)
        else:
            st.warning("El campo 'sni' no está disponible en los datos")

        # Tabla 7: Distribución por nivel SII
        if 'sii' in unique_articles.columns:
            st.subheader("📈 Distribución por nivel SII",
                        help="Clasificación según el nivel del Sistema Institucional de Investigación.")
            sii_stats = unique_articles['sii'].value_counts().reset_index()
            sii_stats.columns = ['Nivel SII', 'Artículos únicos']

            total_row = pd.DataFrame({
                'Nivel SII': ['TOTAL'],
                'Artículos únicos': [sii_stats['Artículos únicos'].sum()]
            })
            sii_stats = pd.concat([sii_stats, total_row], ignore_index=True)
            st.dataframe(sii_stats, hide_index=True)
        else:
            st.warning("El campo 'sii' no está disponible en los datos")

        # Tabla 8: Distribución por tipo de nombramiento
        if 'nombramiento' in unique_articles.columns:
            st.subheader("👔 Distribución por nombramientos",
                        help="Clasificación según el nombramiento de los autores.")
            nombramiento_stats = unique_articles['nombramiento'].value_counts().reset_index()
            nombramiento_stats.columns = ['Tipo de Nombramiento', 'Artículos únicos']

            total_row = pd.DataFrame({
                'Tipo de Nombramiento': ['TOTAL'],
                'Artículos únicos': [nombramiento_stats['Artículos únicos'].sum()]
            })
            nombramiento_stats = pd.concat([nombramiento_stats, total_row], ignore_index=True)
            st.dataframe(nombramiento_stats, hide_index=True)
        else:
            st.warning("El campo 'nombramiento' no está disponible en los datos")

        # Descargar archivo completo
        st.header("📥 Descargar Datos Completos")
        if Path("productos_total.csv").exists():
            with open("productos_total.csv", "rb") as file:
                btn = st.download_button(
                    label="Descargar archivo pro_productos_total.csv completo",
                    data=file,
                    file_name="pro_productos_total.csv",
                    mime="text/csv",
                    help="Descarga el archivo CSV completo con todos los datos"
                )
            if btn:
                st.success("Descarga iniciada")
        else:
            st.warning("El archivo productos_total.csv no está disponible para descargar")

    except Exception as e:
        st.error(f"Error al procesar el archivo: {str(e)}")
        logging.error(f"Error en main: {str(e)}")

if __name__ == "__main__":
    main()

