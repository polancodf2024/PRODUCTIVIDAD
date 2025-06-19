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
    filename='capitulos.log',
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
        self.REMOTE_CAPITULOS_FILE = "pro_capitulos_total.csv"  # Nombre completo del archivo remoto
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

def parse_custom_date(date_str):
    """Función para parsear fechas en formato 'YYYY_MM-DD' o 'YYYY-MM-DD'"""
    if pd.isna(date_str):
        return pd.NaT
    
    try:
        # Primero intentamos con el formato que parece estar en tus datos (2025_06-02)
        if '_' in date_str:
            year_part, month_day_part = date_str.split('_')
            month, day = month_day_part.split('-')
            return datetime(int(year_part), int(month), int(day))
        else:
            # Si no tiene '_', probamos con formato estándar
            return pd.to_datetime(date_str)
    except:
        return pd.NaT

def main():
    st.set_page_config(
        page_title="Análisis de Capítulos",
        page_icon="📚",
        layout="wide"
    )

    # Añadir logo
    if Path(CONFIG.LOGO_PATH).exists():
        st.image(CONFIG.LOGO_PATH, width=200)

    st.title("Análisis de Capítulos de Libros")

    # Sincronizar archivo
    if not sync_capitulos_file():
        st.warning("⚠️ Trabajando con copia local debido a problemas de conexión")

    if not Path("capitulos_total.csv").exists():
        st.error("Archivo no encontrado")
        return

    try:
        # Leer archivo con manejo robusto de valores nulos
        df = pd.read_csv(
            "capitulos_total.csv",
            keep_default_na=True,
            na_values=['None', 'none', 'NONE', '', 'NA', 'na', 'Na', 'n/a', 'N/A'],
            dtype={'year': 'object'}
        )

#        # Mostrar datos crudos para depuración
#        st.subheader("📝 Datos completos (sin filtros)")
#        st.dataframe(df)

        # Verificar campos obligatorios
        required_columns = [
            'autor_principal', 'titulo_libro', 'titulo_capitulo',
            'estado', 'tipo_participacion'
        ]
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            st.error(f"Faltan columnas requeridas: {', '.join(missing_columns)}")
            return

        # Convertir fechas manteniendo registros sin fecha
        df['pub_date'] = df['pub_date'].apply(
            lambda x: parse_custom_date(x) if pd.notna(x) else pd.NaT
        )

        # Filtrar solo registros activos
        df = df[df['estado'] == 'A'].copy()

        st.success(f"✅ Datos cargados: {len(df)} registros activos")
#        st.write(f"📌 Tipos de participación encontrados: {df['tipo_participacion'].unique()}")

        # Configurar rangos de fecha
        valid_dates = df[df['pub_date'].notna()]
        min_date = valid_dates['pub_date'].min() if not valid_dates.empty else datetime.now()
        max_date = valid_dates['pub_date'].max() if not valid_dates.empty else datetime.now()

        # Selector de periodo
        st.header("📅 Selección de Periodo")
        col1, col2 = st.columns(2)

        with col1:
            start_year = st.selectbox(
                "Año inicio",
                range(min_date.year, max_date.year+1),
                index=0
            )
            start_month = st.selectbox(
                "Mes inicio",
                range(1, 13),
                index=min_date.month-1,
                format_func=lambda x: datetime(1900, x, 1).strftime('%B')
            )

        with col2:
            end_year = st.selectbox(
                "Año término",
                range(min_date.year, max_date.year+1),
                index=len(range(min_date.year, max_date.year+1))-1
            )
            end_month = st.selectbox(
                "Mes término",
                range(1, 13),
                index=max_date.month-1,
                format_func=lambda x: datetime(1900, x, 1).strftime('%B')
            )

        # Calcular periodo seleccionado
        date_start = datetime(start_year, start_month, 1)
        date_end = datetime(
            end_year,
            end_month,
            calendar.monthrange(end_year, end_month)[1]
        )

        # Filtrar incluyendo registros sin fecha
        filtered_df = df[
            (df['pub_date'].isna()) |
            (
                (df['pub_date'] >= pd.to_datetime(date_start)) &
                (df['pub_date'] <= pd.to_datetime(date_end))
            )
        ].copy()

        st.markdown(f"**Periodo seleccionado:** {date_start.strftime('%d/%m/%Y')} - {date_end.strftime('%d/%m/%Y')}")
        st.markdown(f"**Registros encontrados:** {len(filtered_df)}")

        # Obtener capítulos únicos
        unique_capitulos = filtered_df.drop_duplicates(subset=['titulo_capitulo'])
        st.markdown(f"**Capítulos únicos:** {len(unique_capitulos)}")

        if filtered_df.empty:
            st.warning("No hay capítulos en el periodo seleccionado")
            return

        # Análisis consolidado
        st.header("📊 Estadísticas Consolidadas")

        # 1. Productividad por investigador (versión corregida)
        st.subheader("🔍 Productividad por investigador")

        # Calcular estadísticas
        investigator_stats = filtered_df.groupby('autor_principal').agg(
            Capítulos_Unicos=('titulo_capitulo', lambda x: len(set(x))),
            Participaciones=('tipo_participacion', lambda x: ', '.join(sorted(set(x)))),
            Primer_Capítulo=('pub_date', 'min'),
            Último_Capítulo=('pub_date', 'max')
        ).reset_index()

        investigator_stats = investigator_stats.sort_values('Capítulos_Unicos', ascending=False)
        investigator_stats.columns = [
            'Investigador', 'Capítulos únicos', 'Tipos de participación',
            'Primer capítulo', 'Último capítulo'
        ]

        # Preparar fila de totales con tipos de datos consistentes
        total_row = pd.DataFrame({
            'Investigador': ['TOTAL'],
            'Capítulos únicos': [investigator_stats['Capítulos únicos'].sum()],
            'Tipos de participación': [''],
            'Primer capítulo': [pd.NaT],  # Usar NaT en lugar de None
            'Último capítulo': [pd.NaT]
        })

        # Asegurar tipos de datos consistentes
        investigator_stats['Primer capítulo'] = pd.to_datetime(investigator_stats['Primer capítulo'])
        investigator_stats['Último capítulo'] = pd.to_datetime(investigator_stats['Último capítulo'])

        # Concatenar con tipos consistentes
        investigator_stats = pd.concat([
            investigator_stats,
            total_row
        ], ignore_index=True)

        # Formatear fechas para visualización
        investigator_stats['Primer capítulo'] = investigator_stats['Primer capítulo'].apply(
            lambda x: x.strftime('%d/%m/%Y') if pd.notna(x) else 'N/A'
        )
        investigator_stats['Último capítulo'] = investigator_stats['Último capítulo'].apply(
            lambda x: x.strftime('%d/%m/%Y') if pd.notna(x) else 'N/A'
        )

        st.dataframe(investigator_stats, hide_index=True)

        # Detalle por investigador
        for _, row in investigator_stats.iterrows():
            if row['Investigador'] != 'TOTAL':
                with st.expander(f"{row['Investigador']} - {row['Capítulos únicos']} capítulos"):
                    investigator_data = filtered_df[
                        filtered_df['autor_principal'] == row['Investigador']
                    ].drop_duplicates(subset=['titulo_capitulo'])

                    display_cols = [
                        'titulo_libro', 'titulo_capitulo', 'editorial',
                        'pub_date', 'isbn_issn', 'tipo_participacion'
                    ]

                    # Añadir campos opcionales si existen
                    optional_cols = ['sni', 'sii', 'nombramiento', 'departamento']
                    for col in optional_cols:
                        if col in investigator_data.columns:
                            display_cols.append(col)

                    # Formatear fecha para visualización
                    temp_df = investigator_data[display_cols].copy()
                    if 'pub_date' in temp_df.columns:
                        temp_df['pub_date'] = temp_df['pub_date'].apply(
                            lambda x: x.strftime('%d/%m/%Y') if pd.notna(x) else 'N/A'
                        )

                    st.dataframe(temp_df)

                    # Botón de descarga
                    csv = temp_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label=f"Descargar {row['Investigador']}",
                        data=csv,
                        file_name=f"capitulos_{row['Investigador'].replace(' ', '_')}.csv",
                        mime='text/csv'
                    )

        # 2. Estadísticas por editorial
        st.subheader("🏢 Editoriales")
        editorial_stats = unique_capitulos['editorial'].value_counts().reset_index()
        editorial_stats.columns = ['Editorial', 'Capítulos']
        st.dataframe(editorial_stats, hide_index=True)

        # 3. Distribución por tipo de participación
        st.subheader("🎭 Tipos de participación")
        participacion_stats = unique_capitulos['tipo_participacion'].value_counts().reset_index()
        participacion_stats.columns = ['Tipo', 'Capítulos']
        st.dataframe(participacion_stats, hide_index=True)

        # 4. Palabras clave
        if 'selected_keywords' in unique_capitulos.columns:
            st.subheader("🧪 Palabras clave")
            try:
                keywords = unique_capitulos['selected_keywords'].dropna().apply(
                    lambda x: [k.strip("[]'\" ") for k in str(x).split(',')]
                ).explode()

                keyword_stats = keywords.value_counts().reset_index()
                keyword_stats.columns = ['Palabra clave', 'Frecuencia']
                st.dataframe(keyword_stats, hide_index=True)
            except Exception as e:
                st.warning(f"No se pudieron procesar palabras clave: {str(e)}")

        # 5. Distribución temporal
        st.subheader("🕰️ Distribución por mes")
        if not unique_capitulos['pub_date'].isna().all():
            time_stats = unique_capitulos[unique_capitulos['pub_date'].notna()].copy()
            time_stats['Mes-Año'] = time_stats['pub_date'].dt.to_period('M').astype(str)
            time_stats = time_stats['Mes-Año'].value_counts().sort_index().reset_index()
            time_stats.columns = ['Mes-Año', 'Capítulos']
            st.dataframe(time_stats, hide_index=True)
        else:
            st.warning("No hay fechas válidas para mostrar distribución temporal")

        # Descarga completa
        st.header("📥 Descargar datos completos")
        if st.button("Exportar todos los datos a CSV"):
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Descargar CSV completo",
                data=csv,
                file_name="capitulos_completos.csv",
                mime='text/csv'
            )

    except Exception as e:
        st.error(f"Error crítico: {str(e)}")
        logging.exception("Error en main:")

if __name__ == "__main__":
    main()
