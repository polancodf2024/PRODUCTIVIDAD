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
    filename='monitoreo_tesis.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ====================
# CONFIGURACIÓN INICIAL
# ====================
class Config:
    def __init__(self):
        # Configuración SFTP usando secrets.toml
        self.REMOTE_TESIS_FILE = f"{st.secrets['prefixes']['tesis']}total.csv"
        self.REMOTE_GENERADOR_PATH = f"{st.secrets['sftp']['dir']}/{st.secrets['prefixes']['generadortesis']}"
        self.REMOTE_DIR = st.secrets["sftp"]["dir"]
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
            comando = f"cd {CONFIG.REMOTE_DIR} && bash {CONFIG.REMOTE_GENERADOR_PATH}"
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
            output_path = os.path.join(CONFIG.REMOTE_DIR, CONFIG.REMOTE_TESIS_FILE)
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
        remote_path = os.path.join(CONFIG.REMOTE_DIR, CONFIG.REMOTE_TESIS_FILE)
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

def main():
    # Añadir logo en la parte superior
    st.image("escudo_COLOR.jpg", width=200)

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
        # Leer y procesar el archivo con los nuevos campos sni y sii
        df = pd.read_csv("tesis_total.csv", header=0, encoding='utf-8')
        df.columns = df.columns.str.strip()  # Limpiar espacios en nombres de columnas

        # Verificación de columnas (para diagnóstico)
        logging.info(f"Columnas detectadas: {df.columns.tolist()}")

        # Verificar que los campos clave existen
        if 'nombramiento' not in df.columns:
            st.warning("El archivo no contiene el campo 'nombramiento'")
        if 'sni' not in df.columns or 'sii' not in df.columns:
            st.warning("El archivo tesis_total.csv no contiene los campos 'sni' y/o 'sii'")

        # Convertir y validar fechas
        df['pub_date'] = pd.to_datetime(df['pub_date'], errors='coerce')
        df = df[(df['estado'] == 'A') & (df['pub_date'].notna())]

        if df.empty:
            st.warning("No hay tesis válidas para analizar")
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

        # Obtener tesis únicas para estadísticas precisas
        unique_tesis = filtered_df.drop_duplicates(subset=['tesis_title'])

        st.markdown(f"**Periodo seleccionado:** {date_start.strftime('%d/%m/%Y')} - {date_end.strftime('%d/%m/%Y')}",
                   help="Rango de fechas seleccionado para el análisis.")
        st.markdown(f"**Tesis encontradas:** {len(filtered_df)}",
                   help="Total de registros en el periodo, incluyendo posibles duplicados de la misma tesis.")
        st.markdown(f"**Tesis únicas:** {len(unique_tesis)}",
                   help="Cantidad de tesis distintas, eliminando duplicados.")

        duplicates_count = len(filtered_df) - len(unique_tesis)

        if duplicates_count > 0:
            if duplicates_count == 1:
                st.warning(f"⚠️ **Nota:** Se detectó {duplicates_count} tesis duplicada.")
            else:
                st.warning(f"⚠️ **Nota:** Se detectaron {duplicates_count} tesis duplicadas.")

        if filtered_df.empty:
            st.warning("No hay tesis en el periodo seleccionado")
            return

        # Análisis consolidado en tablas
        st.header("📊 Estadísticas Consolidadas",
                help="Métricas generales basadas en los filtros aplicados.")

        # Tabla 1: Productividad por investigador (TESIS ÚNICAS) con participación
        st.subheader("🔍 Productividad por investigador",
                   help="Muestra cuántas tesis únicas tiene cada investigador y su posición de autoría.")

        investigator_stats = filtered_df.groupby('investigator_name').agg(
            Tesis_Unicas=('tesis_title', lambda x: len(set(x))),
            Participaciones=('participation_key', lambda x: ', '.join(sorted(set(x))))
        ).reset_index()

        investigator_stats = investigator_stats.sort_values('Tesis_Unicas', ascending=False)
        investigator_stats.columns = ['Investigador', 'Tesis únicas', 'Posición de autoría']

        # Añadir fila de totales
        total_row = pd.DataFrame({
            'Investigador': ['TOTAL'],
            'Tesis únicas': [investigator_stats['Tesis únicas'].sum()],
            'Posición de autoría': ['']
        })
        investigator_stats = pd.concat([investigator_stats.head(10), total_row], ignore_index=True)

        # Mostrar tabla con enlaces clickeables - VERSIÓN MODIFICADA
        for index, row in investigator_stats.iterrows():
            if row['Investigador'] != 'TOTAL':
                with st.expander(f"{row['Investigador']} - {row['Tesis únicas']} tesis"):
                    investigator_tesis = filtered_df[filtered_df['investigator_name'] == row['Investigador']]
                    unique_tesis_investigator = investigator_tesis.drop_duplicates(subset=['tesis_title'])

                    # Mostrar todos los campos disponibles excepto columnas internas
                    excluded_columns = ['estado']  # Solo excluir columna estado
                    display_columns = [col for col in unique_tesis_investigator.columns
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
                        unique_tesis_investigator[display_columns],
                        column_config=column_config,
                        use_container_width=True,
                        hide_index=True
                    )

                    # Botón de descarga
                    csv = unique_tesis_investigator.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Descargar producción de tesis completa",
                        data=csv,
                        file_name=f"tesis_{row['Investigador'].replace(' ', '_')}.csv",
                        mime='text/csv',
                        key=f"download_{index}"
                    )

        # Tabla 2: Programas más frecuentes
        st.subheader("🎓 Programas más frecuentes",
                   help="Listado de programas académicos ordenados por cantidad de tesis.")
        program_stats = unique_tesis.groupby('academic_program').agg(
            Total_Tesis=('academic_program', 'size'),
            Nivel_Academico=('academic_level', lambda x: x.mode()[0] if not x.mode().empty else 'No disponible')
        ).reset_index()
        program_stats = program_stats.sort_values('Total_Tesis', ascending=False)
        program_stats.columns = ['Programa Académico', 'Tesis únicas', 'Nivel Académico más frecuente']

        total_row = pd.DataFrame({
            'Programa Académico': ['TOTAL'],
            'Tesis únicas': [program_stats['Tesis únicas'].sum()],
            'Nivel Académico más frecuente': ['']
        })
        program_stats = pd.concat([program_stats.head(10), total_row], ignore_index=True)
        st.dataframe(program_stats, hide_index=True)

        # Tabla 3: Directores más activos
        st.subheader("👨‍🏫 Directores más activos",
                   help="Listado de directores de tesis ordenados por cantidad de tesis dirigidas.")
        director_stats = unique_tesis['director_name'].value_counts().reset_index()
        director_stats.columns = ['Director', 'Tesis únicas']

        total_row = pd.DataFrame({
            'Director': ['TOTAL'],
            'Tesis únicas': [director_stats['Tesis únicas'].sum()]
        })
        director_stats = pd.concat([director_stats.head(10), total_row], ignore_index=True)
        st.dataframe(director_stats, hide_index=True)

        # Tabla 4: Distribución por niveles académicos
        st.subheader("📚 Distribución por niveles académicos",
                   help="Clasificación de tesis según el nivel académico.")
        level_stats = unique_tesis['academic_level'].value_counts().reset_index()
        level_stats.columns = ['Nivel Académico', 'Tesis únicas']

        total_row = pd.DataFrame({
            'Nivel Académico': ['TOTAL'],
            'Tesis únicas': [level_stats['Tesis únicas'].sum()]
        })
        level_stats = pd.concat([level_stats, total_row], ignore_index=True)
        st.dataframe(level_stats, hide_index=True)

        # Tabla 5: Distribución temporal
        st.subheader("🕰️ Distribución mensual de tesis",
                    help="Evolución mensual de la producción de tesis.")
        time_stats = unique_tesis['pub_date'].dt.strftime('%Y-%m').value_counts().sort_index().reset_index()
        time_stats.columns = ['Mes-Año', 'Tesis únicas']

        total_row = pd.DataFrame({
            'Mes-Año': ['TOTAL'],
            'Tesis únicas': [time_stats['Tesis únicas'].sum()]
        })
        time_stats = pd.concat([time_stats, total_row], ignore_index=True)
        st.dataframe(time_stats, hide_index=True)

        # Tabla 6: Distribución por nivel SNI
        if 'sni' in unique_tesis.columns:
            st.subheader("📊 Distribución por nivel SNI",
                        help="Clasificación según el nivel del Sistema Nacional de Investigadores.")
            sni_stats = unique_tesis['sni'].value_counts().reset_index()
            sni_stats.columns = ['Nivel SNI', 'Tesis únicas']

            total_row = pd.DataFrame({
                'Nivel SNI': ['TOTAL'],
                'Tesis únicas': [sni_stats['Tesis únicas'].sum()]
            })
            sni_stats = pd.concat([sni_stats, total_row], ignore_index=True)
            st.dataframe(sni_stats, hide_index=True)
        else:
            st.warning("El campo 'sni' no está disponible en los datos")

        # Tabla 7: Distribución por nivel SII
        if 'sii' in unique_tesis.columns:
            st.subheader("📈 Distribución por nivel SII",
                        help="Clasificación según el nivel del Sistema Institucional de Investigación.")
            sii_stats = unique_tesis['sii'].value_counts().reset_index()
            sii_stats.columns = ['Nivel SII', 'Tesis únicas']

            total_row = pd.DataFrame({
                'Nivel SII': ['TOTAL'],
                'Tesis únicas': [sii_stats['Tesis únicas'].sum()]
            })
            sii_stats = pd.concat([sii_stats, total_row], ignore_index=True)
            st.dataframe(sii_stats, hide_index=True)
        else:
            st.warning("El campo 'sii' no está disponible en los datos")

        # Tabla 8: Distribución por tipo de nombramiento
        if 'nombramiento' in unique_tesis.columns:
            st.subheader("👔 Distribución por nombramientos",
                        help="Clasificación según el nombramiento de los autores.")
            nombramiento_stats = unique_tesis['nombramiento'].value_counts().reset_index()
            nombramiento_stats.columns = ['Tipo de Nombramiento', 'Tesis únicas']

            total_row = pd.DataFrame({
                'Tipo de Nombramiento': ['TOTAL'],
                'Tesis únicas': [nombramiento_stats['Tesis únicas'].sum()]
            })
            nombramiento_stats = pd.concat([nombramiento_stats, total_row], ignore_index=True)
            st.dataframe(nombramiento_stats, hide_index=True)
        else:
            st.warning("El campo 'nombramiento' no está disponible en los datos")

        # Descargar archivo completo
        st.header("📥 Descargar Datos Completos")
        if Path("tesis_total.csv").exists():
            with open("tesis_total.csv", "rb") as file:
                btn = st.download_button(
                    label="Descargar archivo pro_tesis_total.csv completo",
                    data=file,
                    file_name="pro_tesis_total.csv",
                    mime="text/csv",
                    help="Descarga el archivo CSV completo con todos los datos"
                )
            if btn:
                st.success("Descarga iniciada")
        else:
            st.warning("El archivo tesis_total.csv no está disponible para descargar")

    except Exception as e:
        st.error(f"Error al procesar el archivo: {str(e)}")
        logging.error(f"Error en main: {str(e)}")

if __name__ == "__main__":
    main()
