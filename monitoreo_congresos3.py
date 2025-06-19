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
    filename='monitoreo_congresos.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d'
)

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
    """Sincroniza el archivo congresos_total.csv desde el servidor remoto"""
    try:
        remote_path = os.path.join(CONFIG.REMOTE['DIR'], CONFIG.REMOTE_CONGRESOS_FILE)
        local_path = "congresos_total.csv"
        
        with st.spinner("🔄 Sincronizando archivo congresos_total.csv desde el servidor..."):
            if SSHManager.download_remote_file(remote_path, local_path):
                st.success("✅ Archivo congresos_total.csv sincronizado correctamente")
                return True
            else:
                st.error("❌ No se pudo descargar el archivo congresos_total.csv del servidor")
                return False
    except Exception as e:
        st.error(f"❌ Error en sincronización: {str(e)}")
        logging.error(f"Sync Error: {str(e)}")
        return False

def highlight_participant(name: str, investigator_name: str) -> str:
    """Resalta el nombre del investigador principal"""
    if investigator_name and investigator_name.lower() == name.lower():
        return f"<span style='background-color: {CONFIG.HIGHLIGHT_COLOR};'>{name}</span>"
    return name

def main():
    st.set_page_config(
        page_title="Análisis de Congresos",
        page_icon="🎤",
        layout="wide"
    )

    # Añadir logo en la parte superior
    if Path(CONFIG.LOGO_PATH).exists():
        st.image(CONFIG.LOGO_PATH, width=200)

    st.title("Análisis de Congresos")

    # Paso 1: Ejecutar generador remoto para actualizar datos
    if not ejecutar_generador_remoto():
        st.warning("⚠️ Continuando con datos existentes (pueden no estar actualizados)")

    # Paso 2: Sincronizar archivo congresos_total.csv
    if not sync_congresos_file():
        st.warning("⚠️ Trabajando con copia local de congresos_total.csv debido a problemas de conexión")

    # Verificar si el archivo local existe
    if not Path("congresos_total.csv").exists():
        st.error("No se encontró el archivo congresos_total.csv")
        return

    try:
        # Leer y procesar el archivo
        df = pd.read_csv("congresos_total.csv", header=0, encoding='utf-8')
        df.columns = df.columns.str.strip()  # Limpiar espacios en nombres de columnas

        # Verificación de columnas (para diagnóstico)
        logging.info(f"Columnas detectadas: {df.columns.tolist()}")

        # Verificar que los campos importantes existen
        required_columns = ['economic_number', 'titulo_congreso', 'fecha_exacta_congreso', 'estado']
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            st.warning(f"El archivo congresos_total.csv no contiene los campos requeridos: {', '.join(missing_columns)}")
            return

        # Convertir y validar fechas
        df['fecha_exacta_congreso'] = pd.to_datetime(df['fecha_exacta_congreso'], errors='coerce')
        df = df[(df['estado'] == 'A') & (df['fecha_exacta_congreso'].notna())]

        if df.empty:
            st.warning("No hay congresos válidos para analizar")
            return

        st.success(f"Datos cargados correctamente. Registros activos: {len(df)}")

        # Obtener rangos de fechas disponibles
        min_date = df['fecha_exacta_congreso'].min()
        max_date = df['fecha_exacta_congreso'].max()

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
        filtered_df = df[(df['fecha_exacta_congreso'] >= pd.to_datetime(date_start)) &
                       (df['fecha_exacta_congreso'] <= pd.to_datetime(date_end))]

        # Obtener congresos únicos para estadísticas precisas
        unique_congresos = filtered_df.drop_duplicates(subset=['titulo_congreso'])

        st.markdown(f"**Periodo seleccionado:** {date_start.strftime('%d/%m/%Y')} - {date_end.strftime('%d/%m/%Y')}",
                   help="Rango de fechas seleccionado para el análisis.")
        st.markdown(f"**Registros encontrados:** {len(filtered_df)}",
                   help="Total de registros en el periodo, incluyendo posibles duplicados del mismo congreso.")
        st.markdown(f"**Congresos únicos:** {len(unique_congresos)}",
                   help="Cantidad de congresos distintos, eliminando duplicados.")

        if len(filtered_df) != len(unique_congresos):
            st.warning(f"⚠️ **Nota:** Se detectaron {len(filtered_df) - len(unique_congresos)} registros duplicados del mismo congreso.")

        if filtered_df.empty:
            st.warning("No hay congresos en el periodo seleccionado")
            return

        # Análisis consolidado en tablas
        st.header("📊 Estadísticas Consolidadas",
                help="Métricas generales basadas en los filtros aplicados.")

        # Tabla 1: Participación por investigador
        st.subheader("🔍 Participación por investigador",
                   help="Muestra cuántos congresos únicos tiene cada investigador y su rol de participación.")

        # Crear dataframe con información de participación
        investigator_stats = filtered_df.groupby('economic_number').agg(
            Congresos_Unicos=('titulo_congreso', lambda x: len(set(x))),
            Rol_Participacion=('rol', lambda x: ', '.join(sorted(set(x)))),
            Nombre=('nombramiento', 'first')  # Asumimos que nombramiento es único por economic_number
        ).reset_index()

        investigator_stats = investigator_stats.sort_values('Congresos_Unicos', ascending=False)
        investigator_stats.columns = ['Número Económico', 'Congresos únicos', 'Rol de participación', 'Nombramiento']

        # Añadir fila de totales
        total_row = pd.DataFrame({
            'Número Económico': ['TOTAL'],
            'Congresos únicos': [investigator_stats['Congresos únicos'].sum()],
            'Rol de participación': [''],
            'Nombramiento': ['']
        })
        investigator_stats = pd.concat([investigator_stats.head(10), total_row], ignore_index=True)

        # Mostrar tabla con enlaces clickeables
        for index, row in investigator_stats.iterrows():
            if row['Número Económico'] != 'TOTAL':
                # Crear un expander para cada investigador
                with st.expander(f"{row['Número Económico']} - {row['Congresos únicos']} congresos"):
                    # Filtrar los congresos del investigador
                    investigator_congresos = filtered_df[filtered_df['economic_number'] == row['Número Económico']]
                    unique_congresos_investigator = investigator_congresos.drop_duplicates(subset=['titulo_congreso'])

                    # Mostrar los congresos
                    display_columns = ['titulo_congreso', 'institucion', 'tipo_congreso', 'pais',
                                     'año_congreso', 'fecha_exacta_congreso', 'rol', 'titulo_ponencia']

                    st.write(f"Congresos de {row['Número Económico']}:")
                    st.dataframe(unique_congresos_investigator[display_columns])

                    # Opción para descargar en CSV
                    csv = unique_congresos_investigator.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Descargar participación en congresos (CSV)",
                        data=csv,
                        file_name=f"congresos_{row['Número Económico']}.csv",
                        mime='text/csv',
                        key=f"download_{index}"
                    )

        # Tabla 2: Instituciones organizadoras más frecuentes
        st.subheader("🏛️ Instituciones organizadoras",
                   help="Listado de instituciones que organizaron congresos, ordenadas por frecuencia.")
        institucion_stats = unique_congresos.groupby('institucion').agg(
            Total_Congresos=('institucion', 'size')
        ).reset_index()
        institucion_stats = institucion_stats.sort_values('Total_Congresos', ascending=False)
        institucion_stats.columns = ['Institución', 'Congresos únicos']

        # Añadir fila de totales
        total_row = pd.DataFrame({
            'Institución': ['TOTAL'],
            'Congresos únicos': [institucion_stats['Congresos únicos'].sum()]
        })
        institucion_stats = pd.concat([institucion_stats.head(10), total_row], ignore_index=True)
        st.dataframe(institucion_stats, hide_index=True)

        # Tabla 3: Tipos de congreso más comunes
        st.subheader("🌍 Tipo de congreso",
                   help="Distribución de congresos por tipo (Nacional/Internacional).")
        tipo_stats = unique_congresos['tipo_congreso'].value_counts().reset_index()
        tipo_stats.columns = ['Tipo de Congreso', 'Congresos únicos']

        # Añadir fila de totales
        total_row = pd.DataFrame({
            'Tipo de Congreso': ['TOTAL'],
            'Congresos únicos': [tipo_stats['Congresos únicos'].sum()]
        })
        tipo_stats = pd.concat([tipo_stats, total_row], ignore_index=True)
        st.dataframe(tipo_stats, hide_index=True)

        # Tabla 4: Países más frecuentes
        st.subheader("📍 Países donde se realizaron",
                   help="Distribución de congresos por país de realización.")
        pais_stats = unique_congresos['pais'].value_counts().reset_index()
        pais_stats.columns = ['País', 'Congresos únicos']

        # Añadir fila de totales
        total_row = pd.DataFrame({
            'País': ['TOTAL'],
            'Congresos únicos': [pais_stats['Congresos únicos'].sum()]
        })
        pais_stats = pd.concat([pais_stats, total_row], ignore_index=True)
        st.dataframe(pais_stats, hide_index=True)

        # Tabla 5: Roles de participación
        st.subheader("🎭 Roles de participación",
                   help="Distribución de los roles de participación en los congresos.")
        rol_stats = filtered_df['rol'].value_counts().reset_index()
        rol_stats.columns = ['Rol', 'Participaciones']

        # Añadir fila de totales
        total_row = pd.DataFrame({
            'Rol': ['TOTAL'],
            'Participaciones': [rol_stats['Participaciones'].sum()]
        })
        rol_stats = pd.concat([rol_stats, total_row], ignore_index=True)
        st.dataframe(rol_stats, hide_index=True)

        # Tabla 6: Líneas de investigación más frecuentes
        st.subheader("🧪 Líneas de investigación",
                   help="Líneas de investigación más frecuentes en los congresos.")
        try:
            all_keywords = []
            for keywords in unique_congresos['linea_investigacion']:
                if pd.notna(keywords):
                    # Procesamiento de palabras clave
                    keywords_str = str(keywords).strip()
                    # Si la línea ya está entre comillas, tomarla como una sola
                    if (keywords_str.startswith('"') and keywords_str.endswith('"')) or \
                       (keywords_str.startswith("'") and keywords_str.endswith("'")):
                        all_keywords.append(keywords_str[1:-1])
                    else:
                        # Si no está entre comillas, agregar toda la cadena como una sola línea
                        all_keywords.append(keywords_str)

            keyword_stats = pd.Series(all_keywords).value_counts().reset_index()
            keyword_stats.columns = ['Línea de investigación', 'Frecuencia']

            # Añadir fila de totales
            total_row = pd.DataFrame({
                'Línea de investigación': ['TOTAL'],
                'Frecuencia': [keyword_stats['Frecuencia'].sum()]
            })
            keyword_stats = pd.concat([keyword_stats.head(10), total_row], ignore_index=True)
            st.dataframe(keyword_stats, hide_index=True)
        except Exception as e:
            st.warning(f"No se pudieron procesar las líneas de investigación: {str(e)}")
            logging.error(f"Error procesando líneas de investigación: {str(e)}")

        # Tabla 7: Distribución por departamentos
        if 'departamento' in unique_congresos.columns:
            st.subheader("🏢 Departamentos de adscripción",
                       help="Distribución de congresos por departamento de adscripción del participante.")
            depto_stats = unique_congresos['departamento'].value_counts().reset_index()
            depto_stats.columns = ['Departamento', 'Congresos únicos']

            # Añadir fila de totales
            total_row = pd.DataFrame({
                'Departamento': ['TOTAL'],
                'Congresos únicos': [depto_stats['Congresos únicos'].sum()]
            })
            depto_stats = pd.concat([depto_stats, total_row], ignore_index=True)
            st.dataframe(depto_stats, hide_index=True)
        else:
            st.warning("El campo 'departamento' no está disponible en los datos")

        # Tabla 8: Distribución temporal
        st.subheader("🕰️ Distribución mensual",
                    help="Evolución mensual de participación en congresos en el periodo seleccionado.")

        # Convertir a formato "YYYY-MM"
        time_stats = unique_congresos['fecha_exacta_congreso'].dt.to_period('M').astype(str).value_counts().sort_index().reset_index()
        time_stats.columns = ['Mes-Año', 'Congresos únicos']

        # Añadir fila de totales
        total_row = pd.DataFrame({
            'Mes-Año': ['TOTAL'],
            'Congresos únicos': [time_stats['Congresos únicos'].sum()]
        })
        time_stats = pd.concat([time_stats, total_row], ignore_index=True)
        st.dataframe(time_stats, hide_index=True)

        # Tabla 9: Distribución por nivel SNI
        if 'sni' in unique_congresos.columns:
            st.subheader("📊 Nivel SNI de participantes",
                        help="Distribución de participación en congresos por nivel SNI.")
            sni_stats = unique_congresos['sni'].value_counts().reset_index()
            sni_stats.columns = ['Nivel SNI', 'Congresos únicos']

            # Añadir fila de totales
            total_row = pd.DataFrame({
                'Nivel SNI': ['TOTAL'],
                'Congresos únicos': [sni_stats['Congresos únicos'].sum()]
            })
            sni_stats = pd.concat([sni_stats, total_row], ignore_index=True)
            st.dataframe(sni_stats, hide_index=True)
        else:
            st.warning("El campo 'sni' no está disponible en los datos")

        # Tabla 10: Distribución por nivel SII
        if 'sii' in unique_congresos.columns:
            st.subheader("📈 Nivel SII de participantes",
                        help="Distribución de participación en congresos por nivel SII.")
            sii_stats = unique_congresos['sii'].value_counts().reset_index()
            sii_stats.columns = ['Nivel SII', 'Congresos únicos']

            # Añadir fila de totales
            total_row = pd.DataFrame({
                'Nivel SII': ['TOTAL'],
                'Congresos únicos': [sii_stats['Congresos únicos'].sum()]
            })
            sii_stats = pd.concat([sii_stats, total_row], ignore_index=True)
            st.dataframe(sii_stats, hide_index=True)
        else:
            st.warning("El campo 'sii' no está disponible en los datos")

        # Tabla 11: Distribución por nombramiento
        if 'nombramiento' in unique_congresos.columns:
            st.subheader("👔 Tipo de nombramiento",
                        help="Distribución de participación en congresos por tipo de nombramiento.")
            nombramiento_stats = unique_congresos['nombramiento'].value_counts().reset_index()
            nombramiento_stats.columns = ['Tipo de Nombramiento', 'Congresos únicos']

            # Añadir fila de totales
            total_row = pd.DataFrame({
                'Tipo de Nombramiento': ['TOTAL'],
                'Congresos únicos': [nombramiento_stats['Congresos únicos'].sum()]
            })
            nombramiento_stats = pd.concat([nombramiento_stats, total_row], ignore_index=True)
            st.dataframe(nombramiento_stats, hide_index=True)
        else:
            st.warning("El campo 'nombramiento' no está disponible en los datos")

        # ==========================================
        # SECCIÓN: DESCARGAR ARCHIVO COMPLETO
        # ==========================================
        st.header("📥 Descargar Datos Completos")

        # Opción para descargar el archivo pro_congresos_total.csv
        if Path("congresos_total.csv").exists():
            with open("congresos_total.csv", "rb") as file:
                btn = st.download_button(
                    label="Descargar archivo pro_congresos_total.csv completo",
                    data=file,
                    file_name="pro_congresos_total.csv",
                    mime="text/csv",
                    help="Descarga el archivo CSV completo con todos los datos de congresos"
                )
            if btn:
                st.success("Descarga iniciada")
        else:
            st.warning("El archivo congresos_total.csv no está disponible para descargar")

    except Exception as e:
        st.error(f"Error al procesar el archivo: {str(e)}")
        logging.error(f"Error en main: {str(e)}")

if __name__ == "__main__":
    main()
