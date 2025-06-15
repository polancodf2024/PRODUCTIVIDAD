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
    filename='tesis.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ====================
# CATEGORÍAS DE KEYWORDS PARA TESIS
# ====================
KEYWORD_CATEGORIES = {
    "Enfermedad coronaria": [],
    "Síndrome metabólico": [],
    "Hipertensión arterial sistémica/pulmonar primaria": [],
    "Enfermedad valvular": [],
    "Miocardiopatías y enfermedad de Chagas": [],
    "Sistemas biológicos: celular, molecular y producción de energía": [],
    "Cardiopatías congénitas": [],
    "Nefropatías": [],
    "Elaboración de dispositivos intracardiacos": [],
    "Medio ambiente y sociomedicina": [],
    "COVID-19 (SARS-Cov-2)": [],
    "Otros": [],
}

# ====================
# DEPARTAMENTOS INCICH
# ====================
DEPARTAMENTOS_INCICH = [
    "Bioquímica",
    "Biología Molecular",
    "Biomedicina Cardiovascular",
    "Consulta Externa (Dermatología, Endocrinología, etc.)",
    "Departamento de Enseñanza de Enfermería (DEE)",
    "Endocrinología",
    "Farmacología",
    "Fisiología",
    "Fisiopatología Cardio-Renal",
    "Fisiotepatología Cardiorenal",
    "Inmunología",
    "Instrumentación Electromecánica",
    "Oficina de Apoyo Sistemático para la Investigación Superior (OASIS)",
    "Unidad de Investigación UNAM-INC"
]

# ====================
# CONFIGURACIÓN INICIAL
# ====================
class Config:
    def __init__(self):
        # Configuración SFTP
        self.REMOTE_TESIS_FILE = "pro_tesis_total.csv"  # Nombre completo del archivo remoto
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

def main():
    st.set_page_config(
        page_title="Análisis de Tesis",
        page_icon="📚",
        layout="wide"
    )

    # Mostrar logo si existe
    if Path(CONFIG.LOGO_PATH).exists():
        st.image(CONFIG.LOGO_PATH, width=200)

    st.title("Análisis de Tesis")

    # Sincronizar archivo tesis_total.csv al inicio
    if not sync_tesis_file():
        st.warning("⚠️ Trabajando con copia local de tesis_total.csv debido a problemas de conexión")

    # Verificar si el archivo local existe
    if not Path("tesis_total.csv").exists():
        st.error("No se encontró el archivo tesis_total.csv")
        return

    try:
        # Leer y procesar el archivo
        df = pd.read_csv("tesis_total.csv")

        # Verificar campos requeridos
        required_columns = ['directores', 'titulo_tesis', 'pub_date', 'estado', 'selected_keywords']
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            st.warning(f"Faltan campos requeridos: {', '.join(missing_columns)}")
            return

        # Convertir y validar fechas
        df['pub_date'] = pd.to_datetime(df['pub_date'], errors='coerce')
        df = df[(df['estado'] == 'A') & (df['pub_date'].notna())]

        if df.empty:
            st.warning("No hay tesis válidas para analizar")
            return

        st.success(f"Datos cargados correctamente. Registros activos: {len(df)}")

        # Obtener rangos de fechas
        min_date = df['pub_date'].min()
        max_date = df['pub_date'].max()

        # Selector de rango de fechas
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

        # Calcular fechas
        start_day = 1
        end_day = calendar.monthrange(end_year, end_month)[1]
        date_start = datetime(start_year, start_month, start_day)
        date_end = datetime(end_year, end_month, end_day)

        # Filtrar dataframe
        filtered_df = df[(df['pub_date'] >= pd.to_datetime(date_start)) &
                       (df['pub_date'] <= pd.to_datetime(date_end))]
        unique_tesis = filtered_df.drop_duplicates(subset=['titulo_tesis'])

        st.markdown(f"**Periodo:** {date_start.strftime('%d/%m/%Y')} - {date_end.strftime('%d/%m/%Y')}")
        st.markdown(f"**Registros encontrados:** {len(filtered_df)}")
        st.markdown(f"**Tesis únicas:** {len(unique_tesis)}")

        if len(filtered_df) != len(unique_tesis):
            st.warning(f"⚠️ Se detectaron {len(filtered_df) - len(unique_tesis)} registros duplicados")

        if filtered_df.empty:
            st.warning("No hay tesis en el periodo seleccionado")
            return

        # Análisis consolidado
        st.header("📊 Estadísticas Consolidadas")

        # 1. Productividad por director
        st.subheader("🔍 Productividad por director de tesis")
        director_stats = filtered_df.groupby('directores').agg(
            Tesis_Unicas=('titulo_tesis', lambda x: len(set(x)))
        ).reset_index().sort_values('Tesis_Unicas', ascending=False)

        director_stats.columns = ['Director', 'Tesis únicas']
        total_row = pd.DataFrame({'Director': ['TOTAL'], 'Tesis únicas': [director_stats['Tesis únicas'].sum()]})
        director_stats = pd.concat([director_stats.head(10), total_row], ignore_index=True)

        for index, row in director_stats.iterrows():
            if row['Director'] != 'TOTAL':
                with st.expander(f"{row['Director']} - {row['Tesis únicas']} tesis"):
                    director_tesis = filtered_df[filtered_df['directores'] == row['Director']].drop_duplicates('titulo_tesis')

                    # Mostrar todos los campos disponibles excepto 'estado'
                    display_cols = [col for col in director_tesis.columns if col != 'estado']
                    st.dataframe(
                        director_tesis[display_cols],
                        column_config={
                            "pub_date": st.column_config.DateColumn("Fecha", format="DD/MM/YYYY"),
                            "selected_keywords": st.column_config.ListColumn("Palabras clave")
                        },
                        hide_index=True,
                        use_container_width=True
                    )

                    csv = director_tesis.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "Descargar CSV",
                        data=csv,
                        file_name=f"tesis_{row['Director'].replace(' ', '_')}.csv",
                        mime='text/csv',
                        key=f"download_{index}"
                    )

        # 2. Tipos de tesis
        st.subheader("🎓 Distribución por tipo de tesis")
        tipo_stats = unique_tesis['tipo_tesis'].value_counts().reset_index()
        tipo_stats.columns = ['Tipo', 'Cantidad']
        total_tipo = pd.DataFrame({'Tipo': ['TOTAL'], 'Cantidad': [tipo_stats['Cantidad'].sum()]})
        tipo_completo = pd.concat([tipo_stats, total_tipo], ignore_index=True)
        st.dataframe(tipo_completo)

        # 3. Líneas de investigación (VERSIÓN CORREGIDA)
        st.subheader("🧪 Líneas de investigación")
        try:
            # Nuevo enfoque para manejar líneas con comas
            keyword_counts = {}

            for keywords in unique_tesis['selected_keywords']:
                if pd.notna(keywords):
                    # Procesar como lista literal si empieza con [
                    if keywords.startswith('['):
                        try:
                            # Evaluar como lista literal de Python
                            keyword_list = eval(keywords)
                            if isinstance(keyword_list, list):
                                for kw in keyword_list:
                                    kw = str(kw).strip()
                                    if kw:
                                        keyword_counts[kw] = keyword_counts.get(kw, 0) + 1
                        except:
                            # Fallback para formato incorrecto
                            cleaned = keywords.strip("[]").replace("'", "").split(",")
                            for kw in cleaned:
                                kw = kw.strip()
                                if kw:
                                    keyword_counts[kw] = keyword_counts.get(kw, 0) + 1
                    else:
                        # Tratar todo el string como una sola línea de investigación
                        keyword_counts[keywords.strip()] = keyword_counts.get(keywords.strip(), 0) + 1

            # Convertir a DataFrame
            kw_stats = pd.DataFrame.from_dict(keyword_counts, orient='index', columns=['Frecuencia'])
            kw_stats = kw_stats.reset_index().rename(columns={'index': 'Línea'})
            kw_stats = kw_stats.sort_values('Frecuencia', ascending=False)

            # Agregar total
            total_kw = pd.DataFrame({'Línea': ['TOTAL'], 'Frecuencia': [kw_stats['Frecuencia'].sum()]})
            kw_completo = pd.concat([kw_stats.head(10), total_kw], ignore_index=True)
            st.dataframe(kw_completo)
        except Exception as e:
            st.warning(f"Error procesando palabras clave: {str(e)}")

        # 4. Distribución por departamento
        if 'departamento' in unique_tesis.columns:
            st.subheader("🏛️ Distribución por departamento")
            depto_stats = unique_tesis['departamento'].value_counts().reset_index()
            depto_stats.columns = ['Departamento', 'Cantidad']
            total_depto = pd.DataFrame({'Departamento': ['TOTAL'], 'Cantidad': [depto_stats['Cantidad'].sum()]})
            depto_completo = pd.concat([depto_stats, total_depto], ignore_index=True)
            st.dataframe(depto_completo)

        # 5. Distribución temporal
        st.subheader("🕰️ Distribución mensual")
        time_stats = unique_tesis['pub_date'].dt.to_period('M').astype(str).value_counts().sort_index().reset_index()
        time_stats.columns = ['Mes-Año', 'Cantidad']
        total_time = pd.DataFrame({'Mes-Año': ['TOTAL'], 'Cantidad': [time_stats['Cantidad'].sum()]})
        time_completo = pd.concat([time_stats, total_time], ignore_index=True)
        st.dataframe(time_completo)

        # 6. Distribución por SNI
        if 'sni' in unique_tesis.columns:
            st.subheader("📊 Distribución por nivel de SNI")
            sni_stats = unique_tesis['sni'].value_counts().reset_index()
            sni_stats.columns = ['Nivel SNI', 'Cantidad']
            total_sni = pd.DataFrame({'Nivel SNI': ['TOTAL'], 'Cantidad': [sni_stats['Cantidad'].sum()]})
            sni_completo = pd.concat([sni_stats, total_sni], ignore_index=True)
            st.dataframe(sni_completo)

        # 7. Distribución por SII
        if 'sii' in unique_tesis.columns:
            st.subheader("📈 Distribución por nivel de SII")
            sii_stats = unique_tesis['sii'].value_counts().reset_index()
            sii_stats.columns = ['Nivel SII', 'Cantidad']
            total_sii = pd.DataFrame({'Nivel SII': ['TOTAL'], 'Cantidad': [sii_stats['Cantidad'].sum()]})
            sii_completo = pd.concat([sii_stats, total_sii], ignore_index=True)
            st.dataframe(sii_completo)

        # 8. Tipo de nombramiento
        if 'nombramiento' in unique_tesis.columns:
            st.subheader("👨‍🏫 Distribución por nombramiento")
            nomb_stats = unique_tesis['nombramiento'].value_counts().reset_index()
            nomb_stats.columns = ['Nombramiento', 'Cantidad']
            total_nomb = pd.DataFrame({'Nombramiento': ['TOTAL'], 'Cantidad': [nomb_stats['Cantidad'].sum()]})
            nomb_completo = pd.concat([nomb_stats, total_nomb], ignore_index=True)
            st.dataframe(nomb_completo)

        # 9. Distribución por idioma
        if 'idioma' in unique_tesis.columns:
            st.subheader("🌐 Distribución por idioma")
            idioma_stats = unique_tesis['idioma'].value_counts().reset_index()
            idioma_stats.columns = ['Idioma', 'Cantidad']
            total_idioma = pd.DataFrame({'Idioma': ['TOTAL'], 'Cantidad': [idioma_stats['Cantidad'].sum()]})
            idioma_completo = pd.concat([idioma_stats, total_idioma], ignore_index=True)
            st.dataframe(idioma_completo)

        # 10. Estudiantes con más tesis
        st.subheader("👨‍🎓 Estudiantes")
        estudiante_stats = unique_tesis['estudiante'].value_counts().reset_index()
        estudiante_stats.columns = ['Estudiante', 'Cantidad']
        total_est = pd.DataFrame({'Estudiante': ['TOTAL'], 'Cantidad': [estudiante_stats['Cantidad'].sum()]})
        est_completo = pd.concat([estudiante_stats.head(10), total_est], ignore_index=True)
        st.dataframe(est_completo)

        # Descarga de archivo completo
        st.header("📥 Descargar datos")
        if Path("tesis_total.csv").exists():
            with open("tesis_total.csv", "rb") as f:
                st.download_button(
                    "Descargar archivo completo",
                    data=f,
                    file_name="pro_tesis_total.csv",
                    mime="text/csv"
                )

    except Exception as e:
        st.error(f"Error al procesar datos: {str(e)}")
        logging.error(f"Error en main: {str(e)}")

if __name__ == "__main__":
    main()

