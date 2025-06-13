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
    "Accidente Cerebrovascular": ["accidente cerebrovascular", "acv", "ictus", "stroke"],
    "Alzheimer": ["alzheimer", "demencia", "enfermedad neurodegenerativa"],
    "Arritmias": [
        "arritmia", "fibrilación auricular", "fa", "flutter auricular",
        "taquicardia ventricular", "tv", "fibrilación ventricular", "fv",
        "bradicardia", "bloqueo auriculoventricular", "síndrome de brugada",
        "síndrome de qt largo", "marcapasos", "desfibrilador automático"
    ],
    "Bioinformática": ["bioinformática", "genómica computacional", "análisis de secuencias", "biología de sistemas"],
    "Bioquímica": ["bioquímica", "metabolismo", "enzimas", "rutas metabólicas"],
    "Biología Molecular": ["adn", "arn", "transcripción", "replicación"],
    "Biomarcadores Cardíacos": [
        "troponina", "nt-probnp", "bnp", "ck-mb", "lactato deshidrogenasa",
        "mioglobina", "péptidos natriuréticos"
    ],
    "Biotecnología": ["biotecnología", "terapia génica", "crispr", "organismos modificados genéticamente"],
    "Cáncer de Mama": ["cáncer de mama", "tumor mamario", "neoplasia mamaria"],
    "Cardiología Pediátrica": [
        "cardiopatía congénita", "comunicación interauricular", "cia",
        "comunicación interventricular", "civ", "tetralogía de fallot",
        "transposición grandes vasos", "ductus arterioso persistente"
    ],
    "Cardiomiopatías": [
        "cardiomiopatía", "miocardiopatía", "cardiomiopatía hipertrófica", "hcm",
        "cardiomiopatía dilatada", "dcm", "cardiomiopatía restrictiva",
        "displasia arritmogénica", "miocardiopatía no compactada", "amiloidosis cardíaca"
    ],
    "Endocrinología": ["diabetes", "tiroides", "hormonas", "metabolismo"],
    "Enfermedad Vascular Periférica": [
        "enfermedad arterial periférica", "eap", "claudicación intermitente",
        "índice tobillo-brazo", "isquemia crítica", "arteriopatía obliterante"
    ],
    "Epidemiología": ["epidemiología", "estudios poblacionales", "incidencia", "prevalencia"],
    "Epilepsia": ["epilepsia", "crisis epiléptica", "convulsiones"],
    "Farmacología": ["farmacología", "fármacos", "dosis-respuesta", "toxicidad"],
    "Gastroenterología": ["colon", "hígado", "páncreas", "enfermedad inflamatoria intestinal"],
    "Genética": ["genética", "mutaciones", "genoma humano", "síndromes genéticos"],
    "Hipertensión y Riesgo Cardiovascular": [
        "hipertensión arterial", "hta", "hipertensión pulmonar",
        "crisis hipertensiva", "mapa", "monitorización ambulatoria",
        "riesgo cardiovascular", "score framingham", "ascvd"
    ],
    "Inmunología": ["autoinmunidad", "inmunodeficiencia", "alergias", "linfocitos"],
    "Inmunoterapia": ["inmunoterapia", "terapia car-t", "checkpoint inmunológico"],
    "Insuficiencia Cardíaca": [
        "insuficiencia cardíaca", "ic", "fallo cardíaco", "disfunción ventricular",
        "icfe", "icfd", "fracción de eyección reducida", "fracción de eyección preservada",
        "nyha clase ii", "nyha clase iii", "edema pulmonar", "congestión venosa"
    ],
    "Investigación Clínica": ["ensayo clínico", "randomizado", "estudio de cohorte", "fase iii"],
    "Leucemia": ["leucemia", "leucemias agudas", "leucemia mieloide"],
    "Microbiología": ["microbiología", "bacterias", "virus", "antimicrobianos"],
    "Nefrología": ["insuficiencia renal", "glomerulonefritis", "diálisis"],
    "Neumología": ["asma", "epoc", "fibrosis pulmonar", "síndrome de apnea del sueño"],
    "Neurociencia": ["neurociencia", "plasticidad neuronal", "sinapsis", "neurodegeneración"],
    "Oncología Molecular": ["oncología molecular", "mutaciones tumorales", "biomarcadores cáncer"],
    "Procedimientos Cardiológicos": [
        "cateterismo cardíaco", "angioplastia", "stent coronario",
        "bypass coronario", "cabg", "ecocardiograma", "eco stress",
        "resonancia cardíaca", "prueba de esfuerzo", "holter"
    ],
    "Síndrome Coronario Agudo": [
        "síndrome coronario agudo", "sca", "infarto agudo de miocardio", "iam",
        "iamcest", "iamnest", "angina inestable", "troponina elevada",
        "oclusión coronaria", "elevación st", "depresión st"
    ],
    "Valvulopatías": [
        "valvulopatía", "estenosis aórtica", "insuficiencia aórtica",
        "stenosis mitral", "insuficiencia mitral", "prolapso mitral",
        "tavi", "taavi", "anillo mitral", "reemplazo valvular"
    ],
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
    
    # Añadir logo en la parte superior
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
        # Leer y procesar el archivo con los nuevos campos sni y sii
        df = pd.read_csv("tesis_total.csv")
        
        # Verificar que los campos importantes existen
        required_columns = ['directores', 'titulo_tesis', 'pub_date', 'estado', 'selected_keywords']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            st.warning(f"El archivo tesis_total.csv no contiene los campos requeridos: {', '.join(missing_columns)}")
            return
        
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
        unique_tesis = filtered_df.drop_duplicates(subset=['titulo_tesis'])
        
        st.markdown(f"**Periodo seleccionado:** {date_start.strftime('%d/%m/%Y')} - {date_end.strftime('%d/%m/%Y')}",
                   help="Rango de fechas seleccionado para el análisis.")
        st.markdown(f"**Registros encontrados:** {len(filtered_df)}",
                   help="Total de registros en el periodo, incluyendo posibles duplicados de la misma tesis.")
        st.markdown(f"**Tesis únicas:** {len(unique_tesis)}",
                   help="Cantidad de tesis distintas, eliminando duplicados.")
        
        if len(filtered_df) != len(unique_tesis):
            st.warning(f"⚠️ **Nota:** Se detectaron {len(filtered_df) - len(unique_tesis)} registros duplicados de la misma tesis.")
        
        if filtered_df.empty:
            st.warning("No hay tesis en el periodo seleccionado")
            return
        
        # Análisis consolidado en tablas
        st.header("📊 Estadísticas Consolidadas",
                help="Métricas generales basadas en los filtros aplicados.")
        
        # Tabla 1: Productividad por director (TESIS ÚNICAS)
        st.subheader("🔍 Productividad por Director",
                   help="Muestra cuántas tesis únicas ha dirigido cada investigador.")
        
        # Crear dataframe con información de directores
        director_stats = filtered_df.groupby('directores').agg(
            Tesis_Unicas=('titulo_tesis', lambda x: len(set(x)))
        ).reset_index()
        
        director_stats = director_stats.sort_values('Tesis_Unicas', ascending=False)
        director_stats.columns = ['Director', 'Tesis únicas']
        
        # Añadir fila de totales
        total_row = pd.DataFrame({
            'Director': ['TOTAL'],
            'Tesis únicas': [director_stats['Tesis únicas'].sum()]
        })
        director_stats = pd.concat([director_stats.head(10), total_row], ignore_index=True)
        
        # Mostrar tabla con enlaces clickeables
        for index, row in director_stats.iterrows():
            if row['Director'] != 'TOTAL':
                # Crear un expander para cada director
                with st.expander(f"{row['Director']} - {row['Tesis únicas']} tesis"):
                    # Filtrar las tesis del director
                    director_tesis = filtered_df[filtered_df['directores'] == row['Director']]
                    unique_tesis_director = director_tesis.drop_duplicates(subset=['titulo_tesis'])
                    
                    # Mostrar las tesis (incluyendo los nuevos campos si existen)
                    display_columns = ['titulo_tesis', 'tipo_tesis', 'pub_date', 'estudiante']
                    if 'sni' in unique_tesis_director.columns and 'sii' in unique_tesis_director.columns:
                        display_columns.extend(['sni', 'sii'])
                    
                    st.write(f"Tesis dirigidas por {row['Director']}:")
                    st.dataframe(unique_tesis_director[display_columns])
                    
                    # Opción para descargar en CSV
                    csv = unique_tesis_director.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Descargar producción de tesis en CSV",
                        data=csv,
                        file_name=f"tesis_{row['Director'].replace(' ', '_')}.csv",
                        mime='text/csv',
                        key=f"download_{index}"
                    )
        
        # Tabla 2: Tipos de tesis más comunes (TESIS ÚNICAS)
        st.subheader("🎓 Tipos de Tesis",
                   help="Distribución de los tipos de tesis.")
        tipo_stats = unique_tesis['tipo_tesis'].value_counts().reset_index()
        tipo_stats.columns = ['Tipo de tesis', 'Tesis únicas']
        
        # Añadir fila de totales
        total_row = pd.DataFrame({
            'Tipo de tesis': ['TOTAL'],
            'Tesis únicas': [tipo_stats['Tesis únicas'].sum()]
        })
        tipo_stats = pd.concat([tipo_stats, total_row], ignore_index=True)
        st.dataframe(tipo_stats, hide_index=True)
        
        # Tabla 3: Enfoques más frecuentes (TESIS ÚNICAS)
        st.subheader("🧪 Enfoques más Frecuentes",
                   help="Palabras clave más utilizadas en las tesis, indicando las áreas de investigación predominantes.")
        try:
            all_keywords = []
            for keywords in unique_tesis['selected_keywords']:
                if pd.notna(keywords):
                    # Limpiar y procesar las palabras clave
                    cleaned = str(keywords).strip("[]'").replace("'", "").split(", ")
                    all_keywords.extend([k.strip() for k in cleaned if k.strip()])
            
            keyword_stats = pd.Series(all_keywords).value_counts().reset_index()
            keyword_stats.columns = ['Enfoque', 'Frecuencia']
            
            # Añadir fila de totales
            total_row = pd.DataFrame({
                'Enfoque': ['TOTAL'],
                'Frecuencia': [keyword_stats['Frecuencia'].sum()]
            })
            keyword_stats = pd.concat([keyword_stats.head(10), total_row], ignore_index=True)
            st.dataframe(keyword_stats, hide_index=True)
        except Exception as e:
            st.warning(f"No se pudieron procesar las palabras clave: {str(e)}")
        
        # Tabla 4: Distribución por departamentos (TESIS ÚNICAS)
        if 'departamento' in unique_tesis.columns:
            st.subheader("🏛️ Distribución por Departamento",
                       help="Clasificación de tesis según el departamento de adscripción del director.")
            depto_stats = unique_tesis['departamento'].value_counts().reset_index()
            depto_stats.columns = ['Departamento', 'Tesis únicas']
            
            # Añadir fila de totales
            total_row = pd.DataFrame({
                'Departamento': ['TOTAL'],
                'Tesis únicas': [depto_stats['Tesis únicas'].sum()]
            })
            depto_stats = pd.concat([depto_stats, total_row], ignore_index=True)
            st.dataframe(depto_stats, hide_index=True)
        else:
            st.warning("El campo 'departamento' no está disponible en los datos")
        
        # Tabla 5: Distribución temporal (TESIS ÚNICAS)
        st.subheader("🕰️ Distribución Mensual",
                    help="Evolución mensual de la producción de tesis en el periodo seleccionado.")

        # Convertir a formato "YYYY-MM" - CORRECCIÓN: usar astype(str) en lugar de astipestr
        time_stats = unique_tesis['pub_date'].dt.to_period('M').astype(str).value_counts().sort_index().reset_index()
        time_stats.columns = ['Mes-Año', 'Tesis únicas']

        # Añadir fila de totales
        total_row = pd.DataFrame({
            'Mes-Año': ['TOTAL'],
            'Tesis únicas': [time_stats['Tesis únicas'].sum()]
        })
        time_stats = pd.concat([time_stats, total_row], ignore_index=True)
        st.dataframe(time_stats, hide_index=True)
        
        # Tabla 6: Distribución por nivel SNI (TESIS ÚNICAS)
        if 'sni' in unique_tesis.columns:
            st.subheader("📊 Distribución por Nivel SNI",
                        help="Clasificación de tesis según el nivel del Sistema Nacional de Investigadores (SNI) de los directores.")
            sni_stats = unique_tesis['sni'].value_counts().reset_index()
            sni_stats.columns = ['Nivel SNI', 'Tesis únicas']
            
            # Añadir fila de totales
            total_row = pd.DataFrame({
                'Nivel SNI': ['TOTAL'],
                'Tesis únicas': [sni_stats['Tesis únicas'].sum()]
            })
            sni_stats = pd.concat([sni_stats, total_row], ignore_index=True)
            st.dataframe(sni_stats, hide_index=True)
        else:
            st.warning("El campo 'sni' no está disponible en los datos")
        
        # Tabla 7: Distribución por nivel SII (TESIS ÚNICAS)
        if 'sii' in unique_tesis.columns:
            st.subheader("📈 Distribución por Nivel SII",
                        help="Clasificación de tesis según el nivel del Sistema Institucional de Investigación (SII) de los directores.")
            sii_stats = unique_tesis['sii'].value_counts().reset_index()
            sii_stats.columns = ['Nivel SII', 'Tesis únicas']
            
            # Añadir fila de totales
            total_row = pd.DataFrame({
                'Nivel SII': ['TOTAL'],
                'Tesis únicas': [sii_stats['Tesis únicas'].sum()]
            })
            sii_stats = pd.concat([sii_stats, total_row], ignore_index=True)
            st.dataframe(sii_stats, hide_index=True)
        else:
            st.warning("El campo 'sii' no está disponible en los datos")
        
        # Tabla 8: Distribución por idioma (TESIS ÚNICAS)
        if 'idioma' in unique_tesis.columns:
            st.subheader("🌐 Distribución por Idioma",
                        help="Idiomas en los que están escritas las tesis.")
            idioma_stats = unique_tesis['idioma'].value_counts().reset_index()
            idioma_stats.columns = ['Idioma', 'Tesis únicas']
            
            # Añadir fila de totales
            total_row = pd.DataFrame({
                'Idioma': ['TOTAL'],
                'Tesis únicas': [idioma_stats['Tesis únicas'].sum()]
            })
            idioma_stats = pd.concat([idioma_stats, total_row], ignore_index=True)
            st.dataframe(idioma_stats, hide_index=True)
        else:
            st.warning("El campo 'idioma' no está disponible en los datos")
            
        # Tabla 9: Estudiantes con más tesis (TESIS ÚNICAS)
        st.subheader("👨‍🎓 Estudiantes con más tesis",
                    help="Listado de estudiantes ordenados por cantidad de tesis realizadas.")
        estudiante_stats = unique_tesis['estudiante'].value_counts().reset_index()
        estudiante_stats.columns = ['Estudiante', 'Tesis únicas']
        
        # Añadir fila de totales
        total_row = pd.DataFrame({
            'Estudiante': ['TOTAL'],
            'Tesis únicas': [estudiante_stats['Tesis únicas'].sum()]
        })
        estudiante_stats = pd.concat([estudiante_stats.head(10), total_row], ignore_index=True)
        st.dataframe(estudiante_stats, hide_index=True)
        
        # ==========================================
        # SECCIÓN: DESCARGAR ARCHIVO COMPLETO
        # ==========================================
        st.header("📥 Descargar Datos Completos")
        
        # Opción para descargar el archivo pro_tesis_total.csv
        if Path("tesis_total.csv").exists():
            with open("tesis_total.csv", "rb") as file:
                btn = st.download_button(
                    label="Descargar archivo pro_tesis_total.csv completo",
                    data=file,
                    file_name="pro_tesis_total.csv",
                    mime="text/csv",
                    help="Descarga el archivo CSV completo con todos los datos de tesis"
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
