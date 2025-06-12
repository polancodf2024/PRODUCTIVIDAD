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
# CATEGORÍAS DE KEYWORDS PARA CAPÍTULOS
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
        "iamcest", "iamnest", "angina inestabile", "troponina elevada",
        "oclusión coronaria", "elevación st", "depresión st"
    ],
    "Valvulopatías": [
        "valvulopatía", "estenosis aórtica", "insuficiencia aórtica",
        "stenosis mitral", "insuficiencia mitral", "prolapso mitral",
        "tavi", "taavi", "anillo mitral", "reemplazo valvular"
    ],
}

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

def main():
    st.set_page_config(
        page_title="Análisis de Capítulos",
        page_icon="📚",
        layout="wide"
    )
    
    # Añadir logo en la parte superior
    if Path(CONFIG.LOGO_PATH).exists():
        st.image(CONFIG.LOGO_PATH, width=200)
    
    st.title("Análisis de Capítulos de Libros")
    
    # Sincronizar archivo capitulos_total.csv al inicio
    if not sync_capitulos_file():
        st.warning("⚠️ Trabajando con copia local de capitulos_total.csv debido a problemas de conexión")
    
    # Verificar si el archivo local existe
    if not Path("capitulos_total.csv").exists():
        st.error("No se encontró el archivo capitulos_total.csv")
        return
    
    try:
        # Leer y procesar el archivo con los nuevos campos sni y sii
        df = pd.read_csv("capitulos_total.csv")
        
        # Verificar que los campos importantes existen
        required_columns = ['autor_principal', 'titulo_libro', 'titulo_capitulo', 'pub_date', 'estado', 'selected_keywords']
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
        
        # Obtener capítulos únicos para estadísticas precisas
        unique_capitulos = filtered_df.drop_duplicates(subset=['titulo_capitulo'])
        
        st.markdown(f"**Periodo seleccionado:** {date_start.strftime('%d/%m/%Y')} - {date_end.strftime('%d/%m/%Y')}",
                   help="Rango de fechas seleccionado para el análisis.")
        st.markdown(f"**Registros encontrados:** {len(filtered_df)}",
                   help="Total de registros en el periodo, incluyendo posibles duplicados del mismo capítulo.")
        st.markdown(f"**Capítulos únicos:** {len(unique_capitulos)}",
                   help="Cantidad de capítulos distintos, eliminando duplicados.")
        
        if len(filtered_df) != len(unique_capitulos):
            st.warning(f"⚠️ **Nota:** Se detectaron {len(filtered_df) - len(unique_capitulos)} registros duplicados del mismo capítulo.")
        
        if filtered_df.empty:
            st.warning("No hay capítulos en el periodo seleccionado")
            return
        
        # Análisis consolidado en tablas
        st.header("📊 Estadísticas Consolidadas",
                help="Métricas generales basadas en los filtros aplicados.")
        
        # Tabla 1: Productividad por investigador (CAPÍTULOS ÚNICOS) con participación
        st.subheader("🔍 Productividad por Investigador",
                   help="Muestra cuántos capítulos únicos tiene cada investigador y su tipo de participación.")
        
        # Crear dataframe con información de participación
        investigator_stats = filtered_df.groupby('autor_principal').agg(
            Capítulos_Unicos=('titulo_capitulo', lambda x: len(set(x))),
            Participaciones=('tipo_participacion', lambda x: ', '.join(sorted(set(x))))
        ).reset_index()
        
        investigator_stats = investigator_stats.sort_values('Capítulos_Unicos', ascending=False)
        investigator_stats.columns = ['Investigador', 'Capítulos únicos', 'Tipo de participación']
        
        # Añadir fila de totales
        total_row = pd.DataFrame({
            'Investigador': ['TOTAL'],
            'Capítulos únicos': [investigator_stats['Capítulos únicos'].sum()],
            'Tipo de participación': ['']
        })
        investigator_stats = pd.concat([investigator_stats.head(10), total_row], ignore_index=True)
        
        # Mostrar tabla con enlaces clickeables
        for index, row in investigator_stats.iterrows():
            if row['Investigador'] != 'TOTAL':
                # Crear un expander para cada investigador
                with st.expander(f"{row['Investigador']} - {row['Capítulos únicos']} capítulos"):
                    # Filtrar los capítulos del investigador
                    investigator_capitulos = filtered_df[filtered_df['autor_principal'] == row['Investigador']]
                    unique_capitulos_investigator = investigator_capitulos.drop_duplicates(subset=['titulo_capitulo'])
                    
                    # Mostrar los capítulos (incluyendo los nuevos campos si existen)
                    display_columns = ['titulo_libro', 'titulo_capitulo', 'editorial', 'pub_date', 'isbn_issn']
                    if 'sni' in unique_capitulos_investigator.columns and 'sii' in unique_capitulos_investigator.columns:
                        display_columns.extend(['sni', 'sii'])
                    
                    st.write(f"Capítulos de {row['Investigador']}:")
                    st.dataframe(unique_capitulos_investigator[display_columns])
                    
                    # Opción para descargar en CSV
                    csv = unique_capitulos_investigator.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Descargar producción de capítulos en CSV",
                        data=csv,
                        file_name=f"capitulos_{row['Investigador'].replace(' ', '_')}.csv",
                        mime='text/csv',
                        key=f"download_{index}"
                    )
        
        # Tabla 2: Editoriales más utilizadas (CAPÍTULOS ÚNICOS)
        st.subheader("🏢 Editoriales más Utilizadas",
                   help="Listado de editoriales ordenadas por cantidad de capítulos publicados.")
        editorial_stats = unique_capitulos.groupby('editorial').agg(
            Total_Capitulos=('editorial', 'size')
        ).reset_index()
        editorial_stats = editorial_stats.sort_values('Total_Capitulos', ascending=False)
        editorial_stats.columns = ['Editorial', 'Capítulos únicos']
        
        # Añadir fila de totales
        total_row = pd.DataFrame({
            'Editorial': ['TOTAL'],
            'Capítulos únicos': [editorial_stats['Capítulos únicos'].sum()]
        })
        editorial_stats = pd.concat([editorial_stats.head(10), total_row], ignore_index=True)
        st.dataframe(editorial_stats, hide_index=True)
        
        # Tabla 3: Tipos de participación más comunes (CAPÍTULOS ÚNICOS)
        st.subheader("🎭 Tipos de Participación",
                   help="Distribución de los tipos de participación en los capítulos.")
        participacion_stats = unique_capitulos['tipo_participacion'].value_counts().reset_index()
        participacion_stats.columns = ['Tipo de participación', 'Capítulos únicos']
        
        # Añadir fila de totales
        total_row = pd.DataFrame({
            'Tipo de participación': ['TOTAL'],
            'Capítulos únicos': [participacion_stats['Capítulos únicos'].sum()]
        })
        participacion_stats = pd.concat([participacion_stats, total_row], ignore_index=True)
        st.dataframe(participacion_stats, hide_index=True)
        
        # Tabla 4: Enfoques más frecuentes (CAPÍTULOS ÚNICOS)
        st.subheader("🧪 Enfoques más Frecuentes",
                   help="Palabras clave más utilizadas en los capítulos, indicando las áreas de investigación predominantes.")
        try:
            all_keywords = []
            for keywords in unique_capitulos['selected_keywords']:
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
        
        # Tabla 5: Distribución por departamentos (CAPÍTULOS ÚNICOS)
        if 'departamento' in unique_capitulos.columns:
            st.subheader("🏛️ Distribución por Departamento",
                       help="Clasificación de capítulos según el departamento de adscripción del autor principal.")
            depto_stats = unique_capitulos['departamento'].value_counts().reset_index()
            depto_stats.columns = ['Departamento', 'Capítulos únicos']
            
            # Añadir fila de totales
            total_row = pd.DataFrame({
                'Departamento': ['TOTAL'],
                'Capítulos únicos': [depto_stats['Capítulos únicos'].sum()]
            })
            depto_stats = pd.concat([depto_stats, total_row], ignore_index=True)
            st.dataframe(depto_stats, hide_index=True)
        else:
            st.warning("El campo 'departamento' no está disponible en los datos")
        
        # Tabla 6: Distribución temporal (CAPÍTULOS ÚNICOS)
        st.subheader("🕰️ Distribución Mensual",
                    help="Evolución mensual de la producción de capítulos en el periodo seleccionado.")

        # Convertir a formato "YYYY-MM"
        time_stats = unique_capitulos['pub_date'].dt.to_period('M').astype(str).value_counts().sort_index().reset_index()
        time_stats.columns = ['Mes-Año', 'Capítulos únicos']

        # Añadir fila de totales
        total_row = pd.DataFrame({
            'Mes-Año': ['TOTAL'],
            'Capítulos únicos': [time_stats['Capítulos únicos'].sum()]
        })
        time_stats = pd.concat([time_stats, total_row], ignore_index=True)
        st.dataframe(time_stats, hide_index=True)
        
        # Tabla 7: Distribución por nivel SNI (CAPÍTULOS ÚNICOS)
        if 'sni' in unique_capitulos.columns:
            st.subheader("📊 Distribución por Nivel SNI",
                        help="Clasificación de capítulos según el nivel del Sistema Nacional de Investigadores (SNI) de los autores.")
            sni_stats = unique_capitulos['sni'].value_counts().reset_index()
            sni_stats.columns = ['Nivel SNI', 'Capítulos únicos']
            
            # Añadir fila de totales
            total_row = pd.DataFrame({
                'Nivel SNI': ['TOTAL'],
                'Capítulos únicos': [sni_stats['Capítulos únicos'].sum()]
            })
            sni_stats = pd.concat([sni_stats, total_row], ignore_index=True)
            st.dataframe(sni_stats, hide_index=True)
        else:
            st.warning("El campo 'sni' no está disponible en los datos")
        
        # Tabla 8: Distribución por nivel SII (CAPÍTULOS ÚNICOS)
        if 'sii' in unique_capitulos.columns:
            st.subheader("📈 Distribución por Nivel SII",
                        help="Clasificación de capítulos según el nivel del Sistema Institucional de Investigación (SII) de los autores.")
            sii_stats = unique_capitulos['sii'].value_counts().reset_index()
            sii_stats.columns = ['Nivel SII', 'Capítulos únicos']
            
            # Añadir fila de totales
            total_row = pd.DataFrame({
                'Nivel SII': ['TOTAL'],
                'Capítulos únicos': [sii_stats['Capítulos únicos'].sum()]
            })
            sii_stats = pd.concat([sii_stats, total_row], ignore_index=True)
            st.dataframe(sii_stats, hide_index=True)
        else:
            st.warning("El campo 'sii' no está disponible en los datos")
        
        # Tabla 9: Distribución por países de distribución (CAPÍTULOS ÚNICOS)
        if 'paises_distribucion' in unique_capitulos.columns:
            st.subheader("🌍 Distribución por Países",
                        help="Países donde se distribuyen los libros que contienen los capítulos publicados.")
            
            try:
                all_countries = []
                for countries in unique_capitulos['paises_distribucion']:
                    if pd.notna(countries):
                        cleaned = str(countries).strip().split(", ")
                        all_countries.extend([c.strip() for c in cleaned if c.strip()])
                
                country_stats = pd.Series(all_countries).value_counts().reset_index()
                country_stats.columns = ['País', 'Frecuencia']
                
                # Añadir fila de totales
                total_row = pd.DataFrame({
                    'País': ['TOTAL'],
                    'Frecuencia': [country_stats['Frecuencia'].sum()]
                })
                country_stats = pd.concat([country_stats.head(10), total_row], ignore_index=True)
                st.dataframe(country_stats, hide_index=True)
            except:
                st.warning("No se pudieron procesar los países de distribución")

        # Tabla 10: Distribución por idioma (CAPÍTULOS ÚNICOS)
        if 'idiomas_disponibles' in unique_capitulos.columns:
            st.subheader("🌐 Distribución por Idioma",
                        help="Idiomas en los que están publicados los libros que contienen los capítulos.")
            idioma_stats = unique_capitulos['idiomas_disponibles'].value_counts().reset_index()
            idioma_stats.columns = ['Idioma', 'Capítulos únicos']
            
            # Añadir fila de totales
            total_row = pd.DataFrame({
                'Idioma': ['TOTAL'],
                'Capítulos únicos': [idioma_stats['Capítulos únicos'].sum()]
            })
            idioma_stats = pd.concat([idioma_stats, total_row], ignore_index=True)
            st.dataframe(idioma_stats, hide_index=True)
        else:
            st.warning("El campo 'idiomas_disponibles' no está disponible en los datos")
            
        # Tabla 11: Distribución por formato (CAPÍTULOS ÚNICOS)
        if 'formatos_disponibles' in unique_capitulos.columns:
            st.subheader("📖 Distribución por Formato",
                        help="Formatos disponibles para los libros que contienen los capítulos publicados.")
            formato_stats = unique_capitulos['formatos_disponibles'].value_counts().reset_index()
            formato_stats.columns = ['Formato', 'Capítulos únicos']
            
            # Añadir fila de totales
            total_row = pd.DataFrame({
                'Formato': ['TOTAL'],
                'Capítulos únicos': [formato_stats['Capítulos únicos'].sum()]
            })
            formato_stats = pd.concat([formato_stats, total_row], ignore_index=True)
            st.dataframe(formato_stats, hide_index=True)
        else:
            st.warning("El campo 'formatos_disponibles' no está disponible en los datos")
        
        # ==========================================
        # SECCIÓN: DESCARGAR ARCHIVO COMPLETO
        # ==========================================
        st.header("📥 Descargar Datos Completos")
        
        # Opción para descargar el archivo pro_capitulos_total.csv
        if Path("capitulos_total.csv").exists():
            with open("capitulos_total.csv", "rb") as file:
                btn = st.download_button(
                    label="Descargar archivo pro_capitulos_total.csv completo",
                    data=file,
                    file_name="pro_capitulos_total.csv",
                    mime="text/csv",
                    help="Descarga el archivo CSV completo con todos los datos de capítulos"
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
