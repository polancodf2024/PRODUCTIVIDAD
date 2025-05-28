import streamlit as st
import re
import pandas as pd
import csv
from pathlib import Path
from datetime import datetime
from difflib import get_close_matches
import os

# Configuración de variables desde secrets.toml
SMTP_SERVER = st.secrets["smtp_server"]
SMTP_PORT = st.secrets["smtp_port"]
EMAIL_USER = st.secrets["email_user"]
EMAIL_PASSWORD = st.secrets["email_password"]
NOTIFICATION_EMAIL = st.secrets["notification_email"]
CSV_PRODUCTOS_FILE = st.secrets.get("remote_productos")
CSV_MANUAL_FILE = st.secrets.get("remote_manual")
CSV_TESIS_FILE = st.secrets.get("remote_tesis")
CSV_LIBROS_FILE = st.secrets.get("remote_libros")
CSV_CAPITULOS_FILE = st.secrets.get("remote_capitulos")
REMOTE_HOST = st.secrets["remote_host"]
REMOTE_USER = st.secrets["remote_user"]
REMOTE_PASSWORD = st.secrets["remote_password"]
REMOTE_PORT = st.secrets["remote_port"]
REMOTE_DIR = st.secrets["remote_dir"]

os.environ["TF_ENABLE_ONEDNN_OPTS"] = "0"

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
        "estenosis mitral", "insuficiencia mitral", "prolapso mitral", 
        "tavi", "taavi", "anillo mitral", "reemplazo valvular"
    ],
}

def extract_keywords(title):
    if not title:
        return []
    found_keywords = set()
    title_lower = title.lower()
    for category, keywords in KEYWORD_CATEGORIES.items():
        for keyword in keywords:
            if keyword in title_lower:
                found_keywords.add(category)
                break
    return sorted(found_keywords)

def determinar_grupo(jif5years):
    if pd.isna(jif5years):
        return "Grupo 1 (sin factor de impacto)"
    try:
        jif = float(jif5years)
        if jif <= 0.9:
            return "Grupo 2 (FI ≤ 0.9)"
        elif jif <= 2.99:
            return "Grupo 3 (FI 1-2.99)"
        elif jif <= 5.99:
            return "Grupo 4 (FI 3-5.99)"
        elif jif <= 8.99:
            return "Grupo 5 (FI 6-8.99)"
        elif jif <= 11.99:
            return "Grupo 6 (FI 9-11.99)"
        else:
            return "Grupo 7 (FI ≥ 12)"
    except ValueError:
        return "Grupo 1 (sin factor de impacto)"

def buscar_grupo_revista(nombre_revista, file_path='CopyofImpactFactor2024.xlsx'):
    try:
        df = pd.read_excel(file_path, sheet_name='2024最新完整版IF')
        df['Name_lower'] = df['Name'].str.lower()
        df['Abbr_Name_lower'] = df['Abbr Name'].str.lower()
        df['JIF5Years'] = pd.to_numeric(df['JIF5Years'], errors='coerce')

        exact_match = df[(df['Name_lower'] == nombre_revista.lower()) |
                         (df['Abbr_Name_lower'] == nombre_revista.lower())]
        if not exact_match.empty:
            return determinar_grupo(exact_match.iloc[0]['JIF5Years'])

        closest_match = get_close_matches(
            nombre_revista.lower(),
            df['Name_lower'].tolist() + df['Abbr_Name_lower'].tolist(),
            n=1, cutoff=0.6
        )
        if closest_match:
            match_row = df[(df['Name_lower'] == closest_match[0]) |
                          (df['Abbr_Name_lower'] == closest_match[0])].iloc[0]
            return determinar_grupo(match_row['JIF5Years'])

        return "Grupo no determinado"
    except Exception as e:
        st.warning(f"Error al buscar grupo: {str(e)}")
        return "Grupo no determinado"

def parse_nbib_file(content: str) -> dict:
    data = {
        'corresponding_author': '',
        'coauthors': '',
        'article_title': '',
        'year': '',
        'pub_date': '',
        'volume': '',
        'number': '',
        'pages': '',
        'journal_full': '',
        'journal_abbrev': '',
        'doi': '',
        'jcr_group': '',
        'pmid': '',
        'investigator_name': '',
        'economic_number': '',
        'participation_key': '',
        'selected_keywords': []
    }

    def extract_field(pattern, multi_line=False):
        nonlocal content
        flags = re.DOTALL if multi_line else 0
        match = re.search(pattern, content, flags)
        return match.group(1).strip() if match else ''

    try:
        data['pmid'] = extract_field(r'PMID-\s+(\d+)')
        authors = re.findall(r'FAU\s+-\s+(.*?)\n', content)
        if authors:
            data['corresponding_author'] = authors[0].strip()
            data['coauthors'] = "; ".join(a.strip() for a in authors[1:])
        data['article_title'] = extract_field(r'TI\s+-\s+(.*?)(?:\n[A-Z]{2}\s+-|$)', True)

        pub_date_match = re.search(r'DP\s+-\s+(\d{4}\s+[A-Za-z]{3}\s+\d{1,2})', content)
        if pub_date_match:
            try:
                date_obj = datetime.strptime(pub_date_match.group(1), '%Y %b %d')
                data['pub_date'] = date_obj.strftime('%Y-%m-%d')
                data['year'] = date_obj.strftime('%Y')
            except:
                data['pub_date'] = pub_date_match.group(1)
                data['year'] = pub_date_match.group(1).split()[0]
        else:
            data['year'] = extract_field(r'DP\s+-\s+(\d{4})')
            data['pub_date'] = data['year']

        # Solicitar al usuario que ingrese la fecha completa en formato YYYY-MM-DD
        st.subheader("📅 Fecha de publicación")
        default_date = f"{data['year']}-01-01" if data['year'] else ""
        pub_date = st.text_input("Ingrese la fecha de publicación (YYYY-MM-DD):",
                               value=default_date,
                               help="Formato: Año-Mes-Día (ej. 2023-05-15)")

        # Validar el formato de fecha
        try:
            datetime.strptime(pub_date, '%Y-%m-%d')
            data['pub_date'] = pub_date
        except ValueError:
            st.error("Formato de fecha inválido. Por favor use YYYY-MM-DD")
            return None

        data['volume'] = extract_field(r'VI\s+-\s+(\S+)')
        data['number'] = extract_field(r'IP\s+-\s+(\S+)')
        data['pages'] = extract_field(r'PG\s+-\s+(\S+)')
        data['journal_full'] = extract_field(r'JT\s+-\s+(.*?)\n')
        data['journal_abbrev'] = extract_field(r'TA\s+-\s+(.*?)\n')
        if data['journal_full'] or data['journal_abbrev']:
            data['jcr_group'] = buscar_grupo_revista(data['journal_full'] or data['journal_abbrev'])

        doi_match = re.search(r'DO\s+-\s+(.*?)\n', content) or \
                   re.search(r'(?:LID|AID)\s+-\s+(.*?doi\.org/.*?)\s', content) or \
                   re.search(r'(?:LID|AID)\s+-\s+(10\.\S+)', content)
        if doi_match:
            data['doi'] = doi_match.group(1).strip()

    except Exception as e:
        st.error(f"Error al procesar archivo .nbib: {str(e)}")

    return data

def save_to_csv(data: dict, filename=CSV_PRODUCTOS_FILE):
    try:
        # Crear DataFrame con los datos nuevos
        df_new = pd.DataFrame([data])
        
        # Limpiar los datos (eliminar saltos de línea y espacios extra)
        for col in df_new.columns:
            if df_new[col].dtype == object:
                df_new[col] = df_new[col].astype(str).str.replace(r'\r\n|\n|\r', ' ', regex=True).str.replace(r'\s+', ' ', regex=True).str.strip()

        # Definir las columnas esperadas en el orden deseado
        columns = [
            'economic_number', 'participation_key', 'investigator_name',
            'corresponding_author', 'coauthors', 'article_title', 'year',
            'pub_date', 'volume', 'number', 'pages', 'journal_full',
            'journal_abbrev', 'doi', 'jcr_group', 'pmid', 'selected_keywords'
        ]

        # Si el archivo no existe, crear uno nuevo con las columnas en el orden correcto
        if not Path(filename).exists():
            df_new[columns].to_csv(filename, index=False, encoding='utf-8-sig', 
                                 lineterminator='\n', quoting=csv.QUOTE_ALL)
            return True

        # Si el archivo existe, leerlo y concatenar los datos nuevos
        df_existing = pd.read_csv(filename, encoding='utf-8-sig', dtype={'economic_number': str})
        df_existing['economic_number'] = df_existing['economic_number'].astype(str).str.strip()
        
        # Combinar los DataFrames
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        
        # Guardar el DataFrame combinado
        df_combined[columns].to_csv(filename, index=False, encoding='utf-8-sig', 
                                  lineterminator='\n', quoting=csv.QUOTE_ALL)
        return True
    except Exception as e:
        st.error(f"Error al guardar en CSV: {str(e)}")
        return False

def highlight_author(author: str, investigator_name: str) -> str:
    if investigator_name and investigator_name.lower() == author.lower():
        return f"<span style='background-color: #90EE90;'>{author}</span>"
    return author

def display_author_info(data, investigator_name):
    st.markdown("**Autores**")
    st.markdown(f"📌 Correspondencia: {highlight_author(data['corresponding_author'], investigator_name)}", unsafe_allow_html=True)
    if data['coauthors']:
        st.markdown("👥 Coautores:")
        for author in data['coauthors'].split("; "):
            st.markdown(f"- {highlight_author(author, investigator_name)}", unsafe_allow_html=True)

def display_publication_info(data):
    st.markdown("**Detalles de publicación**")
    st.write(f"📅 Año: {data['year']}")
    st.write(f"**📅 Fecha de publicación:**  \n`{data['pub_date']}`")
    st.write(f"📚 Vol/Núm: {data['volume']}/{data['number']}")
    st.write(f"🔖 Páginas: {data['pages']}")
    st.write(f"🌐 DOI: {data['doi'] or 'No disponible'}")

def main():
    # Configuración de la página con logo y título
    st.set_page_config(
        page_title="Artículos en PubMed",
        page_icon="📊",
        layout="centered"
    )

    # Logo y título
    col1, col2 = st.columns([1, 3])
    with col1:
        st.image("escudo_COLOR.jpg", width=80)
    with col2:
        st.title("📊 Artículos en PubMed")
    
    economic_input = st.text_input("🔢 Número económico del investigador:")
    economic_number = str(economic_input).strip()
    
    if not economic_number or not economic_number.isdigit():
        st.warning("Por favor ingrese un número económico válido (solo dígitos)")
        return

    st.markdown(f"**🧾 Número económico ingresado:** `{economic_number}`")

    # Verificar si el archivo existe, pero no mostrar error si no existe
    if Path(CSV_PRODUCTOS_FILE).exists():
        try:
            productos_df = pd.read_csv(CSV_PRODUCTOS_FILE, encoding='utf-8-sig', dtype={'economic_number': str})
            productos_df['economic_number'] = productos_df['economic_number'].astype(str).str.strip()

            if st.checkbox("📂 Mostrar todos los registros del CSV"):
                st.dataframe(productos_df, use_container_width=True)

            filtered_records = productos_df[productos_df['economic_number'].astype(str).str.strip() == economic_number]

            if not filtered_records.empty:
                st.subheader(f"📋 Registros existentes para el número económico {economic_number}")
                cols_to_show = ['economic_number', 'participation_key', 'investigator_name', 'article_title', 'journal_full']
                st.dataframe(filtered_records[cols_to_show], hide_index=True, use_container_width=True)
            else:
                st.info(f"No se encontraron registros para el número económico {economic_number}")
            
            if st.radio("¿Desea añadir un nuevo registro?", ["No", "Sí"], index=0) == "No":
                st.success("Proceso finalizado. Puede cerrar la aplicación.")
                return

        except Exception as e:
            st.error(f"Error al leer {CSV_PRODUCTOS_FILE}: {str(e)}")
            return
    else:
        st.info(f"No se encontró un archivo {CSV_PRODUCTOS_FILE} existente. Se creará uno nuevo al guardar el primer registro.")

    st.subheader("📤 Subir artículo científico")
    st.markdown("""
ℹ️ **Nota:**  
1. **Busque en otra ventana el artículo** en [PubMed](https://pubmed.ncbi.nlm.nih.gov/)               
2. **Localice el botón 'Cite'** en la página del artículo  
3. **Haga clic en 'Download .nbib'**  
4. **Suba el archivo descargado** en el selector a continuación  
    """)
    uploaded_file = st.file_uploader("Seleccione archivo .nbib", type=".nbib")
    if not uploaded_file:
        return
    try:
        content = uploaded_file.read().decode("utf-8")
    except Exception as e:
        st.error(f"Error al leer archivo: {str(e)}")
        return

    with st.expander("🔍 Ver contenido original"):
        st.code(content)

    data = parse_nbib_file(content)

    st.subheader("📝 Información extraída")
    st.markdown("### 📄 Título del artículo")
    st.info(data['article_title'])

    st.subheader("🔑 Selección de palabras clave (Elija 3)")
    suggested_keywords = extract_keywords(data['article_title'])
    all_categories = list(KEYWORD_CATEGORIES.keys())
    selected_categories = st.multiselect(
        "Seleccione categorías relevantes:",
        options=all_categories,
        default=suggested_keywords[:3],
        max_selections=3
    )
    if len(selected_categories) < 3:
        st.warning(f"Por favor seleccione al menos 3 palabras clave (seleccionadas: {len(selected_categories)})")

    data['selected_keywords'] = selected_categories[:3]

    if data['selected_keywords']:
        st.markdown("**Palabras clave seleccionadas:**")
        st.info(", ".join(data['selected_keywords']))

    cols = st.columns(2)
    with cols[0]:
        display_author_info(data, "")
    with cols[1]:
        display_publication_info(data)

    st.markdown("**Revista**")
    st.write(f"🏛️ Nombre: {data['journal_full']}")
    st.write(f"🏷️ Abreviatura: {data['journal_abbrev']}")
    st.write(f"🏆 Grupo JCR: {data['jcr_group']}")

    st.subheader("👤 Verificación de autoría")
    authors_list = []
    if data['corresponding_author']:
        authors_list.append(data['corresponding_author'])
    if data['coauthors']:
        authors_list.extend(data['coauthors'].split("; "))

    if not authors_list:
        st.error("No se encontraron autores en el artículo")
        return

    investigator_name = st.selectbox("Seleccione su nombre como aparece en la publicación:", authors_list)
    st.markdown(f"**Selección actual:** {highlight_author(investigator_name, investigator_name)}", unsafe_allow_html=True)

    data['investigator_name'] = investigator_name
    data['economic_number'] = economic_number
    data['participation_key'] = "CA" if investigator_name == data['corresponding_author'] else f"{authors_list.index(investigator_name)}C"

    st.markdown("---")
    st.markdown("**Resumen de identificación**")
    cols = st.columns(2)
    with cols[0]:
        st.markdown(f"**🔢 Número económico:**  \n`{economic_number}`")
    with cols[1]:
        st.markdown(f"**🔑 Clave participación:**  \n`{data['participation_key']}`")
    st.markdown(f"**👤 Nombre investigador:**  \n`{investigator_name}`")
    st.markdown("---")

    if st.button("💾 Guardar registro", type="primary"):
        if save_to_csv(data):
            st.balloons()
            st.success("✅ Registro guardado exitosamente!")
            st.subheader("📄 Registro completo capturado")
            
            cols = st.columns(2)
            with cols[0]:
                st.markdown(f"**🔢 Número económico:**  \n`{data['economic_number']}`")
                st.markdown(f"**👤 Investigador:**  \n`{data['investigator_name']}`")
            with cols[1]:
                st.markdown(f"**🔑 Clave participación:**  \n`{data['participation_key']}`")
                st.markdown(f"**📅 Año publicación:**  \n`{data['year']}`")

            
            st.markdown("**📄 Datos completos:**")
            st.json(data)

if __name__ == "__main__":
    main()
