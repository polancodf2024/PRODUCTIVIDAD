import streamlit as st
import re
import pandas as pd
import csv
from pathlib import Path
from datetime import datetime
from difflib import get_close_matches

# Configuración simplificada (sin secrets)
CSV_PRODUCTOS_FILE = "productos.csv"  # Archivo local

KEYWORD_CATEGORIES = {
    "Accidente Cerebrovascular": ["accidente cerebrovascular", "acv", "ictus", "stroke"],
    "Alzheimer": ["alzheimer", "demencia", "enfermedad neurodegenerativa"],
    # ... (mantener todas tus categorías originales aquí)
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

        st.subheader("📅 Fecha de publicación")
        default_date = f"{data['year']}-01-01" if data['year'] else ""
        pub_date = st.text_input("Ingrese la fecha de publicación (YYYY-MM-DD):",
                               value=default_date)
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
        df_new = pd.DataFrame([data])
        
        for col in df_new.columns:
            if df_new[col].dtype == object:
                df_new[col] = df_new[col].astype(str).str.replace(r'\r\n|\n|\r', ' ', regex=True).str.strip()

        columns = [
            'economic_number', 'participation_key', 'investigator_name',
            'corresponding_author', 'coauthors', 'article_title', 'year',
            'pub_date', 'volume', 'number', 'pages', 'journal_full',
            'journal_abbrev', 'doi', 'jcr_group', 'pmid', 'selected_keywords'
        ]

        if not Path(filename).exists():
            df_new[columns].to_csv(filename, index=False, encoding='utf-8-sig')
            return True

        df_existing = pd.read_csv(filename, encoding='utf-8-sig', dtype={'economic_number': str})
        df_existing['economic_number'] = df_existing['economic_number'].astype(str).str.strip()
        
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        df_combined[columns].to_csv(filename, index=False, encoding='utf-8-sig')
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
    st.set_page_config(
        page_title="Artículos en PubMed",
        page_icon="📊",
        layout="centered"
    )

    st.title("📊 Artículos en PubMed")
    
    economic_number = st.text_input("🔢 Número económico del investigador:").strip()
    
    if not economic_number or not economic_number.isdigit():
        st.warning("Por favor ingrese un número económico válido (solo dígitos)")
        return

    if Path(CSV_PRODUCTOS_FILE).exists():
        try:
            productos_df = pd.read_csv(CSV_PRODUCTOS_FILE, encoding='utf-8-sig', dtype={'economic_number': str})
            productos_df['economic_number'] = productos_df['economic_number'].astype(str).str.strip()

            if st.checkbox("📂 Mostrar todos los registros del CSV"):
                st.dataframe(productos_df, use_container_width=True)

            filtered_records = productos_df[productos_df['economic_number'] == economic_number]

            if not filtered_records.empty:
                st.subheader(f"📋 Registros existentes para {economic_number}")
                st.dataframe(filtered_records[['article_title', 'journal_full']], hide_index=True)
            
            if st.radio("¿Desea añadir un nuevo registro?", ["No", "Sí"], index=0) == "No":
                return
        except Exception as e:
            st.error(f"Error al leer {CSV_PRODUCTOS_FILE}: {str(e)}")

    st.subheader("📤 Subir artículo científico")
    uploaded_file = st.file_uploader("Seleccione archivo .nbib", type=".nbib")
    if not uploaded_file:
        return

    try:
        content = uploaded_file.read().decode("utf-8")
        data = parse_nbib_file(content)
        if not data:
            return

        st.subheader("📝 Información extraída")
        st.info(data['article_title'])

        selected_categories = st.multiselect(
            "Seleccione 3 palabras clave:",
            options=list(KEYWORD_CATEGORIES.keys()),
            default=extract_keywords(data['article_title'])[:3],
            max_selections=3
        )
        data['selected_keywords'] = selected_categories[:3]

        cols = st.columns(2)
        with cols[0]:
            display_author_info(data, "")
        with cols[1]:
            display_publication_info(data)

        authors_list = []
        if data['corresponding_author']:
            authors_list.append(data['corresponding_author'])
        if data['coauthors']:
            authors_list.extend(data['coauthors'].split("; "))

        investigator_name = st.selectbox("Seleccione su nombre:", authors_list)
        data['investigator_name'] = investigator_name
        data['economic_number'] = economic_number
        data['participation_key'] = "CA" if investigator_name == data['corresponding_author'] else f"{authors_list.index(investigator_name)}C"

        if st.button("💾 Guardar registro", type="primary"):
            if save_to_csv(data):
                st.balloons()
                st.success("✅ Registro guardado exitosamente!")
                st.json({k: v for k, v in data.items() if k != 'selected_keywords'})

    except Exception as e:
        st.error(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
