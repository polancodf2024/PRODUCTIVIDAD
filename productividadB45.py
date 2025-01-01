import os
import paramiko
import random
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import streamlit as st
import pandas as pd
from difflib import get_close_matches

# Inicializar el estado de la sesión
if "clave_generada" not in st.session_state:
    st.session_state.clave_generada = None

if "correo" not in st.session_state:
    st.session_state.correo = None

if "datos_articulos" not in st.session_state:
    st.session_state.datos_articulos = []

if "validado" not in st.session_state:
    st.session_state.validado = False

# Configuración del servidor remoto y correo
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = "abcdf2024dfabc@gmail.com"
EMAIL_PASSWORD = "hjdd gqaw vvpj hbsy"
REMOTE_HOST = "187.217.52.137"
REMOTE_USER = "POLANCO6"
REMOTE_PASSWORD = "tt6plco6"
REMOTE_DIR = "/home/POLANCO6/BIBLIOGRAFIA"

# Función para obtener los datos personales desde el servidor remoto
def obtener_datos_personales(numero_economico):
    archivo_remoto = f"datos_personales_{numero_economico}.txt"
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(REMOTE_HOST, port=3792, username=REMOTE_USER, password=REMOTE_PASSWORD)

        sftp = ssh.open_sftp()
        remote_path = os.path.join(REMOTE_DIR, archivo_remoto)

        try:
            with sftp.open(remote_path, "r") as file:
                contenido = file.read().decode("utf-8").strip().split(":")
                nombres = contenido[0]
                apellidos = contenido[1]
                numero_economico = contenido[2]
                correo = contenido[-1]
                return nombres, apellidos, numero_economico, correo
        except FileNotFoundError:
            st.error("El número económico no tiene un archivo asociado.")
            return None, None, None, None
        finally:
            sftp.close()
            ssh.close()

    except Exception as e:
        st.error(f"Error al obtener los datos personales: {e}")
        return None, None, None, None

# Función para enviar la clave al correo del usuario
def enviar_clave(correo, clave):
    subject = "Clave de acceso"
    body = f"Hola,\n\nTu clave de acceso es: {clave}\n\nPor favor, ingrésala para continuar."

    mensaje = MIMEMultipart()
    mensaje['From'] = EMAIL_USER
    mensaje['To'] = correo
    mensaje['Subject'] = subject
    mensaje.attach(MIMEText(body, 'plain'))

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls(context=context)
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_USER, correo, mensaje.as_string())
        st.success(f"Se ha enviado una clave de acceso al correo {correo}.")
    except Exception as e:
        st.error(f"Error al enviar la clave: {e}")

# Función para generar una clave de 6 dígitos
def generar_clave():
    return "".join([str(random.randint(0, 9)) for _ in range(6)])

# Función para determinar el grupo basado en JIF5Years
def determinar_grupo(jif5years):
    if pd.isna(jif5years):
        return "Grupo 1"
    elif jif5years <= 0.9:
        return "Grupo 2"
    elif 1 <= jif5years <= 2.99:
        return "Grupo 3"
    elif 3 <= jif5years <= 5.99:
        return "Grupo 4"
    elif 6 <= jif5years <= 8.99:
        return "Grupo 5"
    elif 9 <= jif5years <= 11.99:
        return "Grupo 6"
    else:
        return "Grupo 7"

def buscar_revista(nombre_revista, file_path):
    """Busca la revista y determina su grupo basado en su JIF5Years."""
    sheet_name = '2024最新完整版IF'
    df = pd.read_excel(file_path, sheet_name=sheet_name)

    df['Name_lower'] = df['Name'].str.lower()
    df['Abbr_Name_lower'] = df['Abbr Name'].str.lower()
    df['JIF5Years'] = pd.to_numeric(df['JIF5Years'], errors='coerce')

    exact_match = df[(df['Name_lower'] == nombre_revista.lower()) |
                     (df['Abbr_Name_lower'] == nombre_revista.lower())]

    if not exact_match.empty:
        jif5years = exact_match.iloc[0]['JIF5Years']
        return determinar_grupo(jif5years)
    else:
        all_names = df['Name_lower'].tolist() + df['Abbr_Name_lower'].tolist()
        closest_match = get_close_matches(nombre_revista.lower(), all_names, n=1, cutoff=0.5)
        if closest_match:
            match = closest_match[0]
            matched_row = df[(df['Name_lower'] == match) | (df['Abbr_Name_lower'] == match)].iloc[0]
            jif5years = matched_row['JIF5Years']
            return determinar_grupo(jif5years)
        else:
            return "Grupo no encontrado"

def extraer_concepto_central(revista):
    """
    Determina el concepto central de una revista usando reglas simples y similitud textual.
    Devuelve el concepto en español o 'Concepto no identificado' si no encuentra coincidencias.
    """
    # Lista ampliada de conceptos y palabras clave
    conceptos = {
        "cardiología": ["cardiology", "heart", "vascular", "cardiac", "myocardial", "circulation"],
        "genética cardiovascular": ["genetics", "genomics", "hereditary", "dna", "mutation", "inheritance"],
        "medicina vascular": ["vascular medicine", "vascular", "angiology", "endothelium", "artery", "vein"],
        "hipertensión": ["hypertension", "blood pressure", "arterial pressure", "systolic", "diastolic"],
        "insuficiencia cardíaca": ["heart failure", "cardiac failure", "congestive heart failure", "chf"],
        "medicina general": ["general medicine", "medicine", "nature medicine", "clinical medicine", "practice"],
        "investigación médica": ["research", "medical research", "clinical research", "scientific investigation", "studies"],
        "cirugía cardiovascular": ["cardiovascular surgery", "cardiac surgery", "surgical", "bypass", "angioplasty"],
        "imágenes médicas": ["medical imaging", "radiology", "diagnostic imaging", "nuclear medicine", "mri", "ct"],
        "electrofisiología": ["electrophysiology", "heart rhythm", "arrhythmia", "atrial fibrillation"],
        "prevención cardiovascular": ["prevention", "preventive cardiology", "risk reduction", "health"],
        "cardiopatías congénitas": ["congenital heart", "congenital defects", "pediatric cardiology"],
        "epidemiología cardiovascular": ["epidemiology", "population health", "statistics", "trials", "cohort"],
        "farmacología cardiovascular": ["pharmacology", "drugs", "medications", "pharma", "therapeutics"],
        "metabolismo": ["metabolism", "lipids", "cholesterol", "triglycerides", "diabetes", "obesity"],
        "rehabilitación cardiovascular": ["rehabilitation", "cardiac rehab", "exercise", "recovery"],
        "biomedicina": ["biomedicine", "biomedical", "bioscience", "bioinformatics", "biotechnology", "biomarkers"],
    }

    # Lista específica para revistas conocidas, incluidas las de prefijo "Bio"
    revistas_conceptos = {
        "nature medicine": "medicina general",
        "journal of the american college of cardiology": "cardiología",
        "european heart journal": "cardiología",
        "circulation": "cardiología",
        "hypertension": "hipertensión",
        "journal of clinical hypertension": "hipertensión",
        "american heart journal": "cardiología",
        "international journal of cardiology": "cardiología",
        "clinical cardiology": "cardiología",
        "vascular health and risk management": "medicina vascular",
        "heart rhythm": "electrofisiología",
        "diabetes care": "metabolismo",
        "journal of lipid research": "metabolismo",
        "bioinformatics": "biomedicina",
        "biomarkers": "biomedicina",
        "biotechnology advances": "biomedicina",
        "biological psychiatry": "biomedicina",
        "biochimica et biophysica acta": "biomedicina",
        "biomedicine & pharmacotherapy": "biomedicina",
        "biosensors and bioelectronics": "biomedicina",
    }

    # Normalizar el nombre
    nombre_lower = revista.lower()

    # Verificar coincidencias exactas
    if nombre_lower in revistas_conceptos:
        return revistas_conceptos[nombre_lower]

    # Limpiar y preprocesar el nombre
    palabras_comunes = {"journal", "review", "bulletin", "annals", "reports", "international", "american", "european"}
    palabras = [palabra for palabra in nombre_lower.split() if palabra not in palabras_comunes]
    nombre_normalizado = " ".join(palabras)

    # Buscar coincidencias parciales
    for concepto, patrones in conceptos.items():
        coincidencias = get_close_matches(nombre_normalizado, patrones, n=1, cutoff=0.3)
        if coincidencias:
            return concepto

    # Manejo específico para revistas con prefijo "Bio"
    if nombre_lower.startswith("bio"):
        return "biomedicina"

    # Registrar conceptos no identificados
    with open("conceptos_no_identificados.txt", "a") as file:
        file.write(f"{nombre_revista}\n")
    return "Concepto no identificado"


# Función para guardar datos en un archivo
def guardar_datos(numero_economico, datos):
    archivo_ocurrencias = f"ocurrencias_Nuevos_{numero_economico}.txt"
    with open(archivo_ocurrencias, "w", encoding="utf-8") as file:
        for dato in datos:
            file.write(f"{dato}\n")
    return archivo_ocurrencias

# Función para subir archivo al servidor remoto
def subir_archivo_servidor(nombre_archivo):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(REMOTE_HOST, port=3792, username=REMOTE_USER, password=REMOTE_PASSWORD)

        sftp = ssh.open_sftp()
        remote_path = os.path.join(REMOTE_DIR, os.path.basename(nombre_archivo))
        sftp.put(nombre_archivo, remote_path)
        sftp.close()
        ssh.close()

        st.success(f"El archivo {nombre_archivo} se subió correctamente al servidor remoto.")
    except Exception as e:
        st.error(f"Error al subir el archivo al servidor remoto: {e}")

# Función para enviar archivo al usuario
def enviar_archivo_usuario(correo, archivo):
    subject = "Registro de Artículos Científicos"
    body = "Hola,\n\nAdjuntamos el archivo con los registros de artículos científicos que has proporcionado.\n\nSaludos."

    mensaje = MIMEMultipart()
    mensaje['From'] = EMAIL_USER
    mensaje['To'] = correo
    mensaje['Subject'] = subject
    mensaje.attach(MIMEText(body, 'plain'))

    try:
        with open(archivo, "rb") as adjunto:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(adjunto.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(archivo)}')
            mensaje.attach(part)

        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls(context=context)
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_USER, correo, mensaje.as_string())
        st.success(f"El archivo se envió correctamente al correo {correo}.")
    except Exception as e:
        st.error(f"Error al enviar el archivo: {e}")

# Interfaz principal
# Mostrar el logo y título
st.image("escudo_COLOR.jpg", width=150)
st.title("Validación de Usuario por Número Económico")

if not st.session_state.validado:
    numero_economico = st.text_input("Ingresa tu número económico:")

    if st.button("Enviar Clave"):
        if numero_economico:
            nombres, apellidos, numeco, correo = obtener_datos_personales(numero_economico)
            if correo:
                clave = generar_clave()
                st.session_state.clave_generada = clave
                st.session_state.nombres = nombres
                st.session_state.apellidos = apellidos
                st.session_state.numero_economico = numeco
                st.session_state.correo = correo
                enviar_clave(correo, clave)
            else:
                st.error("No se encontró un correo asociado al número económico proporcionado.")
        else:
            st.error("Por favor, ingresa tu número económico.")

    clave_ingresada = st.text_input("Ingresa la clave enviada a tu correo:", type="password")

    if st.button("Validar Clave", key="validar_clave"):
        if "clave_generada" in st.session_state and clave_ingresada == st.session_state.clave_generada:
            st.success("Clave válida. Acceso concedido.")
            st.session_state.validado = True
        else:
            st.error("Clave incorrecta. Acceso denegado.")

if st.session_state.validado:
    nombres = st.session_state.nombres
    apellidos = st.session_state.apellidos
    numero_economico = st.session_state.numero_economico
    correo = st.session_state.correo

    st.write(f"Nombre: {nombres}")
    st.write(f"Apellidos: {apellidos}")
    st.write(f"Número Económico: {numero_economico}")
    st.write(f"Correo Electrónico: {correo}")

    st.header("Captura de Datos del Artículo")

    autores = st.text_input("Lista de Coautores:", value="" if "Autores" not in st.session_state else st.session_state.autores, key="autores")
    titulo = st.text_input("Título del Artículo:", value="" if "titulo" not in st.session_state else st.session_state.titulo, key="titulo")
    anio = st.text_input("Año de Publicación:", value="" if "anio" not in st.session_state else st.session_state.anio, key="anio")
    revista = st.text_input("Nombre de la Revista:", value="" if "revista" not in st.session_state else st.session_state.revista, key="revista")
    numerovolumen = st.text_input("Número y Volumen:", value="" if "numerovolumen" not in st.session_state else st.session_state.numerovolumen, key="numerovolumen")
    paginas = st.text_input("Páginas:", value="" if "paginas" not in st.session_state else st.session_state.paginas, key="paginas")
    doi = st.text_input("doi:", value="" if "doi" not in st.session_state else st.session_state.doi, key="doi")

    if st.button("Guardar Datos del Artículo"):
        if all([autores, titulo, anio, revista, numerovolumen, paginas, doi]):
            grupo = buscar_revista(revista, "CopyofImpactFactor2024.xlsx")
            concepto_central = extraer_concepto_central(revista)
            nuevo_registro = f"{autores} |{titulo} |{anio} |{revista} |{numerovolumen} |{paginas} |doi: {doi} |{grupo} |{concepto_central}"
            st.session_state.datos_articulos.append(nuevo_registro)
            archivo = guardar_datos(numero_economico, st.session_state.datos_articulos)
            st.success(f"Datos guardados en {archivo}.")
            enviar_archivo_usuario(correo, archivo)
        else:
            st.error("Por favor, completa todos los campos antes de guardar.")


    if st.button("Subir Archivo al Servidor"):
        archivo = guardar_datos(numero_economico, st.session_state.datos_articulos)
        subir_archivo_servidor(archivo)


    if st.button("Cerrar Sesión."):
        st.write("Gracias por usar la aplicación. Hasta luego.")

