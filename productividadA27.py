import os
import csv
import ssl
import smtplib
import tempfile
import random
import re


from pathlib import Path

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

import streamlit as st
import requests
from bs4 import BeautifulSoup
import paramiko
import pandas as pd
from difflib import get_close_matches


# Inicializar el estado de mostrar_ocurrencias
if "mostrar_ocurrencias" not in st.session_state:
    st.session_state.mostrar_ocurrencias = False

if "correo" not in st.session_state:
    st.session_state.correo = None


# Configuración de correo
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = "abcdf2024dfabc@gmail.com"
EMAIL_PASSWORD = "hjdd gqaw vvpj hbsy"
REMOTE_HOST = "187.217.52.137"
REMOTE_USER = "POLANCO6"
REMOTE_PASSWORD = "tt6plco6"
REMOTE_DIR = "/home/POLANCO6/BIBLIOGRAFIA"


def procesar_registro(registro):
    """Divide el cuarto campo del registro en subcampos específicos según el formato requerido."""
    campos = registro.split(
        "|")  # Suponemos que los campos están separados por |
    if len(campos) < 6:
        raise ValueError("El registro debe tener al menos 6 campos.")

    cuarto_campo = campos[3]

    # Reemplazar el símbolo ';' por un espacio
    cuarto_campo = cuarto_campo.replace(";", " ")

    # Colocar delimitador '|' justo después del año de cuatro dígitos, incluso si está seguido por un espacio
    cuarto_campo = re.sub(r"(\b\d{4})\s*", r"\1|", cuarto_campo, count=1)

    # Sustituir el primer ':' leído desde el inicio del cuarto campo por '|'
    cuarto_campo = re.sub(r":", r"|", cuarto_campo, count=1)

    # Colocar delimitador justo antes de 'doi'
    cuarto_campo = re.sub(r"\bdoi:\s*", r"|doi: ", cuarto_campo)

    # Reconstruir el cuarto campo asegurando consistencia
    partes = cuarto_campo.split('|')

    if len(partes) < 4:
        raise ValueError(
            "El cuarto campo no cumple con el formato esperado tras división.")

    # Reconstruir el cuarto campo con los delimitadores correctos
    cuarto_campo = f"{partes[0]}|{partes[1]}|{partes[2]}|{partes[3]}"

    # Actualizar el cuarto campo en el registro
    campos[3] = cuarto_campo

    # Reconstruir el registro
    return "|".join(campos)


def procesar_archivo_remoto(numero_economico, remote_host, remote_user, remote_password, remote_dir, remote_port):
    """Lee el archivo de ocurrencias desde el servidor remoto, lo procesa y lo sustituye."""
    archivo_remoto = f"ocurrencias_PubMed_{numero_economico}.txt"
    archivo_local = f"{archivo_remoto}_local"
    archivo_procesado = f"{archivo_remoto}_procesado"

    try:
        # Conexión al servidor remoto
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(remote_host, port=remote_port,
                    username=remote_user, password=remote_password)

        sftp = ssh.open_sftp()
        remote_path = os.path.join(remote_dir, archivo_remoto)

        # Descargar el archivo remoto
        print(
            f"Descargando archivo {archivo_remoto} desde el servidor remoto...")
        sftp.get(remote_path, archivo_local)

        # Procesar el archivo localmente
        print(f"Procesando archivo localmente: {archivo_local}...")
        with open(archivo_local, "r", encoding="utf-8") as infile:
            registros = infile.readlines()

        registros_procesados = []
        for registro in registros:
            registro = registro.strip()
            try:
                procesado = procesar_registro(
                    registro)  # Usa la función integrada
                registros_procesados.append(procesado)
            except ValueError as e:
                print(
                    f"Error al procesar el registro: {registro}. Detalles: {e}")

        # Guardar el archivo procesado
        with open(archivo_procesado, "w", encoding="utf-8") as outfile:
            outfile.write("\n".join(registros_procesados))
        print(f"Archivo procesado guardado como: {archivo_procesado}")

        # Subir el archivo procesado al servidor para reemplazar el original
        print(
            f"Sustituyendo el archivo remoto {archivo_remoto} con el archivo procesado...")
        sftp.put(archivo_procesado, remote_path)

        print(f"El archivo procesado ha sido subido y reemplazado en el servidor remoto.")

        # Cerrar conexiones
        sftp.close()
        ssh.close()

        # Limpiar archivos locales
        os.remove(archivo_local)
        os.remove(archivo_procesado)

    except FileNotFoundError:
        print(f"El archivo {archivo_remoto} no existe en el servidor remoto.")
    except Exception as e:
        print(f"Error durante la operación remota: {e}")


# Función para obtener los datos del archivo remoto
def obtener_datos_personales(numero_economico):
    """Obtiene los datos personales del archivo remoto datos_personales_{numeco}.txt."""
    archivo_remoto = f"datos_personales_{numero_economico}.txt"
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(REMOTE_HOST, port=3792, username=REMOTE_USER,
                    password=REMOTE_PASSWORD)

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
    """Envía un correo con la clave al usuario."""
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
    """Genera una clave numérica aleatoria de 6 dígitos."""
    return "".join([str(random.randint(0, 7)) for _ in range(6)])


def generar_variantes_nombres(nombres, apellidos):
    """Genera variantes del nombre completo y abreviado, incluyendo variantes con guiones."""
    nombre_completo = f"{nombres.strip()}, {apellidos}[Author]"
    nombres_split = nombres.strip().split()
    iniciales = "".join([n[0] for n in nombres_split])
    nombre_abreviado = f"{apellidos.split()[0]} {iniciales}[Author]"

    # Variantes con guiones en apellidos
    apellidos_con_guion = apellidos.replace(" ", "-")
    nombre_completo_guion = f"{nombres.strip()}, {apellidos_con_guion}[Author]"
    nombre_abreviado_guion = f"{apellidos_con_guion.split()[0]} {iniciales}[Author]"

    return [nombre_completo, nombre_abreviado, nombre_completo_guion, nombre_abreviado_guion]


def crear_nombres_txt(nombres, apellidos, correo):
    """Crea el archivo nombres{correo}.txt con el formato adecuado."""
    variantes_nombres = generar_variantes_nombres(nombres, apellidos)
    afiliaciones = ["Cardiología[Affiliation]", "Cardiology[Affiliation]"]

    nombre_archivo = f"nombres{correo}.txt"
    with open(nombre_archivo, "w", encoding="utf-8") as file:
        for nombre in variantes_nombres:
            for afiliacion in afiliaciones:
                file.write(f"({nombre}) AND ({afiliacion})\n")

    return numero_economico


def buscar_en_pubmed(termino):
    """Busca los resultados en PubMed para un término específico con manejo mejorado de tiempos de espera."""
    base_url = "https://pubmed.ncbi.nlm.nih.gov/"
    resultados = []
    encontrados = set()
    page = 1

    # Definir el año de inicio
    anio = 2020
    max_retries = 5  # Número máximo de reintentos
    retry_delay = 5  # Tiempo entre reintentos en segundos

    while True:
        url = f"{base_url}?term={requests.utils.quote(termino)}&filter=dates.{anio}-3000/12/12&page={page}"

        for intento in range(max_retries):
            try:
                # Incrementa el tiempo de espera
                response = requests.get(url, timeout=30)
                if response.status_code != 200:
                    st.warning(
                        f"Error al conectar con PubMed (status code: {response.status_code}).")
                    break

                soup = BeautifulSoup(response.text, "html.parser")
                articulos = soup.find_all("article", class_="full-docsum")

                if not articulos:
                    return resultados

                for articulo in articulos:
                    titulo_elemento = articulo.find("a", class_="docsum-title")
                    titulo = titulo_elemento.text.strip() if titulo_elemento else "Título no disponible"
                    detalle_elemento = articulo.find(
                        "span", class_="docsum-journal-citation full-journal-citation")
                    detalles = detalle_elemento.text.strip(
                    ) if detalle_elemento else "Detalles no disponibles"
                    autores_elemento = articulo.find(
                        "span", class_="docsum-authors full-authors")
                    autores = autores_elemento.text.strip(
                    ) if autores_elemento else "Autores no disponibles"

                    # Dividir detalles en revista y resto
                    if detalles and "." in detalles:
                        partes_detalles = detalles.split(".", 1)
                        revista = partes_detalles[0].strip()
                        resto_detalles = partes_detalles[1].strip() if len(
                            partes_detalles) > 1 else ""
                    else:
                        revista = "Revista no especificada"
                        resto_detalles = detalles

                    identificador = f"{titulo},{revista},{resto_detalles},{autores}"

                    if identificador not in encontrados:
                        encontrados.add(identificador)
                        resultados.append(
                            f"{autores}|{titulo}|{revista}|{resto_detalles}")

                page += 1
                break  # Sal del bucle de reintentos si la conexión fue exitosa

            except requests.exceptions.ReadTimeout:
                if intento < max_retries - 1:
                    st.warning(
                        f"Tiempo de espera agotado. Reintentando ({intento + 1}/{max_retries})...")
                    time.sleep(retry_delay)
                else:
                    st.error(
                        "Tiempo de espera agotado tras varios intentos. Abortando búsqueda en PubMed.")
                    return resultados

            except requests.exceptions.RequestException as e:
                if intento < max_retries - 1:
                    st.warning(
                        f"Error: {e}. Reintentando en {retry_delay} segundos...")
                    time.sleep(retry_delay)
                else:
                    st.error(
                        f"Error persistente: {e}. Abortando búsqueda en PubMed.")
                    return resultados

    return resultados


def determinar_grupo(jif5years):
    """Determina el grupo al que pertenece la revista según su JIF5Years."""
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
        closest_match = get_close_matches(
            nombre_revista.lower(), all_names, n=1, cutoff=0.5)
        if closest_match:
            match = closest_match[0]
            matched_row = df[(df['Name_lower'] == match) | (
                df['Abbr_Name_lower'] == match)].iloc[0]
            jif5years = matched_row['JIF5Years']
            return determinar_grupo(jif5years)
        else:
            return "Grupo no encontrado"


def quitar_duplicados(archivo):
    try:
        # Crear un archivo temporal en el mismo directorio que el archivo original
        directorio = os.path.dirname(archivo)
        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8", dir=directorio) as temp_file:
            with open(archivo, "r", encoding="utf-8") as f:
                lineas_unicas = set(f.readlines())

            # Escribir líneas únicas en el archivo temporal
            temp_file.writelines(sorted(lineas_unicas))

        # Sobrescribir el archivo original con el contenido del archivo temporal
        with open(archivo, "w", encoding="utf-8") as f:
            with open(temp_file.name, "r", encoding="utf-8") as temp_f:
                f.writelines(temp_f.readlines())

        # Eliminar el archivo temporal
        os.remove(temp_file.name)

#        print(f"Duplicados eliminados del archivo: {archivo}")
    except Exception as e:
        print(f"Error: {e}")


def procesar_nombres(correo):
    """Lee y procesa los nombres y afiliaciones en el archivo 'nombres{correo}.txt'."""
    if not correo:
        st.error("El correo no está definido. Por favor, verifica.")
        return False

    file_name = f"nombres{correo}.txt"
    archivo_ocurrencias = f"ocurrencias_PubMed_{correo}.txt"

    if not os.path.exists(file_name):
        st.error(f"El archivo '{file_name}' no existe. Por favor, verifica.")
        return False

    with open(file_name, mode="r", encoding="utf-8") as file:
        lineas = file.readlines()

    resultados = []
    for linea in lineas:
        termino = linea.strip()
        if not termino:
            continue

        resultados.extend(buscar_en_pubmed(termino))

    if resultados:
        with open(archivo_ocurrencias, "w", encoding="utf-8") as file:
            for resultado in sorted(resultados):
                file.write(f"{resultado}\n")

        # Eliminar duplicados del archivo generado
        quitar_duplicados(archivo_ocurrencias)

        st.success(
            f"Se generó el archivo: {archivo_ocurrencias} con {len(resultados)} resultados.")
        return True
    else:
        st.warning("No se encontraron resultados en PubMed.")
        return False


def extraer_concepto_central(nombre_revista):
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
    nombre_lower = nombre_revista.lower()

    # Verificar coincidencias exactas
    if nombre_lower in revistas_conceptos:
        return revistas_conceptos[nombre_lower]

    # Limpiar y preprocesar el nombre
    palabras_comunes = {"journal", "review", "bulletin", "annals",
                        "reports", "international", "american", "european"}
    palabras = [palabra for palabra in nombre_lower.split()
                if palabra not in palabras_comunes]
    nombre_normalizado = " ".join(palabras)

    # Buscar coincidencias parciales
    for concepto, patrones in conceptos.items():
        coincidencias = get_close_matches(
            nombre_normalizado, patrones, n=1, cutoff=0.3)
        if coincidencias:
            return concepto

    # Manejo específico para revistas con prefijo "Bio"
    if nombre_lower.startswith("bio"):
        return "biomedicina"

    # Registrar conceptos no identificados
    with open("conceptos_no_identificados.txt", "a") as file:
        file.write(f"{nombre_revista}\n")
    return "Concepto no identificado"


def mostrar_editor_ocurrencias(numero_economico):
    """
    Muestra y actualiza ocurrencias con conceptos centrales.
    """
    archivo_ocurrencias = f"ocurrencias_PubMed_{numero_economico}.txt"
    file_path = 'CopyofImpactFactor2024.xlsx'

    if not os.path.exists(archivo_ocurrencias):
        st.error(f"El archivo {archivo_ocurrencias} no existe.")
        return

    # Recupera el correo del estado de sesión
    correo = st.session_state.get("correo")
    if not correo:
        st.error("El correo del usuario no está disponible.")
        return

    with open(archivo_ocurrencias, "r", encoding="utf-8") as file:
        ocurrencias = [line.strip() for line in file.readlines()]

    if "estado_ocurrencias" not in st.session_state:
        st.session_state.estado_ocurrencias = {ocurrencia: (
            "", ocurrencia) for ocurrencia in ocurrencias}

    if ocurrencias:
        st.header("Artículos Científicos Generados")

        for i, ocurrencia in enumerate(ocurrencias):
            accion, ocurrencia_editada = st.session_state.estado_ocurrencias.get(
                ocurrencia, ("", ocurrencia))
            col1, col2, col3 = st.columns([8, 1, 1])

            with col1:
                color = "white"
                if accion == "aceptar":
                    color = "#d4edda"  # Verde claro para aceptados
                elif accion == "borrar":
                    color = "#f8d7da"  # Rojo claro para rechazados

                st.markdown(
                    f'<div style="background-color: {color}; padding: 5px; border-radius: 5px;">{ocurrencia_editada}</div>',
                    unsafe_allow_html=True
                )

            with col2:
                if st.button("✔️", key=f"aceptar_{i}_single_click"):
                    st.session_state.estado_ocurrencias[ocurrencia] = (
                        "aceptar", ocurrencia)

            with col3:
                if st.button("❌", key=f"borrar_{i}_single_click"):
                    st.session_state.estado_ocurrencias[ocurrencia] = (
                        "borrar", ocurrencia)

        if st.button("Actualización"):
            ocurrencias_definitivas = []
            for ocurrencia, (accion, editada) in st.session_state.estado_ocurrencias.items():
                if accion == "aceptar":
                    partes = editada.split(".")
                    if len(partes) > 2:
                        titulo_articulo = partes[1].strip()
                        revista_completa = partes[2].strip()

                        import re
                        match = re.search(r"^\D+", revista_completa)
                        nombre_revista = match.group(0).strip(
                        ) if match else revista_completa.strip()

                        grupo = buscar_revista(nombre_revista, file_path)
                        concepto_central = extraer_concepto_central(
                            nombre_revista)

                        ocurrencias_definitivas.append(
                            f"{editada} |{grupo} |{concepto_central}")
                    else:
                        ocurrencias_definitivas.append(
                            f"{editada} |Grupo no encontrado |Concepto no identificado")

            with open(archivo_ocurrencias, "w", encoding="utf-8") as file:
                for ocurrencia in ocurrencias_definitivas:
                    file.write(f"{ocurrencia}\n")

            st.success(
                "El archivo de ocurrencias ha sido actualizado correctamente con los grupos y conceptos centrales.")
            # Ahora el correo se toma del estado de sesión
            subir_archivo_servidor(archivo_ocurrencias, correo)

            remote_host = "187.217.52.137"
            remote_user = "POLANCO6"
            remote_password = "tt6plco6"
            remote_dir = "/home/POLANCO6/BIBLIOGRAFIA"
            remote_port = "3792"

            st.write("Iniciando procesamiento del archivo remoto...")
            procesar_archivo_remoto(numero_economico, remote_host,
                                    remote_user, remote_password, remote_dir, remote_port)
            st.success("Actualización Finalizada.")

    else:
        st.info("No se encontraron artículos en el archivo generado.")


def subir_archivo_servidor(nombre_archivo, correo):
    """Sube el archivo generado al servidor remoto y envía una copia en formato TXT al correo del usuario."""
    REMOTE_HOST = "187.217.52.137"
    REMOTE_USER = "POLANCO6"
    REMOTE_PASSWORD = "tt6plco6"
    REMOTE_DIR = "/home/POLANCO6/BIBLIOGRAFIA"

    try:
        # Subir el archivo al servidor remoto
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(REMOTE_HOST, port=3792, username=REMOTE_USER,
                    password=REMOTE_PASSWORD)

        sftp = ssh.open_sftp()
        remote_path = os.path.join(
            REMOTE_DIR, os.path.basename(nombre_archivo))
        sftp.put(nombre_archivo, remote_path)
        sftp.close()
        ssh.close()

        st.success(
            f"El archivo {nombre_archivo} se subió correctamente al servidor remoto.")

        # Enviar el archivo TXT al correo del usuario
        send_email_with_attachment(
            email_recipient=correo,
            subject="Archivo de Ocurrencias Científicas",
            body="Adjunto hallarás el archivo TXT de artículos científicos extraídos de PubMed.",
            attachment_path=nombre_archivo
        )

        st.success(
            f"El archivo TXT se envió correctamente al correo {correo}.")

    except Exception as e:
        st.error(f"Error: {e}")


# Función para enviar correos con archivo adjunto
def send_email_with_attachment(email_recipient, subject, body, attachment_path):
    mensaje = MIMEMultipart()
    mensaje['From'] = EMAIL_USER
    mensaje['To'] = email_recipient
    mensaje['Subject'] = subject
    mensaje.attach(MIMEText(body, 'plain'))

    # Adjuntar el archivo
    try:
        with open(attachment_path, "rb") as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition',
                            f'attachment; filename={Path(attachment_path).name}')
            mensaje.attach(part)
    except Exception as e:
        print(f"Error al adjuntar el archivo: {e}")

    # Enviar el correo
    context = ssl.create_default_context()
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls(context=context)
        server.login(EMAIL_USER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_USER, email_recipient, mensaje.as_string())


# Interfaz de Streamlit
# Mostrar el logo y título
st.image("escudo_COLOR.jpg", width=150)

st.title("Validación de Usuario por Número Económico")

numero_economico = st.text_input("Ingresa tu número económico:")
clave_ingresada = st.text_input(
    "Ingresa la clave enviada a tu correo:", type="password")

if st.button("Enviar Clave"):
    if numero_economico:
        nombres, apellidos, numeco, correo = obtener_datos_personales(
            numero_economico)
        if correo:
            clave = generar_clave()
            st.session_state.clave_generada = clave  # Guardar clave en la sesión
            st.session_state.nombres = nombres
            st.session_state.apellidos = apellidos
            st.session_state.numero_economico = numeco
            st.session_state.correo = correo
            enviar_clave(correo, clave)
    else:
        st.error("Por favor, ingresa tu número económico.")

if st.button("Validar Clave", key="validar_clave"):
    if "clave_generada" in st.session_state and clave_ingresada == st.session_state.clave_generada:
        st.success("Clave válida. Acceso concedido.")

        nombres = st.session_state.nombres
        apellidos = st.session_state.apellidos
        numero_economico = st.session_state.numero_economico
        correo = st.session_state.correo  # Simplificado

        st.write(f"Nombre: {nombres}")
        st.write(f"Apellidos: {apellidos}")
        st.write(f"Número Económico: {numero_economico}")
        st.write(f"Correo Electrónico: {correo}")

        with st.spinner("Procesando, por favor espera..."):
            if nombres and apellidos and numero_economico and correo:
                numero_economico = crear_nombres_txt(
                    nombres, apellidos, numero_economico)
                if procesar_nombres(numero_economico):
                    st.session_state.mostrar_ocurrencias = True
    else:
        st.error("Clave incorrecta. Acceso denegado.")


# Mostrar editor de ocurrencias si el estado está activado
if st.session_state.mostrar_ocurrencias:
    mostrar_editor_ocurrencias(st.session_state.numero_economico)


if st.button("Cerrar Sesión."):
    st.write("Gracias por usar la aplicación. Hasta luego.")
