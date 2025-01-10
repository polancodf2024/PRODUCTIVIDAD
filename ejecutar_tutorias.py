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

def descargar_archivo_remoto(sftp, remote_path):
    try:
        local_temp_path = f"temp_{os.path.basename(remote_path)}"
        sftp.get(remote_path, local_temp_path)
        return local_temp_path
    except FileNotFoundError:
        return None  # El archivo no existe


def actualizar_archivo_remoto(numero_economico, nuevos_registros):
    """
    Actualiza el archivo remoto combinando los registros existentes y los nuevos.
    Si el archivo remoto no existe, se crea uno nuevo.
    """
    archivo_remoto = f"ocurrencias_Tutorias_{numero_economico}.txt"

    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(REMOTE_HOST, port=3792, username=REMOTE_USER, password=REMOTE_PASSWORD)

        sftp = ssh.open_sftp()
        remote_path = os.path.join(REMOTE_DIR, archivo_remoto)

        # Descargar el archivo remoto existente
        archivo_local_temp = descargar_archivo_remoto(sftp, remote_path)

        registros_existentes = []

        # Si el archivo no existe, crear un archivo temporal nuevo
        if not archivo_local_temp:
            archivo_local_temp = f"temp_{archivo_remoto}"
            with open(archivo_local_temp, "w", encoding="utf-8") as f:
                pass  # Crear un archivo vacío

        else:
            # Leer los registros existentes del archivo descargado
            with open(archivo_local_temp, "r", encoding="utf-8") as f:
                registros_existentes = f.readlines()

        # Fusionar registros existentes con los nuevos registros
        registros_totales = set(registros_existentes + nuevos_registros)

        # Guardar los registros combinados en el archivo temporal local
        with open(archivo_local_temp, "w", encoding="utf-8") as f:
            f.writelines(registros_totales)

        # Subir el archivo actualizado al servidor remoto
        sftp.put(archivo_local_temp, remote_path)

        st.success(f"El archivo remoto {archivo_remoto} fue actualizado correctamente.")

        # Limpiar archivo temporal
        os.remove(archivo_local_temp)

        sftp.close()
        ssh.close()

    except Exception as e:
        st.error(f"Error al actualizar el archivo remoto: {e}")


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



# Función para guardar datos en un archivo
def guardar_datos(numero_economico, datos):
    """
    Añade los registros al archivo local de ocurrencias con un nombre basado en el número económico.
    """
    archivo_ocurrencias = f"temporal_Tutorias_{numero_economico}.txt"
    try:
        # Abrir el archivo en modo 'a' para agregar nuevos registros
        with open(archivo_ocurrencias, "a", encoding="utf-8") as file:
            for dato in datos:
                file.write(f"{dato}\n")
        return archivo_ocurrencias
    except Exception as e:
        st.error(f"Error al guardar datos localmente: {e}")
        return None



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
    subject = "Registro de Tutorías"
    body = "Hola,\n\nAdjuntamos el archivo con los registros de tutorías que has proporcionado.\n\nSaludos."

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


def ejecutar_tutorias():
    st.title("Registro de Tutorías")

    if not st.session_state.validado:
        # Solicitar número económico
        numero_economico = st.text_input("Ingresa tu número económico:")

        # Botón para enviar la clave
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

        # Solicitar la clave ingresada por el usuario
        clave_ingresada = st.text_input("Ingresa la clave enviada a tu correo:", type="password")

        # Botón para validar la clave
        if st.button("Validar Clave", key="validar_clave"):
            if "clave_generada" in st.session_state and clave_ingresada == st.session_state.clave_generada:
                st.success("Clave válida. Acceso concedido.")
                st.session_state.validado = True
            else:
                st.error("Clave incorrecta. Acceso denegado.")

    if st.session_state.validado:
        # Mostrar información del usuario validado
        nombres = st.session_state.nombres
        apellidos = st.session_state.apellidos
        numero_economico = st.session_state.numero_economico
        correo = st.session_state.correo

        st.write(f"Nombre: {nombres}")
        st.write(f"Apellidos: {apellidos}")
        st.write(f"Número Económico: {numero_economico}")
        st.write(f"Correo Electrónico: {correo}")

        st.header("Captura de Datos de la Tutoría")

        # Campos para ingresar datos de la tutoría
        autores = st.text_input("Tutor principal:", value="", key="autores")
        tesista = st.text_input("Tesista:", value="", key="tesista")
        titulo = st.text_input("Título:", value="", key="titulo")
        grado = st.selectbox(
            "Grado académico:",
            ["Especialidad", "Licenciatura", "Maestría", "Doctorado"],
            key="grado"
        )
        tipo_tutoria = st.selectbox(
            "Tipo de Tutoría:",
            ["TE", "TL", "TM", "TD"],
            key="tipo_tutoria"
        )

        # Botón para mostrar el registro antes de guardarlo
        if st.button("Mostrar Registro antes de Guardar"):
            if all([autores, titulo, tesista, grado, tipo_tutoria]):
                nuevo_registro = f"{autores} | {tesista} | {titulo} | {grado} | {tipo_tutoria}\n"

                # Mostrar el registro generado
                st.write("Este es el registro que se enviará:")
                st.text(nuevo_registro)

                # Opciones para enviar el registro por correo
                enviar_por_correo = st.radio("¿Deseas enviar este registro por correo?", ("Sí", "No"))

                if enviar_por_correo == "Sí":
                    if correo:
                        # Enviar el registro por correo
                        subject = "Confirmación de Registro de Tutoría"
                        body = f"Hola {nombres},\n\nEl registro de tu tutoría es el siguiente:\n\n{nuevo_registro}\n\nGracias por tu colaboración."
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
                            st.success(f"Se ha enviado el registro al correo {correo}.")
                        except Exception as e:
                            st.error(f"Error al enviar el correo: {e}")
                    else:
                        st.error("No se ha encontrado un correo asociado a tu número económico.")

                # Confirmación para guardar el registro
                grabar = st.radio("¿Deseas guardar este registro?", ("Sí", "No"))

                if grabar == "Sí":
                    st.session_state.datos_articulos.append(nuevo_registro)
                    archivo_local = guardar_datos(numero_economico, [nuevo_registro])
                    if archivo_local:
                        st.success(f"Registro guardado en el archivo local: {archivo_local}")
                else:
                    st.info("El registro no ha sido guardado.")
            else:
                st.error("Por favor, completa todos los campos antes de mostrar el registro.")

        # Botón para subir los datos al servidor remoto
        if st.button("Subir Archivo al Servidor"):
            if st.session_state.datos_articulos:
                # Confirmación antes de subir al servidor
                st.write("Verifica los datos que vas a subir al servidor:")
                for registro in st.session_state.datos_articulos:
                    st.text(registro)

                subir = st.radio("¿Deseas subir estos datos al servidor?", ("Sí", "No"))
                if subir == "Sí":
                    # Generar el archivo local si aún no se ha hecho
                    archivo_local = guardar_datos(numero_economico, st.session_state.datos_articulos)
                    if archivo_local:
                        actualizar_archivo_remoto(numero_economico, st.session_state.datos_articulos)
                        st.success("Los datos se han subido al servidor correctamente.")
                    else:
                        st.error("No se pudo generar el archivo local para la subida.")
                else:
                    st.info("La subida al servidor ha sido cancelada.")
            else:
                st.error("No hay datos para subir al servidor.")

        # Botón para cerrar sesión
        if st.button("Cerrar Sesión"):
            st.session_state.validado = False
            st.session_state.clave_generada = None
            st.session_state.datos_articulos = []
            st.success("Has cerrado sesión correctamente.")

        if st.button("Cerrar Sesión."):
            st.write("Gracias por usar la aplicación. Hasta luego.")
