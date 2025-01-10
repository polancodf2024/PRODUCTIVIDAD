import os
import csv
import ssl
import smtplib
import tempfile
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

# Variables de configuración
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = "abcdf2024dfabc@gmail.com"
EMAIL_PASSWORD = "hjdd gqaw vvpj hbsy"
NOTIFICATION_EMAIL = "polanco@unam.mx"
REMOTE_HOST = "187.217.52.137"
REMOTE_USER = "POLANCO6"
REMOTE_PASSWORD = "tt6plco6"
REMOTE_DIR = "/home/POLANCO6/BIBLIOGRAFIA"
DUPLICADOS_FILE = "duplicados.txt"

def consultar_duplicados(numero_economico):
    """Verifica si el número económico ya existe en el archivo duplicados.txt en el servidor remoto."""
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(REMOTE_HOST, port=3792, username=REMOTE_USER,
                    password=REMOTE_PASSWORD)

        sftp = ssh.open_sftp()
        remote_path = os.path.join(REMOTE_DIR, DUPLICADOS_FILE)

        try:
            with sftp.open(remote_path, "r") as file:
                duplicados = file.read().decode("utf-8").splitlines()
        except FileNotFoundError:
            duplicados = []

        sftp.close()
        ssh.close()

        return numero_economico in duplicados

    except Exception as e:
        st.error(f"Error al consultar duplicados: {e}")
        return False

def registrar_duplicado(numero_economico):
    """Agrega el número económico al archivo duplicados.txt en el servidor remoto."""
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(REMOTE_HOST, port=3792, username=REMOTE_USER,
                    password=REMOTE_PASSWORD)

        sftp = ssh.open_sftp()
        remote_path = os.path.join(REMOTE_DIR, DUPLICADOS_FILE)

        try:
            with sftp.open(remote_path, "a") as file:
                file.write(f"{numero_economico}\n")
        except FileNotFoundError:
            with sftp.open(remote_path, "w") as file:
                file.write(f"{numero_economico}\n")

        sftp.close()
        ssh.close()

    except Exception as e:
        st.error(f"Error al registrar duplicado: {e}")

def guardar_datos_personales(nombres, apellidos, numero_economico, nivel_sni, nivel_sni_salud, correo):
    """Guarda los datos personales en un archivo de texto en formato delimitado por dos puntos."""
    archivo = f"datos_personales_{numero_economico}.txt"
    with open(archivo, "w", encoding="utf-8") as file:
        file.write(
            f"{nombres}:{apellidos}:{numero_economico}:{nivel_sni}:{nivel_sni_salud}:{correo}\n")
    return archivo

def enviar_correo_confirmacion(correo, nombres, archivo):
    """Envía un correo de confirmación al usuario con el archivo adjunto."""
    subject = "Confirmación de Registro"
    body = f"Hola {nombres},\n\nTu registro se ha realizado exitosamente.\n\nAdjunto encontrarás el archivo con tus datos registrados.\n\nGracias por usar nuestra plataforma."

    mensaje = MIMEMultipart()
    mensaje['From'] = EMAIL_USER
    mensaje['To'] = correo
    mensaje['Subject'] = subject
    mensaje.attach(MIMEText(body, 'plain'))

    try:
        # Adjuntar el archivo
        with open(archivo, "rb") as adjunto:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(adjunto.read())
            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename={Path(archivo).name}'
            )
            mensaje.attach(part)

        # Enviar el correo
        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls(context=context)
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_USER, correo, mensaje.as_string())
        st.success(
            f"Se ha enviado un correo de confirmación a {correo} con el archivo adjunto.")
    except Exception as e:
        st.error(f"Error al enviar el correo: {e}")

def subir_archivo_servidor(nombre_archivo):
    """Sube el archivo generado al servidor remoto."""
    try:
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
    except Exception as e:
        st.error(f"Error al subir el archivo al servidor remoto: {e}")

def interfaz_principal():
    # Interfaz de Streamlit
    # Mostrar el logo y título
    st.image("escudo_COLOR.jpg", width=150)

    st.title("Registro del Usuario")

    nombres = st.text_input("Ingresa tus nombres:")
    apellidos = st.text_input("Ingresa tus apellidos:")
    numero_economico = st.text_input("Número Económico:")

    nivel_sni = st.selectbox("Nivel de SNI:", [
        "No pertenece", "Candidato", "I", "II", "III", "Emérito"
    ])

    nivel_sni_salud = st.selectbox("Nivel de SNI-Salud:", [
        "No pertenece", "Candidato", "A", "B", "C", "D", "E", "F"
    ])

    correo_1 = st.text_input("Correo electrónico:")
    correo_2 = st.text_input("Confirme su correo electrónico:")

    if correo_1 and correo_2:
        if correo_1 != correo_2:
            st.error("Los correos electrónicos no coinciden.")
        else:
            st.success("Los correos coinciden.")

    if st.button("Guardar Datos"):
        if nombres and apellidos and numero_economico and correo_1 == correo_2 and correo_1:
            if consultar_duplicados(numero_economico):
                st.error(
                    "El número económico ya está registrado. No se pueden guardar los datos nuevamente.")
            else:
                archivo = guardar_datos_personales(
                    nombres, apellidos, numero_economico, nivel_sni, nivel_sni_salud, correo_1)
                subir_archivo_servidor(archivo)
                registrar_duplicado(numero_economico)
                enviar_correo_confirmacion(correo_1, nombres, archivo)
        else:
            st.error("Por favor, completa todos los campos correctamente.")

    if st.button("Cerrar Sesión."):
        st.write("Gracias por usar la aplicación. Hasta luego.")

if __name__ == "__main__":
    interfaz_principal()

