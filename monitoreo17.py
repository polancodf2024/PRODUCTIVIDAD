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

# Configuración de logging
logging.basicConfig(
    filename='monitoreo.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ====================
# CONFIGURACIÓN INICIAL
# ====================
class Config:
    def __init__(self):
        # Configuración SFTP
        self.REMOTE_PRODUCTOS_PREFIX = st.secrets["prefixes"]["productos"]
        self.TIMEOUT_SECONDS = 30
        
        self.REMOTE = {
            'HOST': st.secrets["sftp"]["host"],
            'USER': st.secrets["sftp"]["user"],
            'PASSWORD': st.secrets["sftp"]["password"],
            'PORT': st.secrets["sftp"]["port"],
            'DIR': st.secrets["sftp"]["dir"]
        }

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
                        # Crear archivo local con estructura correcta
                        columns = [
                            'economic_number', 'departamento', 'participation_key', 'investigator_name',
                            'corresponding_author', 'coauthors', 'article_title', 'year',
                            'pub_date', 'volume', 'number', 'pages', 'journal_full',
                            'journal_abbrev', 'doi', 'jcr_group', 'pmid', 'selected_keywords',
                            'estado'
                        ]
                        pd.DataFrame(columns=columns).to_csv(local_path, index=False)
                        logging.info(f"Archivo remoto no encontrado, creado local con estructura: {local_path}")
                        return True
                        
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

    @staticmethod
    def upload_remote_file(local_path, remote_path):
        """Sube un archivo con verificación de integridad"""
        if not os.path.exists(local_path):
            logging.error(f"Archivo local no existe: {local_path}")
            st.error("El archivo local no existe")
            return False
            
        for attempt in range(SSHManager.MAX_RETRIES):
            ssh = SSHManager.get_connection()
            if not ssh:
                return False
                
            try:
                with ssh.open_sftp() as sftp:
                    sftp.put(local_path, remote_path)
                    
                    if SSHManager.verify_file_integrity(local_path, remote_path, sftp):
                        logging.info(f"Archivo subido correctamente: {local_path} a {remote_path}")
                        return True
                    else:
                        logging.warning(f"Error de integridad en subida, reintentando... (intento {attempt + 1})")
                        if attempt < SSHManager.MAX_RETRIES - 1:
                            time.sleep(SSHManager.RETRY_DELAY)
                        else:
                            raise Exception("Fallo en verificación de integridad después de múltiples intentos")
                            
            except Exception as e:
                logging.error(f"Error en subida (intento {attempt + 1}): {str(e)}")
                if attempt == SSHManager.MAX_RETRIES - 1:
                    st.error(f"Error subiendo archivo remoto después de {SSHManager.MAX_RETRIES} intentos: {str(e)}")
                    return False
                    
            finally:
                ssh.close()

def sync_productos_file():
    """Sincroniza el archivo productos desde el servidor remoto"""
    try:
        remote_filename = f"{CONFIG.REMOTE_PRODUCTOS_PREFIX}productos.csv"
        remote_path = os.path.join(CONFIG.REMOTE['DIR'], remote_filename)
        local_path = "productos.csv"

        with st.spinner("🔄 Sincronizando archivo productos desde el servidor..."):
            if SSHManager.download_remote_file(remote_path, local_path):
                st.success("✅ Archivo productos sincronizado correctamente")
                return True
            else:
                # Si no existe el archivo remoto, crear uno local con estructura correcta
                columns = [
                    'economic_number', 'departamento', 'participation_key', 'investigator_name',
                    'corresponding_author', 'coauthors', 'article_title', 'year',
                    'pub_date', 'volume', 'number', 'pages', 'journal_full',
                    'journal_abbrev', 'doi', 'jcr_group', 'pmid', 'selected_keywords',
                    'estado'
                ]

                # Verificar si el archivo local ya existe
                if not Path(local_path).exists():
                    pd.DataFrame(columns=columns).to_csv(local_path, index=False)
                    st.info("ℹ️ No se encontró archivo remoto. Se creó uno nuevo localmente con la estructura correcta.")
                else:
                    # Si el archivo local existe pero está vacío o corrupto
                    try:
                        df = pd.read_csv(local_path)
                        if df.empty:
                            pd.DataFrame(columns=columns).to_csv(local_path, index=False)
                    except:
                        pd.DataFrame(columns=columns).to_csv(local_path, index=False)

                st.warning("⚠️ Trabajando con copia local de productos debido a problemas de conexión")
                return False

    except Exception as e:
        st.error(f"❌ Error en sincronización: {str(e)}")
        logging.error(f"Sync Error: {str(e)}")
        return False

def main():
    # Añadir logo en la parte superior
    st.image("escudo_COLOR.jpg", width=200)
    
    st.title("Análisis de Manuscritos")
    
    # Sincronizar archivo productos al inicio
    if not sync_productos_file():
        st.warning("⚠️ Trabajando con copia local de productos debido a problemas de conexión")
    
    # Verificar si el archivo local existe y tiene datos
    try:
        if not Path("productos.csv").exists():
            st.error("No se encontró el archivo productos")
            return
        
        # Leer y procesar el archivo
        df = pd.read_csv("productos.csv")
        
        # Verificar si el DataFrame está vacío
        if df.empty:
            st.warning("El archivo productos está vacío")
            return
            
        # Convertir y validar fechas
        df['pub_date'] = pd.to_datetime(df['pub_date'], errors='coerce')
        df = df[(df['estado'] == 'A') & (df['pub_date'].notna())]
        
        if df.empty:
            st.warning("No hay publicaciones válidas para analizar")
            return
        
        st.success(f"Datos cargados correctamente. Registros activos: {len(df)}")
        
        # Resto del código de análisis (se mantiene igual)
        # ... [todo el código de análisis que ya tenías]
        
    except pd.errors.EmptyDataError:
        st.error("El archivo productos está vacío o corrupto")
        logging.error("El archivo productos está vacío o corrupto")
        return
    except Exception as e:
        st.error(f"Error al procesar el archivo: {str(e)}")
        logging.error(f"Error en main: {str(e)}")
        return

if __name__ == "__main__":
    main()
