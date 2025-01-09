import streamlit as st
from ejecutar_datos import ejecutar_datos
from ejecutar_pubmed import ejecutar_pubmed
from ejecutar_nuevos import ejecutar_nuevos
from ejecutar_tutorias import ejecutar_tutorias

# Interfaz de Streamlit
# Mostrar el logo y título
st.image("escudo_COLOR.jpg", width=150)

def ejecutar_script(script):
    """Ejecuta un script de Python como subprograma."""
    st.error("Ejecutar scripts externos no es necesario aquí, ya que el programa se integra directamente.")

# Título de la aplicación
st.title("Registro de Producción")

# Menú lateral
opciones = ["Inicio", "Registro del Usuario", "Extracción Artículos PubMed", "Captura de Artículos", "Captura de Tutorías", "Salir"]
opcion_seleccionada = st.sidebar.selectbox("Selecciona una opción:", opciones)

# Manejo de opciones del menú
if opcion_seleccionada == "Inicio":
    st.write("Selecciona una opción del menú para comenzar.")

elif opcion_seleccionada == "Registro del Usuario":
    st.write("Ejecutando el programa de Registro del Usuario...")
    ejecutar_datos()

elif opcion_seleccionada == "Extracción Artículos PubMed":
    st.write("Ejecutando el programa de Extracción...")
    ejecutar_pubmed()

elif opcion_seleccionada == "Captura de Artículos":
    st.write("Ejecutando el programa de Artículos...")
    ejecutar_nuevos()

elif opcion_seleccionada == "Captura de Tutorías":
    st.write("Ejecutando el programa de Tutorías...")
    ejecutar_tutorias()

elif opcion_seleccionada == "Salir":
    st.write("Gracias por usar la aplicación. Hasta luego.")
    st.stop()

else:
    st.error("Opción no válida.")

