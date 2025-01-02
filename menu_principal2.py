import streamlit as st

st.title("Menú Principal")

# Opciones del menú
opcion = st.sidebar.selectbox(
    "Seleccione una opción:",
    ["Inicio", "Registro del Usuario", "Extracción desde PubMed", "Salir"]
)

if opcion == "Inicio":
    st.write("Bienvenido al sistema. Seleccione una opción en el menú lateral.")
elif opcion == "Registro del Usuario":
    # Redirección automática a la URL del programa
    js = "window.location.href = 'https://appuctividad-pgm-final.streamlit.app/';"
    st.components.v1.html(f"<script>{js}</script>", height=0)
elif opcion == "Extracción desde PubMed":
    js = "window.location.href = 'https://appuctividad-pgm-pubmed.streamlit.app/';"
    st.components.v1.html(f"<script>{js}</script>", height=0)
elif opcion == "Salir":
    st.write("Gracias por usar la aplicación. Cierre esta ventana para salir.")

