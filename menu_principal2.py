import streamlit as st

st.title("Menú Principal")

# Opciones del menú
opcion = st.sidebar.selectbox("Seleccione una opción:", 
    ["Inicio", "Registro del Usuario", "Extracción desde PubMed", "Salir"])

if opcion == "Inicio":
    st.write("Bienvenido al sistema. Seleccione una opción en el menú lateral.")
elif opcion == "Registro del Usuario":
    st.markdown("[Ir a Registro del Usuario](https://appuctividad-pgm-final.streamlit.app/)", unsafe_allow_html=True)
elif opcion == "Extracción desde PubMed":
    st.markdown("[Ir a Extracción desde PubMed](https://appuctividad-pgm-pubmed.streamlit.app/)", unsafe_allow_html=True)
elif opcion == "Salir":
    st.write("Gracias por usar la aplicación. Cierre esta ventana para salir.")

