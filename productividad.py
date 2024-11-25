import streamlit as st
import pandas as pd
import os

# Inicialización de sesión para almacenar datos temporalmente
if "data" not in st.session_state:
    st.session_state.data = {}
if "current_user" not in st.session_state:
    st.session_state.current_user = None

# Función para validar correos
def validar_correo(email1, email2):
    """Valida que los correos electrónicos coincidan."""
    return email1 == email2

# Función para guardar datos en un archivo CSV
def guardar_csv(data):
    """Guarda los datos en un archivo CSV."""
    df = pd.DataFrame(data.values())
    df.to_csv("productividad_usuario.csv", index=False, encoding="utf-8")
    st.success("Datos enviados y guardados en productividad_usuario.csv")

# Función para cargar datos desde el CSV
def cargar_csv():
    """Carga los datos desde un archivo CSV al iniciar."""
    if os.path.exists("productividad_usuario.csv"):
        df = pd.read_csv("productividad_usuario.csv", encoding="utf-8", dtype={"Número económico": str})
        for _, row in df.iterrows():
            numero_economico = row["Número económico"]
            if numero_economico not in st.session_state.data:
                st.session_state.data[numero_economico] = {
                    "Número económico": numero_economico,
                    "Nombre completo": row["Nombre completo"],
                    "Correo electrónico": row["Correo electrónico"],
                    "Títulos de libros": eval(row["Títulos de libros"]) if isinstance(row["Títulos de libros"], str) else [],
                    "Capítulos de libro": eval(row["Capítulos de libro"]) if isinstance(row["Capítulos de libro"], str) else [],
                    "Actividades académicas": eval(row["Actividades académicas"]) if isinstance(row["Actividades académicas"], str) else [],
                }

# Cargar datos al iniciar (solo si no están cargados)
if not st.session_state.data:
    cargar_csv()

# Mostrar logo
st.image("escudo_COLOR.jpg", width=150)
st.title("Productividad del Usuario")

# Campo de Número económico
st.header("Número Económico")
numero_economico = st.text_input("Número económico:")
if numero_economico:
    st.session_state.current_user = numero_economico
    if numero_economico in st.session_state.data:
        st.info(f"Datos cargados para el usuario con número económico: {numero_economico}")
    else:
        st.warning("Número económico no registrado. Puedes comenzar a capturar tus datos.")
        st.session_state.data[numero_economico] = {
            "Número económico": numero_economico,
            "Nombre completo": "",
            "Correo electrónico": "",
            "Títulos de libros": [],
            "Capítulos de libro": [],
            "Actividades académicas": []
        }

# Validar si hay un usuario activo
if st.session_state.current_user:
    user_data = st.session_state.data[st.session_state.current_user]

    # Sección de datos básicos
    st.header("Datos Básicos")
    user_data["Nombre completo"] = st.text_input("Nombre completo:", value=user_data["Nombre completo"])
    correo_1 = st.text_input("Correo electrónico:", value=user_data["Correo electrónico"])
    correo_2 = st.text_input("Confirme su correo electrónico:")
    if correo_1 and correo_2:
        if validar_correo(correo_1, correo_2):
            user_data["Correo electrónico"] = correo_1
        else:
            st.error("Los correos electrónicos no coinciden.")

    # Sección para títulos de libros
    st.header("Títulos de Libros")
    nuevo_libro = st.text_input("Añadir título de libro:")
    if st.button("Añadir libro"):
        if nuevo_libro:
            user_data["Títulos de libros"].append(nuevo_libro)
            st.success(f"Libro '{nuevo_libro}' añadido.")
        else:
            st.error("El título no puede estar vacío.")
    if user_data["Títulos de libros"]:
        st.subheader("Lista de Títulos de Libros")
        libros_a_eliminar = []
        for i, libro in enumerate(user_data["Títulos de libros"]):
            col1, col2 = st.columns([4, 1])
            with col1:
                st.write(f"{i + 1}. {libro}")
            with col2:
                if st.button(f"Eliminar libro {i + 1}", key=f"delete_book_{i}"):
                    libros_a_eliminar.append(i)
        for index in sorted(libros_a_eliminar, reverse=True):
            user_data["Títulos de libros"].pop(index)
    if st.button("Actualizar Títulos de Libros"):
        st.success("Títulos de libros actualizados correctamente.")

    # Sección para capítulos de libros
    st.header("Capítulos de Libro")
    nuevo_capitulo = st.text_input("Añadir capítulo de libro:")
    if st.button("Añadir capítulo"):
        if nuevo_capitulo:
            user_data["Capítulos de libro"].append(nuevo_capitulo)
            st.success(f"Capítulo '{nuevo_capitulo}' añadido.")
        else:
            st.error("El capítulo no puede estar vacío.")
    if user_data["Capítulos de libro"]:
        st.subheader("Lista de Capítulos de Libros")
        capitulos_a_eliminar = []
        for i, capitulo in enumerate(user_data["Capítulos de libro"]):
            col1, col2 = st.columns([4, 1])
            with col1:
                st.write(f"{i + 1}. {capitulo}")
            with col2:
                if st.button(f"Eliminar capítulo {i + 1}", key=f"delete_chapter_{i}"):
                    capitulos_a_eliminar.append(i)
        for index in sorted(capitulos_a_eliminar, reverse=True):
            user_data["Capítulos de libro"].pop(index)
    if st.button("Actualizar Capítulos de Libro"):
        st.success("Capítulos de libro actualizados correctamente.")

    # Sección para actividades académicas
    st.header("Actividades Académicas")
    nueva_actividad = st.text_input("Añadir actividad académica:")
    if st.button("Añadir actividad"):
        if nueva_actividad:
            user_data["Actividades académicas"].append(nueva_actividad)
            st.success(f"Actividad '{nueva_actividad}' añadida.")
        else:
            st.error("La actividad no puede estar vacía.")
    if user_data["Actividades académicas"]:
        st.subheader("Lista de Actividades Académicas")
        actividades_a_eliminar = []
        for i, actividad in enumerate(user_data["Actividades académicas"]):
            col1, col2 = st.columns([4, 1])
            with col1:
                st.write(f"{i + 1}. {actividad}")
            with col2:
                if st.button(f"Eliminar actividad {i + 1}", key=f"delete_activity_{i}"):
                    actividades_a_eliminar.append(i)
        for index in sorted(actividades_a_eliminar, reverse=True):
            user_data["Actividades académicas"].pop(index)
    if st.button("Actualizar Actividades Académicas"):
        st.success("Actividades académicas actualizadas correctamente.")

    # Botón Guardar
    st.header("Guardar Reporte")
    if st.button("Guardar", key="guardar", help="Guarda tu reporte"):
        guardar_csv(st.session_state.data)
        st.balloons()

