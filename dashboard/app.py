import os
import json

import pandas as pd
import streamlit as st
import plotly.express as px


RESULTS_DIR = "results"


st.set_page_config(
    page_title="Análisis distribuido ATUS",
    page_icon="🚗",
    layout="wide"
)


def cargar_csv(nombre_archivo: str) -> pd.DataFrame:
    ruta = os.path.join(RESULTS_DIR, nombre_archivo)

    if not os.path.exists(ruta):
        return pd.DataFrame()

    return pd.read_csv(ruta)


def cargar_json(nombre_archivo: str) -> dict:
    ruta = os.path.join(RESULTS_DIR, nombre_archivo)

    if not os.path.exists(ruta):
        return {}

    with open(ruta, "r", encoding="utf-8") as f:
        return json.load(f)


def mostrar_mensaje_sin_resultados():
    st.warning(
        "Todavía no existen resultados. Primero ejecuta el benchmark dentro del contenedor ray-head:\n\n"
        "`python src/prepare_data.py --simulate --rows 200000`\n\n"
        "`python src/benchmark.py`"
    )


st.title("🚗 Sistema distribuido para análisis de siniestros viales ATUS")
st.write(
    "Dashboard del proyecto final de Cómputo Paralelo y Distribuido usando "
    "Python, Ray, Ray Cluster, Docker, Pandas y Streamlit."
)

summary_ray = cargar_json("ray_summary.json")
summary_seq = cargar_json("sequential_summary.json")
benchmark = cargar_csv("benchmark.csv")

if not summary_ray:
    mostrar_mensaje_sin_resultados()
    st.stop()


st.header("Indicadores generales")

col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("Total accidentes", f"{summary_ray.get('total_accidentes', 0):,}")
col2.metric("Con heridos", f"{summary_ray.get('accidentes_con_heridos', 0):,}")
col3.metric("Con fallecidos", f"{summary_ray.get('accidentes_con_fallecidos', 0):,}")
col4.metric("Total heridos", f"{summary_ray.get('total_heridos', 0):,}")
col5.metric("Total fallecidos", f"{summary_ray.get('total_fallecidos', 0):,}")


if not benchmark.empty:
    st.header("Comparación de rendimiento")

    col_a, col_b = st.columns([1, 2])

    with col_a:
        st.dataframe(benchmark, use_container_width=True)

    with col_b:
        fig = px.bar(
            benchmark,
            x="metodo",
            y="tiempo_segundos",
            title="Tiempo de ejecución por método",
            text="tiempo_segundos"
        )
        st.plotly_chart(fig, use_container_width=True)


st.header("Análisis por entidad federativa")

ranking_entidad = cargar_csv("ray_ranking_entidad.csv")

if not ranking_entidad.empty:
    top_entidades = ranking_entidad.head(15)

    fig = px.bar(
        top_entidades,
        x="entidad",
        y="accidentes",
        title="Top 15 entidades con más accidentes"
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(ranking_entidad, use_container_width=True)


st.header("Análisis municipal")

ranking_municipio = cargar_csv("ray_ranking_municipio.csv")

if not ranking_municipio.empty:
    top_municipios = ranking_municipio.head(20)

    fig = px.bar(
        top_municipios,
        x="municipio",
        y="accidentes",
        color="entidad",
        title="Top 20 municipios con mayor siniestralidad"
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(ranking_municipio, use_container_width=True)


st.header("Distribución por hora del día")

accidentes_por_hora = cargar_csv("ray_accidentes_por_hora.csv")

if not accidentes_por_hora.empty:
    fig = px.line(
        accidentes_por_hora,
        x="hora",
        y="accidentes",
        markers=True,
        title="Accidentes por hora del día"
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(accidentes_por_hora, use_container_width=True)


st.header("Causas más frecuentes")

ranking_causas = cargar_csv("ray_ranking_causas.csv")

if not ranking_causas.empty:
    fig = px.bar(
        ranking_causas.head(15),
        x="causa",
        y="accidentes",
        title="Ranking de causas de accidentes"
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(ranking_causas, use_container_width=True)


st.header("Tipos de accidente")

ranking_tipos = cargar_csv("ray_ranking_tipos.csv")

if not ranking_tipos.empty:
    fig = px.pie(
        ranking_tipos,
        names="tipo_accidente",
        values="accidentes",
        title="Distribución por tipo de accidente"
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(ranking_tipos, use_container_width=True)


st.header("Tendencia mensual")

tendencia_mensual = cargar_csv("ray_tendencia_mensual.csv")

if not tendencia_mensual.empty:
    tendencia_mensual["periodo"] = (
        tendencia_mensual["anio"].astype(str)
        + "-"
        + tendencia_mensual["mes"].astype(str).str.zfill(2)
    )

    fig = px.line(
        tendencia_mensual,
        x="periodo",
        y="accidentes",
        markers=True,
        title="Tendencia mensual de accidentes"
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(tendencia_mensual, use_container_width=True)


st.header("Zonas con mayor gravedad")

zonas_gravedad = cargar_csv("ray_zonas_gravedad.csv")

if not zonas_gravedad.empty:
    top_gravedad = zonas_gravedad.head(20)

    fig = px.bar(
        top_gravedad,
        x="municipio",
        y="indice_gravedad",
        color="entidad",
        title="Top 20 zonas con mayor índice de gravedad"
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(zonas_gravedad, use_container_width=True)


st.success("Dashboard cargado correctamente.")