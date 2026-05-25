import os
import time
import json

import pandas as pd


DATA_PATH = "data/processed/atus_clean.parquet"
RESULTS_DIR = "results"


def asegurar_directorio_resultados():
    os.makedirs(RESULTS_DIR, exist_ok=True)


def cargar_datos() -> pd.DataFrame:
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(
            "No existe data/processed/atus_clean.parquet. "
            "Primero ejecuta: python src/prepare_data.py"
        )

    return pd.read_parquet(DATA_PATH)


def agregar_indice_gravedad(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["indice_gravedad"] = df["heridos"] + (df["fallecidos"] * 5)
    return df


def ejecutar_analisis_secuencial(df: pd.DataFrame) -> dict:
    df = agregar_indice_gravedad(df)

    total_accidentes = len(df)
    accidentes_con_heridos = int((df["heridos"] > 0).sum())
    accidentes_con_fallecidos = int((df["fallecidos"] > 0).sum())
    total_heridos = int(df["heridos"].sum())
    total_fallecidos = int(df["fallecidos"].sum())

    ranking_entidad = (
        df.groupby("entidad")
        .size()
        .reset_index(name="accidentes")
        .sort_values("accidentes", ascending=False)
    )

    ranking_municipio = (
        df.groupby(["entidad", "municipio"])
        .agg(
            accidentes=("municipio", "size"),
            heridos=("heridos", "sum"),
            fallecidos=("fallecidos", "sum"),
            indice_gravedad=("indice_gravedad", "sum")
        )
        .reset_index()
        .sort_values("accidentes", ascending=False)
    )

    accidentes_por_hora = (
        df.groupby("hora")
        .size()
        .reset_index(name="accidentes")
        .sort_values("hora")
    )

    ranking_causas = (
        df.groupby("causa")
        .size()
        .reset_index(name="accidentes")
        .sort_values("accidentes", ascending=False)
    )

    ranking_tipos = (
        df.groupby("tipo_accidente")
        .size()
        .reset_index(name="accidentes")
        .sort_values("accidentes", ascending=False)
    )

    tendencia_mensual = (
        df.groupby(["anio", "mes"])
        .agg(
            accidentes=("mes", "size"),
            heridos=("heridos", "sum"),
            fallecidos=("fallecidos", "sum")
        )
        .reset_index()
        .sort_values(["anio", "mes"])
    )

    zonas_gravedad = (
        df.groupby(["entidad", "municipio"])
        .agg(
            accidentes=("municipio", "size"),
            heridos=("heridos", "sum"),
            fallecidos=("fallecidos", "sum"),
            indice_gravedad=("indice_gravedad", "sum")
        )
        .reset_index()
        .sort_values("indice_gravedad", ascending=False)
    )

    resumen = {
        "total_accidentes": int(total_accidentes),
        "accidentes_con_heridos": accidentes_con_heridos,
        "accidentes_con_fallecidos": accidentes_con_fallecidos,
        "total_heridos": total_heridos,
        "total_fallecidos": total_fallecidos,
    }

    return {
        "resumen": resumen,
        "ranking_entidad": ranking_entidad,
        "ranking_municipio": ranking_municipio,
        "accidentes_por_hora": accidentes_por_hora,
        "ranking_causas": ranking_causas,
        "ranking_tipos": ranking_tipos,
        "tendencia_mensual": tendencia_mensual,
        "zonas_gravedad": zonas_gravedad,
    }


def guardar_resultados(resultados: dict, tiempo: float):
    asegurar_directorio_resultados()

    with open(os.path.join(RESULTS_DIR, "sequential_summary.json"), "w", encoding="utf-8") as f:
        data = resultados["resumen"].copy()
        data["tiempo_segundos"] = tiempo
        json.dump(data, f, indent=4, ensure_ascii=False)

    for nombre, valor in resultados.items():
        if isinstance(valor, pd.DataFrame):
            ruta = os.path.join(RESULTS_DIR, f"sequential_{nombre}.csv")
            valor.to_csv(ruta, index=False, encoding="utf-8")


def main():
    print("Ejecutando análisis secuencial con Pandas...")

    inicio = time.perf_counter()

    df = cargar_datos()
    resultados = ejecutar_analisis_secuencial(df)

    fin = time.perf_counter()
    tiempo = fin - inicio

    guardar_resultados(resultados, tiempo)

    print("Análisis secuencial terminado.")
    print(f"Tiempo: {tiempo:.4f} segundos")
    print("Resumen:")
    print(json.dumps(resultados["resumen"], indent=4, ensure_ascii=False))


if __name__ == "__main__":
    main()