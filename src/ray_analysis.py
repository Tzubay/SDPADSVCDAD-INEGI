import os
import time
import json
from typing import List

import ray
import pandas as pd


DATA_PATH = "data/processed/atus_clean.parquet"
RESULTS_DIR = "results"


def asegurar_directorio_resultados():
    os.makedirs(RESULTS_DIR, exist_ok=True)


def dividir_dataframe(df: pd.DataFrame, num_particiones: int) -> List[pd.DataFrame]:
    if num_particiones <= 0:
        num_particiones = 1

    tamanio = len(df)
    paso = max(tamanio // num_particiones, 1)

    particiones = []
    for inicio in range(0, tamanio, paso):
        particiones.append(df.iloc[inicio:inicio + paso].copy())

    return particiones


@ray.remote
def analizar_particion(df: pd.DataFrame) -> dict:
    df = df.copy()
    df["indice_gravedad"] = df["heridos"] + (df["fallecidos"] * 5)

    resumen = {
        "total_accidentes": int(len(df)),
        "accidentes_con_heridos": int((df["heridos"] > 0).sum()),
        "accidentes_con_fallecidos": int((df["fallecidos"] > 0).sum()),
        "total_heridos": int(df["heridos"].sum()),
        "total_fallecidos": int(df["fallecidos"].sum()),
    }

    ranking_entidad = (
        df.groupby("entidad")
        .size()
        .reset_index(name="accidentes")
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
    )

    accidentes_por_hora = (
        df.groupby("hora")
        .size()
        .reset_index(name="accidentes")
    )

    ranking_causas = (
        df.groupby("causa")
        .size()
        .reset_index(name="accidentes")
    )

    ranking_tipos = (
        df.groupby("tipo_accidente")
        .size()
        .reset_index(name="accidentes")
    )

    tendencia_mensual = (
        df.groupby(["anio", "mes"])
        .agg(
            accidentes=("mes", "size"),
            heridos=("heridos", "sum"),
            fallecidos=("fallecidos", "sum")
        )
        .reset_index()
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
    )

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


def reducir_resumen(resultados: list[dict]) -> dict:
    resumen_final = {
        "total_accidentes": 0,
        "accidentes_con_heridos": 0,
        "accidentes_con_fallecidos": 0,
        "total_heridos": 0,
        "total_fallecidos": 0,
    }

    for resultado in resultados:
        for clave in resumen_final:
            resumen_final[clave] += int(resultado["resumen"][clave])

    return resumen_final


def reducir_dataframe(
    resultados: list[dict],
    nombre: str,
    columnas_grupo: list[str],
    columnas_suma: list[str],
    ordenar_por: str
) -> pd.DataFrame:
    partes = [r[nombre] for r in resultados]
    df = pd.concat(partes, ignore_index=True)

    agregado = (
        df.groupby(columnas_grupo)[columnas_suma]
        .sum()
        .reset_index()
        .sort_values(ordenar_por, ascending=False)
    )

    return agregado


def reducir_tendencia_mensual(resultados: list[dict]) -> pd.DataFrame:
    partes = [r["tendencia_mensual"] for r in resultados]
    df = pd.concat(partes, ignore_index=True)

    agregado = (
        df.groupby(["anio", "mes"])
        .agg(
            accidentes=("accidentes", "sum"),
            heridos=("heridos", "sum"),
            fallecidos=("fallecidos", "sum")
        )
        .reset_index()
        .sort_values(["anio", "mes"])
    )

    return agregado


def reducir_resultados(resultados: list[dict]) -> dict:
    resumen = reducir_resumen(resultados)

    ranking_entidad = reducir_dataframe(
        resultados=resultados,
        nombre="ranking_entidad",
        columnas_grupo=["entidad"],
        columnas_suma=["accidentes"],
        ordenar_por="accidentes"
    )

    ranking_municipio = reducir_dataframe(
        resultados=resultados,
        nombre="ranking_municipio",
        columnas_grupo=["entidad", "municipio"],
        columnas_suma=["accidentes", "heridos", "fallecidos", "indice_gravedad"],
        ordenar_por="accidentes"
    )

    accidentes_por_hora = reducir_dataframe(
        resultados=resultados,
        nombre="accidentes_por_hora",
        columnas_grupo=["hora"],
        columnas_suma=["accidentes"],
        ordenar_por="hora"
    ).sort_values("hora")

    ranking_causas = reducir_dataframe(
        resultados=resultados,
        nombre="ranking_causas",
        columnas_grupo=["causa"],
        columnas_suma=["accidentes"],
        ordenar_por="accidentes"
    )

    ranking_tipos = reducir_dataframe(
        resultados=resultados,
        nombre="ranking_tipos",
        columnas_grupo=["tipo_accidente"],
        columnas_suma=["accidentes"],
        ordenar_por="accidentes"
    )

    tendencia_mensual = reducir_tendencia_mensual(resultados)

    zonas_gravedad = reducir_dataframe(
        resultados=resultados,
        nombre="zonas_gravedad",
        columnas_grupo=["entidad", "municipio"],
        columnas_suma=["accidentes", "heridos", "fallecidos", "indice_gravedad"],
        ordenar_por="indice_gravedad"
    )

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


def guardar_resultados(resultados: dict, tiempo: float, num_particiones: int):
    asegurar_directorio_resultados()

    with open(os.path.join(RESULTS_DIR, "ray_summary.json"), "w", encoding="utf-8") as f:
        data = resultados["resumen"].copy()
        data["tiempo_segundos"] = tiempo
        data["particiones"] = num_particiones
        json.dump(data, f, indent=4, ensure_ascii=False)

    for nombre, valor in resultados.items():
        if isinstance(valor, pd.DataFrame):
            ruta = os.path.join(RESULTS_DIR, f"ray_{nombre}.csv")
            valor.to_csv(ruta, index=False, encoding="utf-8")


def ejecutar_ray(num_particiones: int = 12) -> tuple[dict, float]:
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(
            "No existe data/processed/atus_clean.parquet. "
            "Primero ejecuta: python src/prepare_data.py"
        )

    inicio = time.perf_counter()

    print("Conectando con Ray Cluster...")
    ray.init(address="auto", ignore_reinit_error=True)

    print("Leyendo dataset...")
    df = pd.read_parquet(DATA_PATH)

    print(f"Registros leídos: {len(df):,}")
    print(f"Dividiendo en {num_particiones} particiones...")

    particiones = dividir_dataframe(df, num_particiones)

    print("Enviando tareas distribuidas a Ray...")
    futuros = [analizar_particion.remote(particion) for particion in particiones]

    resultados_parciales = ray.get(futuros)

    print("Reduciendo resultados parciales...")
    resultados_finales = reducir_resultados(resultados_parciales)

    fin = time.perf_counter()
    tiempo = fin - inicio

    guardar_resultados(resultados_finales, tiempo, len(particiones))

    ray.shutdown()

    return resultados_finales, tiempo


def main():
    print("Ejecutando análisis distribuido con Ray...")

    resultados, tiempo = ejecutar_ray(num_particiones=12)

    print("Análisis distribuido terminado.")
    print(f"Tiempo: {tiempo:.4f} segundos")
    print("Resumen:")
    print(json.dumps(resultados["resumen"], indent=4, ensure_ascii=False))


if __name__ == "__main__":
    main()