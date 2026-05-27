import os
import glob
import argparse
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
OUTPUT_PARQUET = os.path.join(PROCESSED_DIR, "atus_clean.parquet")
OUTPUT_CSV = os.path.join(PROCESSED_DIR, "atus_clean.csv")


ESTADOS = [
    "Aguascalientes", "Baja California", "Baja California Sur", "Campeche",
    "Coahuila", "Colima", "Chiapas", "Chihuahua", "Ciudad de México",
    "Durango", "Guanajuato", "Guerrero", "Hidalgo", "Jalisco",
    "México", "Michoacán", "Morelos", "Nayarit", "Nuevo León",
    "Oaxaca", "Puebla", "Querétaro", "Quintana Roo", "San Luis Potosí",
    "Sinaloa", "Sonora", "Tabasco", "Tamaulipas", "Tlaxcala",
    "Veracruz", "Yucatán", "Zacatecas"
]

CAUSAS = [
    "Conductor",
    "Peatón o pasajero",
    "Falla del vehículo",
    "Mala condición del camino",
    "Otra"
]

TIPOS_ACCIDENTE = [
    "Colisión con vehículo automotor",
    "Colisión con peatón",
    "Colisión con objeto fijo",
    "Volcadura",
    "Salida del camino",
    "Otro"
]


def normalizar_columna(nombre: str) -> str:
    nombre = str(nombre).strip().lower()
    # Eliminar BOM si existe
    nombre = nombre.replace("\ufeff", "").replace("ï»¿", "")
    reemplazos = {
        "á": "a",
        "é": "e",
        "í": "i",
        "ó": "o",
        "ú": "u",
        "ñ": "n",
        " ": "_",
        "-": "_",
        ".": "",
        "/": "_",
    }

    for original, nuevo in reemplazos.items():
        nombre = nombre.replace(original, nuevo)

    while "__" in nombre:
        nombre = nombre.replace("__", "_")

    return nombre


def leer_csv_seguro(ruta: str) -> pd.DataFrame:
    # Intentar primero utf-8-sig para manejar BOM correctamente
    try:
        return pd.read_csv(ruta, encoding="utf-8-sig", low_memory=False)
    except UnicodeDecodeError:
        try:
            return pd.read_csv(ruta, encoding="utf-8", low_memory=False)
        except UnicodeDecodeError:
            return pd.read_csv(ruta, encoding="latin-1", low_memory=False)


def cargar_catalogo(ruta: str) -> pd.DataFrame | None:
    if not os.path.exists(ruta):
        return None

    df = leer_csv_seguro(ruta)
    df.columns = [normalizar_columna(c) for c in df.columns]
    return df


def buscar_archivos_atus() -> list[str]:
    patrones = [
        os.path.join(RAW_DIR, "**", "atus_anual_*.csv"),
        os.path.join(RAW_DIR, "**", "conjunto_de_datos", "*.csv"),
        os.path.join(RAW_DIR, "*.csv"),
    ]

    archivos = []

    for patron in patrones:
        archivos.extend(glob.glob(patron, recursive=True))

    archivos = sorted(list(set(archivos)))

    archivos = [
        archivo for archivo in archivos
        if "diccionario" not in archivo.lower()
        and "catalogo" not in archivo.lower()
        and "tc_" not in os.path.basename(archivo).lower()
    ]

    return archivos


def encontrar_base_atus() -> str | None:
    # Primero verificar si los catalogos están directamente en raw
    if os.path.exists(os.path.join(RAW_DIR, "catalogos")):
        return RAW_DIR

    posibles = glob.glob(
        os.path.join(RAW_DIR, "**", "conjunto_de_datos_atus_anual_csv"),
        recursive=True
    )

    if posibles:
        return posibles[0]

    return None


def construir_diccionario_entidades(base_atus: str | None) -> dict:
    if not base_atus:
        return {}

    ruta = os.path.join(base_atus, "catalogos", "tc_entidad.csv")
    catalogo = cargar_catalogo(ruta)

    if catalogo is None:
        return {}

    columnas = list(catalogo.columns)

    col_id = None
    col_nombre = None

    for col in columnas:
        if col in ["id_entidad", "entidad", "cve_entidad", "cve_ent"]:
            col_id = col
        if col in ["nom_entidad", "desc_entidad", "entidad_federativa", "nombre", "nom_ent"]:
            col_nombre = col

    if col_id is None:
        for col in columnas:
            if "entidad" in col and col != col_nombre:
                col_id = col
                break

    if col_nombre is None:
        for col in columnas:
            if "nom" in col or "desc" in col:
                col_nombre = col
                break

    if col_id is None or col_nombre is None:
        return {}

    catalogo[col_id] = pd.to_numeric(catalogo[col_id], errors="coerce")

    return dict(zip(catalogo[col_id], catalogo[col_nombre].astype(str)))


def construir_diccionario_municipios(base_atus: str | None) -> dict:
    if not base_atus:
        return {}

    ruta = os.path.join(base_atus, "catalogos", "tc_municipio.csv")
    catalogo = cargar_catalogo(ruta)

    if catalogo is None:
        return {}

    columnas = list(catalogo.columns)

    col_entidad = None
    col_municipio = None
    col_nombre = None

    for col in columnas:
        if col in ["id_entidad", "entidad", "cve_entidad", "cve_ent"]:
            col_entidad = col
        if col in ["id_municipio", "municipio", "cve_municipio", "cve_mun"]:
            col_municipio = col
        if col in ["nom_municipio", "desc_municipio", "nombre", "nom_mun"]:
            col_nombre = col

    if col_entidad is None:
        for col in columnas:
            if "entidad" in col:
                col_entidad = col
                break

    if col_municipio is None:
        for col in columnas:
            if "municipio" in col and col != col_nombre:
                col_municipio = col
                break

    if col_nombre is None:
        for col in columnas:
            if "nom" in col or "desc" in col:
                col_nombre = col
                break

    if col_entidad is None or col_municipio is None or col_nombre is None:
        return {}

    catalogo[col_entidad] = pd.to_numeric(catalogo[col_entidad], errors="coerce").fillna(-1).astype(int)
    catalogo[col_municipio] = pd.to_numeric(catalogo[col_municipio], errors="coerce").fillna(-1).astype(int)

    resultado = {}

    for _, fila in catalogo.iterrows():
        llave = (fila[col_entidad], fila[col_municipio])
        resultado[llave] = str(fila[col_nombre])

    return resultado


def generar_datos_simulados(n: int = 200_000) -> pd.DataFrame:
    print(f"Generando dataset simulado con {n:,} registros...")

    np.random.seed(42)
    random.seed(42)

    fecha_inicio = datetime(2019, 1, 1)
    fecha_fin = datetime(2024, 12, 31)
    dias_rango = (fecha_fin - fecha_inicio).days

    fechas = [
        fecha_inicio + timedelta(days=int(x))
        for x in np.random.randint(0, dias_rango, size=n)
    ]

    estados = np.random.choice(ESTADOS, size=n)

    municipios = []
    for estado in estados:
        municipios.append(f"Municipio_{random.randint(1, 80)}_{estado[:3]}")

    pesos_horas = np.array([
        25, 18, 14, 12, 12, 18,
        35, 55, 60, 45, 40, 42,
        45, 43, 46, 52, 60, 70,
        75, 65, 55, 45, 35, 25
    ], dtype=float)

    pesos_horas = pesos_horas / pesos_horas.sum()

    horas = np.random.choice(
        list(range(24)),
        size=n,
        p=pesos_horas
    )

    causas = np.random.choice(
        CAUSAS,
        size=n,
        p=[0.72, 0.10, 0.06, 0.07, 0.05]
    )

    tipos = np.random.choice(
        TIPOS_ACCIDENTE,
        size=n,
        p=[0.48, 0.12, 0.16, 0.09, 0.08, 0.07]
    )

    heridos = np.random.poisson(lam=0.28, size=n)
    fallecidos = np.random.choice(
        [0, 1, 2, 3],
        size=n,
        p=[0.965, 0.028, 0.006, 0.001]
    )

    df = pd.DataFrame({
        "fecha": fechas,
        "anio": [f.year for f in fechas],
        "mes": [f.month for f in fechas],
        "hora": horas,
        "entidad": estados,
        "municipio": municipios,
        "causa": causas,
        "tipo_accidente": tipos,
        "heridos": heridos,
        "fallecidos": fallecidos,
    })

    return df


def obtener_columna(df: pd.DataFrame, opciones: list[str]) -> str | None:
    columnas = list(df.columns)

    for opcion in opciones:
        opcion = normalizar_columna(opcion)
        if opcion in columnas:
            return opcion

    for col in columnas:
        for opcion in opciones:
            opcion = normalizar_columna(opcion)
            if opcion in col:
                return col

    return None


def sumar_columnas_existentes(df: pd.DataFrame, columnas: list[str]) -> pd.Series:
    total = pd.Series(0, index=df.index)

    for col in columnas:
        col_norm = normalizar_columna(col)
        if col_norm in df.columns:
            total = total + pd.to_numeric(df[col_norm], errors="coerce").fillna(0)

    return total


def limpiar_dataset_atus_real(df: pd.DataFrame, base_atus: str | None) -> pd.DataFrame:
    print("Limpiando dataset real de ATUS...")

    df = df.copy()
    df.columns = [normalizar_columna(c) for c in df.columns]

    dic_entidades = construir_diccionario_entidades(base_atus)
    dic_municipios = construir_diccionario_municipios(base_atus)

    col_anio = obtener_columna(df, ["anio", "año"])
    col_mes = obtener_columna(df, ["mes", "id_mes"])
    col_hora = obtener_columna(df, ["id_hora", "hora"])
    
    col_entidad = obtener_columna(df, ["id_entidad", "entidad", "edo", "cve_ent", "cve_entidad"])
    col_municipio = obtener_columna(df, ["id_municipio", "municipio", "mun", "cve_mun", "cve_municipio"])
    col_causa = obtener_columna(df, ["causaacci", "causa", "causa_accidente"])
    col_tipo = obtener_columna(df, ["tipaccid", "tipo_accidente", "clase", "clasacc"])

    limpio = pd.DataFrame()

    if col_anio:
        limpio["anio"] = pd.to_numeric(df[col_anio], errors="coerce")
    else:
        limpio["anio"] = 2024

    if col_mes:
        limpio["mes"] = pd.to_numeric(df[col_mes], errors="coerce")
    else:
        limpio["mes"] = 1

    if col_hora:
        limpio["hora"] = pd.to_numeric(df[col_hora], errors="coerce")
    else:
        limpio["hora"] = 0

    if col_entidad:
        entidad_id = pd.to_numeric(df[col_entidad], errors="coerce").fillna(-1).astype(int)
        entidad_id = entidad_id.where(entidad_id.between(1, 32))
        if dic_entidades:
            limpio["entidad"] = entidad_id.map(dic_entidades)
        else:
            limpio["entidad"] = entidad_id.astype(str)
    else:
        entidad_id = pd.Series(np.nan, index=df.index)
        limpio["entidad"] = "No especificado"

    if col_municipio:
        municipio_id = pd.to_numeric(df[col_municipio], errors="coerce").fillna(-1).astype(int)

        if dic_municipios and col_entidad:
            nombres_municipio = []
            for ent, mun in zip(entidad_id, municipio_id):
                if pd.isna(mun) or pd.isna(ent) or mun == -1 or ent == -1:
                    nombres_municipio.append("No especificado")
                    continue
                
                ent_int = int(ent)
                mun_int = int(mun)
                
                nombre = (
                    dic_municipios.get((ent_int, mun_int)) or
                    dic_municipios.get((str(ent_int), str(mun_int))) or
                    dic_municipios.get((f"{ent_int:02d}", f"{mun_int:03d}")) or
                    dic_municipios.get(mun_int) or
                    dic_municipios.get(str(mun_int))
                )
                
                nombres_municipio.append(nombre if nombre else "No especificado")
            limpio["municipio"] = nombres_municipio
        else:
            limpio["municipio"] = df[col_municipio].astype(str)
    else:
        limpio["municipio"] = "No especificado"

    mapeo_causas = {
        "1": "Conductor", "2": "Peatón o pasajero", "3": "Falla del vehículo",
        "4": "Mala condición del camino", "5": "Otra"
    }
    mapeo_tipos = {
        "1": "Colisión con vehículo automotor", "2": "Colisión con peatón", 
        "3": "Colisión con animal", "4": "Colisión con objeto fijo", 
        "5": "Volcadura", "6": "Caída de pasajero", "7": "Salida del camino", 
        "8": "Incendio", "9": "Colisión con ferrocarril", "10": "Colisión con motocicleta", 
        "11": "Colisión con ciclista", "0": "Otro"
    }

    if col_causa:
        causa_raw = df[col_causa].astype(str).str.strip().str.replace(".0", "", regex=False)
        limpio["causa"] = causa_raw.map(mapeo_causas).fillna(causa_raw)
    else:
        limpio["causa"] = "No especificado"

    if col_tipo:
        tipo_raw = df[col_tipo].astype(str).str.strip().str.replace(".0", "", regex=False)
        limpio["tipo_accidente"] = tipo_raw.map(mapeo_tipos).fillna(tipo_raw)
    else:
        limpio["tipo_accidente"] = "No especificado"

    columnas_heridos = [
        "condherido", "pasaherido", "peatherido", "ciclherido",
        "otroherido", "neherido", "heridos", "lesionados"
    ]

    columnas_fallecidos = [
        "condmuerto", "pasamuerto", "peatmuerto", "ciclmuerto",
        "otromuerto", "nemuerto", "fallecidos", "muertos"
    ]

    limpio["heridos"] = sumar_columnas_existentes(df, columnas_heridos)
    limpio["fallecidos"] = sumar_columnas_existentes(df, columnas_fallecidos)

    limpio["anio"] = limpio["anio"].fillna(2024).astype(int)
    limpio["mes"] = limpio["mes"].fillna(1).clip(1, 12).astype(int)
    limpio["hora"] = limpio["hora"].fillna(0).clip(0, 23).astype(int)
    limpio["heridos"] = limpio["heridos"].fillna(0).clip(lower=0).astype(int)
    limpio["fallecidos"] = limpio["fallecidos"].fillna(0).clip(lower=0).astype(int)

    limpio["fecha"] = pd.to_datetime(
        limpio["anio"].astype(str) + "-" + limpio["mes"].astype(str).str.zfill(2) + "-01",
        errors="coerce"
    )

    for col in ["entidad", "municipio", "causa", "tipo_accidente"]:
        limpio[col] = limpio[col].astype(str).str.strip()
        limpio[col] = limpio[col].replace(["nan", "None", "", "NaN"], "No especificado")

    limpio = limpio[limpio["entidad"] != "No especificado"]
    limpio = limpio.dropna(subset=["fecha"])

    limpio = limpio[
        [
            "fecha", "anio", "mes", "hora", "entidad",
            "municipio", "causa", "tipo_accidente",
            "heridos", "fallecidos"
        ]
    ]

    return limpio


def leer_dataset_real_atus() -> pd.DataFrame | None:
    archivos = buscar_archivos_atus()

    if not archivos:
        return None

    print("Archivos ATUS encontrados:")
    for archivo in archivos:
        print(f" - {archivo}")

    base_atus = encontrar_base_atus()
    dataframes_limpios = []

    print("\nProcesando archivos anuales uno por uno para ahorrar memoria...")

    for i, archivo in enumerate(archivos, start=1):
        print(f"\n[{i}/{len(archivos)}] Leyendo: {archivo}")

        df_raw = leer_csv_seguro(archivo)
        
        # --- PARCHE DETECTOR DINÁMICO DE DESFASES DE COLUMNAS ---
        df_raw.columns = [normalizar_columna(c) for c in df_raw.columns]
        col_anio_header = obtener_columna(df_raw, ["anio", "año"])
        
        if col_anio_header and col_anio_header in df_raw.columns:
            idx_anio_header = df_raw.columns.get_loc(col_anio_header)
            idx_anio_real = -1
            
            # Analizamos las primeras 10 columnas buscando dónde están realmente los años (valores > 1990)
            for j, col in enumerate(df_raw.columns[:10]):
                valores = pd.to_numeric(df_raw[col], errors='coerce')
                if valores.notna().sum() > 0 and 1990 <= valores.mean() <= 2030:
                    idx_anio_real = j
                    break
            
            # Si encontramos el año real a la izquierda de la columna del header del año, hay desfase
            if idx_anio_real != -1 and idx_anio_real < idx_anio_header:
                desfase = idx_anio_header - idx_anio_real
                print(f"  [!] Desfase de {desfase} columna(s) detectado en archivo crudo. Corrigiendo headers...")
                # Desplazamos las etiquetas del header hacia la derecha agregando nombres fantasma al inicio
                nuevas_cols = df_raw.columns[desfase:].tolist() + [f"fantasma_{x}" for x in range(desfase)]
                df_raw.columns = nuevas_cols
        # --------------------------------------------------------

        print(f"Registros crudos del archivo: {len(df_raw):,}")

        df_limpio = limpiar_dataset_atus_real(df_raw, base_atus)

        print(f"Registros limpios del archivo: {len(df_limpio):,}")

        dataframes_limpios.append(df_limpio)

        del df_raw

    print("\nUniendo datasets limpios...")
    df_total = pd.concat(dataframes_limpios, ignore_index=True)

    del dataframes_limpios

    print(f"Registros reales limpios unidos: {len(df_total):,}")

    return df_total


def guardar_dataset(df: pd.DataFrame) -> None:
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    print(f"Guardando Parquet en: {OUTPUT_PARQUET}")
    df.to_parquet(OUTPUT_PARQUET, index=False)

    muestra_csv = os.path.join(PROCESSED_DIR, "atus_clean_sample.csv")

    print(f"Guardando muestra CSV en: {muestra_csv}")
    df.head(10000).to_csv(muestra_csv, index=False, encoding="utf-8")

    print("Dataset preparado correctamente.")
    print(f"Registros: {len(df):,}")
    print(f"Columnas: {list(df.columns)}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--simulate",
        action="store_true",
        help="Genera datos simulados aunque exista CSV real."
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=200_000,
        help="Cantidad de registros simulados."
    )

    args = parser.parse_args()

    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    if args.simulate:
        df = generar_datos_simulados(args.rows)
    else:
        df = leer_dataset_real_atus()

        if df is None:
            print("No se encontraron archivos reales de ATUS en data/raw/.")
            print("Se usarán datos simulados.")
            df = generar_datos_simulados(args.rows)

    guardar_dataset(df)


if __name__ == "__main__":
    main()