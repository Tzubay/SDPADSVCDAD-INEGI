import os
import json
import time

import pandas as pd

from sequential_analysis import cargar_datos, ejecutar_analisis_secuencial, guardar_resultados
from ray_analysis import ejecutar_ray


RESULTS_DIR = "results"
BENCHMARK_FILE = os.path.join(RESULTS_DIR, "benchmark.csv")


def main():
    os.makedirs(RESULTS_DIR, exist_ok=True)

    print("==============================================")
    print("BENCHMARK: Pandas secuencial vs Ray distribuido")
    print("==============================================")

    print("\n[1/2] Ejecutando Pandas secuencial...")
    inicio_seq = time.perf_counter()

    df = cargar_datos()
    resultados_seq = ejecutar_analisis_secuencial(df)

    fin_seq = time.perf_counter()
    tiempo_seq = fin_seq - inicio_seq

    guardar_resultados(resultados_seq, tiempo_seq)

    print(f"Tiempo secuencial: {tiempo_seq:.4f} segundos")

    print("\n[2/2] Ejecutando Ray distribuido...")
    resultados_ray, tiempo_ray = ejecutar_ray(num_particiones=12)

    print(f"Tiempo Ray: {tiempo_ray:.4f} segundos")

    if tiempo_ray > 0:
        speedup = tiempo_seq / tiempo_ray
    else:
        speedup = 0

    benchmark = pd.DataFrame([
        {
            "metodo": "Pandas secuencial",
            "tiempo_segundos": tiempo_seq,
            "speedup": 1.0
        },
        {
            "metodo": "Ray distribuido",
            "tiempo_segundos": tiempo_ray,
            "speedup": speedup
        }
    ])

    benchmark.to_csv(BENCHMARK_FILE, index=False, encoding="utf-8")

    resumen = {
        "tiempo_secuencial": tiempo_seq,
        "tiempo_ray": tiempo_ray,
        "speedup": speedup,
        "interpretacion": (
            "Un speedup mayor a 1 indica que Ray fue más rápido. "
            "Un speedup menor a 1 puede ocurrir con datasets pequeños debido "
            "al costo de coordinar tareas distribuidas."
        )
    }

    with open(os.path.join(RESULTS_DIR, "benchmark_summary.json"), "w", encoding="utf-8") as f:
        json.dump(resumen, f, indent=4, ensure_ascii=False)

    print("\n==============================================")
    print("RESULTADO DEL BENCHMARK")
    print("==============================================")
    print(f"Tiempo Pandas secuencial: {tiempo_seq:.4f} s")
    print(f"Tiempo Ray distribuido:   {tiempo_ray:.4f} s")
    print(f"Speedup:                  {speedup:.4f}x")
    print("\nArchivos generados en carpeta results/")


if __name__ == "__main__":
    main()