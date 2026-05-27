# Sistema Distribuido para Análisis de Siniestros Viales en México (1997 - 2024)

Este repositorio contiene la implementación de un sistema de ingeniería de datos a gran escala diseñado para procesar, limpiar y analizar el histórico completo de **Accidentes de Tránsito Terrestre en Zonas Urbanas y Suburbanas (ATUS)** de México, emitido por el INEGI. El sistema consolida y procesa **10,730,849 registros reales** abarcando 28 años de datos consecutivos.

El núcleo del proyecto radica en una arquitectura híbrida de cómputo, comparando el paradigma de procesamiento secuencial clásico contra un modelo de ejecución distribuida **MapReduce** montada sobre un clúster orquestado con **Ray** y **Docker**.

---

## 🛠️ Arquitectura del Sistema e Infraestructura

El entorno de ejecución está completamente contenedorizado y simula un entorno de producción distribuido con límites estrictos de hardware mediante **Docker Compose**:

```text
                                [ Clúster Ray ]
                         ┌───────────────────────────┐
                         │   ray-head (Master)       │ <─── [ Puerto 8265: Dashboard ]
                         │   Mem: 6GB | SHM: 2GB     │
                         └─────────────┬─────────────┘
                                       │
                ┌──────────────────────┼──────────────────────┐
                ▼                      ▼                      ▼
       ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
       │  ray-worker-1   │    │  ray-worker-2   │    │  ray-worker-3   │
       │ Mem: 6GB | SHM  │    │ Mem: 6GB | SHM  │    │ Mem: 6GB | SHM  │
       └─────────────────┘    └─────────────────┘    └─────────────────┘
```

* **Nodo Central (`ray-head`):** Coordina el clúster, gestiona el plan de ejecución y centraliza las tareas de reducción de datos. Expone el puerto `6379` para comunicación de red interna y el puerto `8265` para la interfaz de telemetría de Ray.
* **Nodos de Cómputo (`ray-worker-1, 2, 3`):** Tres workers independientes configurados para enlazarse dinámicamente al nodo central tras una latencia controlada de inicialización.
* **Aislamiento de Recursos:** Cada contenedor tiene un tope de hardware de **6 GB de RAM** y **8 GB de Swap**.
* **Optimización de Memoria Compartida (`shm_size: "2gb"`):** Configuración crítica asignada a los contenedores para evitar cuellos de botella en el Object Store de Ray (Plasma Memory), permitiendo la transferencia de particiones de DataFrames entre procesos sin costos de serialización repetitivos.
* **Capa de Presentación (`atus-dashboard`):** Un contenedor dedicado que levanta una interfaz interactiva en **Streamlit** (Puerto `8501`) para consumir el archivo maestro procesado.

---

## 📂 Estructura Detallada del Repositorio

```text
.
├── dashboard/
│   ├── app.py                      # Aplicación interactiva de Streamlit (Visualizaciones)
│   └── debug_proyecto.py           # Script de auditoría y detección de inconsistencias crudas
├── data/
│   ├── processed/
│   │   ├── atus_clean.parquet      # Dataset maestro unificado en almacenamiento columnar
│   │   └── atus_clean_sample.csv   # Muestra de validación del pipeline (10,000 filas)
│   └── raw/
│       ├── catalogos/              # Catálogos maestros del INEGI (tc_entidad.csv, tc_municipio.csv)
│       └── conjunto_de_datos/      # Repositorio de los 28 archivos CSV históricos (1997-2024)
├── results/                        # Artefactos y métricas de salida del sistema
│   ├── benchmark.csv               # Tabla comparativa de tiempos de ejecución
│   ├── ray_*.csv                   # Agregaciones generadas por el motor distribuido
│   └── sequential_*.csv            # Agregaciones generadas por el motor de Pandas
├── src/
│   ├── prepare_data.py             # ETL Pipeline: Extracción, Limpieza y Parser de Desfases
│   ├── sequential_analysis.py      # Motor analítico secuencial mono-hilo (Pandas)
│   ├── ray_analysis.py             # Motor analítico distribuido multiproceso (Ray)
│   └── benchmark.py                # Script de automatización de pruebas de carga
├── Dockerfile                      # Entorno base con dependencias del clúster
└── docker-compose.yml              # Orquestador de la topología de red y límites del clúster
```

---

## ⚡ Estrategias de Procesamiento de Datos

### 1. Limpieza y Parser Dinámico de Columnas (*ETL*)
A lo largo del histórico 1997-2024, el INEGI varió la estructura interna de sus archivos CSV. El pipeline implementa un algoritmo de **detección dinámica de desalineación columnar** (*Column Shifting*). El script inspecciona las primeras filas de cada archivo crudo de manera probabilística; si detecta que la columna identificada con el año real se encuentra recorrida a la izquierda respecto a la etiqueta del encabezado, el pipeline corrige dinámicamente las etiquetas reordenando los headers sobre la marcha, evitando la pérdida masiva de datos en 23 de los 28 archivos históricos.

### 2. Mapeo de Catálogos Binivel
Para la normalización de la geografía, se estructuró un cargador indexado por tuplas binivel `(id_entidad, id_municipio)`. Esto previene fallos de cruce por inconsistencia de tipos de datos en registros crudos antiguos (donde coexistían enteros, flotantes y nulos) y decodifica las categorías numéricas crudas en etiquetas semánticas inteligibles para el usuario final.

### 3. Modelo Analítico MapReduce (Ray)
El procesamiento distribuido en `ray-analysis.py` replica el flujo clásico de MapReduce sobre memoria:
* **Split:** El dataset maestro Parquet se lee en memoria y se segmenta en **12 sub-dataframes particionados** de manera contigua utilizando rebanado por índices (`.iloc`).
* **Map:** Las particiones se dispersan a los workers mediante la llamada a funciones remotas decoradas con `@ray.remote` (`analizar_particion`). Cada worker calcula de forma aislada las agregaciones elementales de vectores (agrupaciones por hora, causa, tipo de accidente, etc.) y computa de forma paralela la métrica compuesta del **Índice de Gravedad**:

$$Índice\ de\ Gravedad = Heridos + (Fallecidos \times 5)$$

* **Reduce:** El nodo central recolecta las promesas de datos (*Futures*) usando `ray.get()`, concatena las respuestas parciales y ejecuta una reducción final a través de agregaciones agregadas (`.groupby().sum()`) antes de persistir los resultados en disco.

---

## 📈 Análisis de Rendimiento y Paradoja Distribuida

El pipeline expone una comparativa detallada entre la ejecución puramente secuencial y la distribuida:

| Métrica de Rendimiento | Pandas Secuencial | Ray Distribuido (12 Particiones) |
| :--- | :--- | :--- |
| **Tiempo de Procesamiento** | **~6.53 segundos** | **~8.86 segundos** |
| **Speedup Resultante** | *1.00x (Base)* | **0.73x** |

### Análisis de Ingeniería
Los resultados demuestran de manera empírica uno de los axiomas más importantes del cómputo paralelo: **"Más nodos no significan menor tiempo si el costo de comunicación domina al de cómputo"**.

Debido a que el dataset maestro está almacenado bajo un formato altamente optimizado como **Apache Parquet**, Pandas puede ejecutar operaciones vectoriales directas en memoria RAM de manera ultra veloz. Al introducir Ray para consultas de agregación aritméticas simples, el sistema experimenta un **overhead de coordinación**: el tiempo invertido en fragmentar el DataFrame, serializar las particiones a través de la red hacia los contenedores de los workers, coordinar los hilos del scheduler y serializar de vuelta los resultados intermedios al nodo maestro termina superando el tiempo de procesamiento en un solo hilo local.

---

## 🚀 Guía de Despliegue y Ejecución

### Requisitos Previos
* Docker y Docker Compose instalados en el sistema operativo anfitrión.

### 1. Orquestar el Clúster
Levanta la infraestructura completa (Master, Workers y Dashboard) en segundo plano:
```bash
docker-compose up -d --build
```

### 2. Ejecutar el Pipeline de Preparación de Datos (ETL)
Accede al contenedor principal (`ray-head`) e inicia el procesamiento de la data histórica cruda:
```bash
docker exec -it ray-head bash
python src/prepare_data.py
```
*Este comando consolidará los 10.7 millones de registros del INEGI en un archivo maestro columnar unificado en `data/processed/atus_clean.parquet`.*

### 3. Ejecutar las Pruebas de Carga y Benchmark
Para calcular las métricas analíticas tanto en el motor secuencial como en el distribuido, y evaluar el desempeño del sistema, ejecuta:
```bash
python src/benchmark.py
```
Los resultados finales consolidados se guardarán automáticamente en la carpeta `results/`.

### 4. Acceder a las Interfaces Gráficas
* **Telemetría del Clúster Ray:** Abre en tu navegador `http://localhost:8265` para monitorear el estado de los recursos de hardware, la cola de tareas de los workers y el Object Store.
* **Dashboard de Analítica Interactiva:** Abre en tu navegador `http://localhost:8501` para explorar mapas de calor, series de tiempo mensuales y rankings de siniestralidad urbana generados mediante Streamlit.
