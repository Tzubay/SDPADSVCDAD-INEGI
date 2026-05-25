# Sistema distribuido para análisis de siniestros viales ATUS

Proyecto final de Cómputo Paralelo y Distribuido.

## Tecnologías

- Python
- Pandas
- Ray
- Ray Cluster
- Docker
- Docker Compose
- Streamlit
- Parquet

## Objetivo

Analizar datos de accidentes viales de México usando procesamiento secuencial y distribuido.

El sistema calcula:

- Total de accidentes
- Accidentes con heridos
- Accidentes con fallecidos
- Ranking por entidad federativa
- Ranking por municipio
- Accidentes por hora
- Causas más frecuentes
- Tipos de accidente
- Tendencia mensual
- Índice de gravedad
- Comparación Pandas vs Ray

## Estructura

```text
atus-ray-distribuido/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── README.md
├── data/
│   ├── raw/
│   └── processed/
├── results/
├── src/
│   ├── prepare_data.py
│   ├── sequential_analysis.py
│   ├── ray_analysis.py
│   └── benchmark.py
└── dashboard/
    └── app.py