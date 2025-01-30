# Proyecto2-DW-BdAA

Este proyecto implementa un proceso ETL para la construcción de un Data Warehouse.

## Requisitos Previos

### 1. Instalación de SQL Server
Para ejecutar el código, es necesario tener instalado **SQL Server**.
- Descarga e instalación: [SQL Server Downloads](https://www.microsoft.com/es-es/sql-server/sql-server-downloads)

### 2. Instalación de Python
El proceso ETL está desarrollado en **Python**, por lo que debes instalarlo previamente.
- Descarga e instalación: [Python Downloads](https://www.python.org/downloads/)

### 3. Instalación de Dependencias
Las siguientes bibliotecas de Python son necesarias para la ejecución del ETL:

```bash
pip install pandas sqlalchemy pyodbc
```

## Descarga de Datasets

Se requieren dos conjuntos de datos en formato CSV:

- **Dataset 1 (Reseñas de Steam 2021)**: [Descargar aquí](https://www.kaggle.com/datasets/najzeko/steam-reviews-2021/data)
- **Dataset 2 (Juegos de Steam)**: [Descargar aquí](https://www.kaggle.com/datasets/mexwell/steamgames)

Coloca los archivos descargados en una carpeta llamada **`Data`** dentro del directorio principal del proyecto.

## Ejecución del Código

Una vez instalados los requisitos y descargados los datasets, puedes ejecutar el código del proceso ETL.

```bash
python script_etl.py
```

## Visualización de Resultados
Para analizar gráficamente los datos procesados, se recomienda utilizar **Tableau Desktop**.
- Descarga e instalación: [Tableau Desktop](https://www.tableau.com/products/desktop/download)
