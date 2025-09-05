<div id="top" align="center">
  <img src="https://media.giphy.com/media/LmN8EsTEjS8LgtxP9X/giphy.gif" alt="Data Transformation GIF" width="100"/>
  <h1>Proyecto ETL: Automatización de Constancias de Entrenamiento y Preparación de Datos RRHH</h1>
  <h3>Un pipeline ETL robusto para transformar datos de entrenamiento y recursos humanos en insights accionables.</h3>
</div>

<br>

## 📄 Tabla de Contenidos

*   [Acerca del Proyecto](#acerca-del-proyecto)
*   [El Problema](#el-problema)
*   [La Solución: Nuestro Pipeline ETL](#la-solución-nuestro-pipeline-etl)
    *   [1. Identificación y Filtro de Archivos (`generador_lista_no_excluidos.py`)](#1-identificación-y-filtro-de-archivos-generador_lista_no_excluidospy)
    *   [2. Extracción, Transformación y Carga de Constancias (`etl_pdf_entrenamiento.py`)](#2-extracción-transformación-y-carga-de-constancias-etl_pdf_entrenamientopy)
    *   [3. Preparación de Tablas Maestras para Dashboards (`etl_bd_hc.py`)](#3-preparación-de-tablas-maestras-para-dashboards-etl_bd_hcpy)
*   [Tecnologías Utilizadas](#tecnologías-utilizadas)
*   [Estado Actual y Futuro](#estado-actual-y-futuro)
*   [Estructura del Proyecto](#estructura-del-proyecto)
*   [Contribución](#contribución)
*   [Contacto](#contacto)

---

## 🎯 Acerca del Proyecto

Este proyecto implementa un pipeline ETL (Extracción, Transformación y Carga) completo diseñado para automatizar el procesamiento de constancias de entrenamiento en formato PDF y la consolidación de diversas fuentes de datos de Recursos Humanos. El objetivo final es generar información limpia, estructurada y lista para ser consumida en dashboards interactivos, facilitando la toma de decisiones estratégicas en áreas como la gestión del talento, ausentismo y cobertura operacional.

## ⚠️ El Problema

Tradicionalmente, el manejo de constancias de entrenamiento implicaba tareas manuales repetitivas y propensas a errores, como:
*   Búsqueda manual de nuevos archivos PDF en múltiples ubicaciones.
*   Filtrado manual de constancias irrelevantes o duplicadas.
*   Extracción manual de datos clave (nombre, curso, fecha, instructor, grupo) de cada PDF.
*   Consolidación de esta información con datos de empleados.
*   Organización física de los PDFs en carpetas específicas.
*   Preparación y limpieza de diversas bases de datos de RRHH (maestro HC, bajas, asistencia, cobertura).

Estos procesos consumían mucho tiempo y recursos, impidiendo una visión ágil y precisa del estado del entrenamiento y el capital humano.

## ✅ La Solución: Nuestro Pipeline ETL

Nuestro proyecto aborda estos desafíos a través de una arquitectura modular compuesta por tres scripts principales que trabajan en conjunto para automatizar todo el flujo de datos.

### 1. Identificación y Filtro de Archivos (`generador_lista_no_excluidos.py`)

Este script actúa como la **primera fase de descubrimiento**. Su función es escanear recursivamente las carpetas fuente definidas, aplicando un conjunto de reglas de exclusión para directorios y archivos PDF.
*   **Escaneo Inteligente:** Recorre las carpetas fuente buscando archivos PDF.
*   **Reglas de Exclusión:** Filtra archivos y directorios basándose en prefijos, sufijos, años no vigentes en el nombre del archivo y la fecha de última modificación (ej. excluyendo archivos anteriores a 2024).
*   **Detección de Archivos Procesados:** Utiliza un log de `registro_archivos_procesados.txt` para identificar y saltar archivos que ya fueron procesados previamente, asegurando que solo se trabajen con "archivos nuevos no excluidos".
*   **Identificación de PDFs Agrupados:** Determina si un PDF es "agrupado" (múltiples páginas, indicando varias constancias en un solo archivo) o "standalone" (una constancia por archivo).
*   **Output:** Genera un archivo `lista_pdfs_nuevos_no_excluidos.txt` que contiene las rutas de los PDFs que necesitan ser procesados, junto con un flag indicando si son agrupados o individuales.

### 2. Extracción, Transformación y Carga de Constancias (`etl_pdf_entrenamiento.py`)

Este es el **corazón del proceso ETL de constancias**. Toma la lista generada por el script anterior y realiza la extracción detallada y la transformación de los datos.
*   **Gestión de Configuración:** Utiliza una clase `Config` para centralizar todas las rutas, patrones y parámetros, facilitando la mantenibilidad.
*   **Manejo de PDFs Agrupados:** Divide automáticamente los PDFs agrupados en archivos temporales individuales, procesando cada constancia de forma independiente.
*   **Extracción de Datos Avanzada:** Emplea expresiones regulares (`re`) y la librería `PyMuPDF (fitz)` para extraer de forma robusta el nombre del empleado, curso, fecha, instructor y grupo de diferentes formatos de constancias (SAT, SMS, AVSEC).
*   **Normalización y Homologación:** Limpia y normaliza los nombres de los empleados, cursos e instructores (ej. eliminando acentos, espacios extra), y **homologa** los nombres de los cursos a categorías estándar (ej. "SAT(Rampa)", "AVSEC", "SMS").
*   **Parseo de Fechas:** Extrae y normaliza las fechas de los cursos, incluso manejando diferentes formatos y rangos, para calcular la fecha de vigencia y asignar un `estatus_vigencia` (Vigente/Vencido).
*   **Integración con HC:** Realiza un proceso de **doble merge** con una tabla maestra de empleados (HC) para asociar cada constancia a un número de empleado (`#emp`) y su estatus. Se implementan estrategias de coincidencia robustas para nombres.
*   **Filtrado de Negocio:** Aplica reglas de negocio para descartar constancias específicas (ej. por instructor, nombre de archivo, prefijos de grupo) o eliminar duplicados.
*   **Generación de Nombres Estándar:** Crea nombres de archivo estandarizados para las constancias procesadas (ej. `CURSO_DD-MM-YYYY_NOMBRE_COMPLETO.pdf`).
*   **Organización Automática de Archivos:** Copia los PDFs procesados a una estructura de carpetas `[Número de Empleado]`, distinguiendo entre empleados activos y aquellos con estatus de `BAJA` (enviándolos a una subcarpeta específica).
*   **Reporte de No Coincidencias:** Identifica y exporta las constancias que no pudieron ser asociadas a un número de empleado, facilitando la revisión manual.
*   **Output:** Exporta el historial consolidado de constancias a archivos `datos_constancias.xlsx` y `datos_constancias.csv` con formato. Mantiene actualizado el `registro_archivos_procesados.txt`.

### 3. Preparación de Tablas Maestras para Dashboards (`etl_bd_hc.py`)

Este script se encarga de procesar y estructurar diversas fuentes de datos de Recursos Humanos, generando tablas limpias y desnormalizadas, listas para ser consumidas directamente por un dashboard de inteligencia de negocios.
*   **Carga y Limpieza Genérica:** Utiliza funciones genéricas para cargar y limpiar datos de archivos Excel y CSV, normalizando nombres de columnas y eliminando duplicados.
*   **Procesamiento de Datos Maestros:**
    *   **`hc_table`**: Carga y limpia la base de datos maestra de capital humano, creando una columna de `nombre_completo` estandarizada y normalizando campos como IDs y fechas.
    *   **`hc_bajas_table`**: Procesa los registros de empleados dados de baja.
    *   **`puestos_table`**: Crea una tabla de dimensiones para cargos/puestos homologados, incluyendo detalles como área y horas diarias.
    *   **`cursos_table`**: Genera una tabla de dimensiones para los cursos de entrenamiento.
    *   **`asistencia_table`**: Consolida los registros de asistencia a entrenamientos.
    *   **`ausentismo_table`**: Procesa los datos de faltas y ausentismo del reloj checador.
    *   **`cobertura_table`**: Prepara los datos relacionados con la cobertura de personal y requerimientos de puestos.
*   **Integración de Datos:** Realiza merges clave para enriquecer las tablas (ej. uniendo el maestro HC con los puestos homologados).
*   **Output:** Exporta múltiples archivos CSV a la carpeta `data\processed\dashboard_tables`, listos para ser conectados a herramientas como Power BI o Tableau.

## 🛠️ Tecnologías Utilizadas

*   **Python 3.x:** Lenguaje principal de desarrollo.
*   **Pandas:** Para manipulación y análisis de datos en DataFrames.
*   **PyMuPDF (fitz):** Para extracción eficiente de texto de documentos PDF.
*   **`re` (Regular Expressions):** Para patrones de búsqueda avanzados y extracción de datos en texto.
*   **`os`, `shutil`, `datetime`, `unicodedata`, `numpy`:** Módulos estándar de Python para operaciones de sistema, fechas y numéricas.
*   **XlsxWriter (a través de `pandas.ExcelWriter`):** Para la exportación de DataFrames a Excel con formato personalizado.

<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python Badge"/>
  <img src="https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white" alt="Pandas Badge"/>
  <img src="https://img.shields.io/badge/PyMuPDF-000000?style=for-the-badge&logo=pdf&logoColor=white" alt="PyMuPDF Badge"/>
  <img src="https://img.shields.io/badge/Regular%20Expressions-FF9900?style=for-the-badge&logo=regex&logoColor=white" alt="Regex Badge"/>
  <img src="https://img.shields.io/badge/Git-F05032?style=for-the-badge&logo=git&logoColor=white" alt="Git Badge"/>
</p>

## 🚀 Estado Actual y Futuro

El proyecto se encuentra en una **fase avanzada de proceso y refinamiento**. Actualmente, los scripts son funcionales y demuestran la capacidad de automatizar de manera efectiva la extracción, transformación y carga de datos.

**Próximos Pasos:**
*   **Empaquetamiento:** Se planea empaquetar el proyecto para facilitar su despliegue y uso en diferentes entornos.
*   **Interfaz Gráfica de Usuario (GUI):** La futura implementación de una GUI permitirá a usuarios no técnicos interactuar con el pipeline de forma intuitiva, facilitando la configuración de rutas y la ejecución de los procesos con solo unos clics. Esto mejorará significativamente la usabilidad y accesibilidad del sistema.

## 📁 Estructura del Proyecto
* .
* ├── data/
* │ ├── processed/
* │ │ ├── dashboard_tables/ # Tablas limpias para dashboards (CSV)
* │ │ ├── Certificados Entrenamiento Viva Handling/ # PDFs organizados por empleado
* │ │ │ ├── 0/ # Constancias sin #emp asignado
* │ │ │ ├── 0/ # Constancias sin #emp asignado
* │ │ │ ├── 12345/ # Ejemplo: Carpeta de empleado 12345 (activos)
* │ │ │ └── 1. BAJAS/ # Constancias de empleados dados de baja
* │ │ │ └── 54321/ # Ejemplo: Carpeta de empleado 54321 (baja)
* │ │ ├── temp_split_pdfs/ # PDFs temporales generados al dividir agrupados
* │ │ ├── datos_constancias.xlsx
* │ │ ├── datos_constancias.csv
* │ │ ├── datos_constancias_sin_emp.xlsx
* │ │ ├── datos_constancias_sin_emp.csv
* │ │ └── registro_archivos_procesados.txt
* │ └── raw/ # Fuentes de datos originales
* ├── etl_bd_hc.py # Script para la preparación de tablas de HC para dashboards
* ├── etl_pdf_entrenamiento.py # Script principal ETL de constancias PDF
* ├── generador_lista_no_excluidos.py # Script para identificar y filtrar nuevos PDFs
* └── README.md

## 🤝 Contribución

Actualmente, el proyecto se mantiene de forma individual. Si estás interesado en contribuir o tienes sugerencias, no dudes en contactarme.

## 📞 Contacto

Puedes conectar conmigo a través de mi perfil de LinkedIn:

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/bryan-betancur-420103255/)