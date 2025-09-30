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
    *   [Visión General de la Configuración Centralizada](#visión-general-de-la-configuración-centralizada)
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

Nuestro proyecto aborda estos desafíos a través de una arquitectura modular compuesta por tres scripts principales que trabajan en conjunto para automatizar todo el flujo de datos, orquestados por un script `main.py`.

### Visión General de la Configuración Centralizada

Una mejora fundamental en este pipeline es la introducción de la clase `Config`. Esta clase centraliza **todas las rutas de archivos, nombres de carpetas, nombres de hojas de cálculo, reglas de exclusión y mapeos de texto** utilizados en los diferentes scripts ETL. Al cargar una única instancia de `Config` al inicio del proceso, se logra:
*   **Mantenibilidad:** Facilita la actualización de rutas y parámetros en un solo lugar.
*   **Consistencia:** Asegura que todos los scripts utilicen los mismos valores de configuración.
*   **Robustez:** Simplifica la gestión de dependencias y evita la dispersión de la lógica de configuración.
*   **Preparación del Entorno:** Se encarga de crear las carpetas de salida necesarias, tanto locales como en OneDrive, para asegurar que el pipeline pueda almacenar sus resultados sin problemas.

Cada función del pipeline ahora recibe el objeto `Config`, lo que garantiza que operen con un conjunto coherente y actualizado de parámetros.

### 1. Identificación y Filtro de Archivos (`generador_lista_no_excluidos.py`)

Este script actúa como la **primera fase de descubrimiento**. Su función es escanear recursivamente las carpetas fuente definidas (a través del objeto `Config`), aplicando un conjunto de reglas de exclusión para directorios y archivos PDF.
*   **Escaneo Inteligente:** Recorre las `source_folders_pdfs` buscando archivos PDF.
*   **Reglas de Exclusión:** Filtra archivos y directorios basándose en `excluded_prefixes`, `excluded_suffixes`, `non_vigentes_years_in_filename` (años no vigentes en el nombre del archivo) y la fecha de última modificación (`min_mod_year`, ej. excluyendo archivos anteriores a 2024).
*   **Detección de Archivos Procesados:** Utiliza un log persistente (`registro_archivos_procesados.txt`, gestionado en memoria por `processed_files_set_in_memory` en `Config`) para identificar y saltar archivos que ya fueron procesados previamente, asegurando que solo se trabajen con "archivos nuevos no excluidos".
*   **Identificación de PDFs Agrupados:** Determina si un PDF es "agrupado" (múltiples páginas, indicando varias constancias en un solo archivo) o "standalone" (una constancia por archivo).
*   **Output:** Genera un archivo `lista_pdfs_nuevos_no_excluidos.txt` que contiene las rutas de los PDFs que necesitan ser procesados, junto con un flag indicando si son agrupados o individuales.

### 2. Extracción, Transformación y Carga de Constancias (`etl_pdf_entrenamiento.py`)

Este es el **corazón del proceso ETL de constancias**. Toma la lista generada por el script anterior y realiza la extracción detallada y la transformación de los datos, utilizando el objeto `Config` para todos sus parámetros internos.
*   **Gestión de Carpetas de Bajas (`mover_carpetas_bajas`):** Una nueva funcionalidad clave es la identificación y movimiento automático de carpetas de empleados con estatus 'BAJA' (según el `hc_table.csv`) desde la ruta de certificados activos (`onedrive_certs_active`) a una subcarpeta de bajas (`onedrive_certs_bajas`). Esto asegura una organización de archivos limpia y evita el procesamiento innecesario de certificados de personal inactivo. Se incluye una robusta función `rmtree_onerror_retry` para manejar errores de permisos al eliminar carpetas en el destino.
*   **Manejo de PDFs Agrupados:** Divide automáticamente los PDFs agrupados en archivos temporales individuales, procesando cada constancia de forma independiente. La lógica de división ha sido mejorada para omitir páginas que no contienen certificados válidos, optimizando el procesamiento.
*   **Extracción de Datos Avanzada (`extraer_datos_constancia`):** Emplea expresiones regulares (`re`) y la librería `PyMuPDF (fitz)` para extraer de forma robusta el nombre del empleado, curso, fecha, instructor y grupo de diferentes formatos de constancias (determinados por `nombres_archivos_sat`, `nombres_archivos_sms`, `nombres_archivos_avsec`).
*   **Normalización y Homologación (`normalizar_acentos`, `homologar_curso`):** Limpia y normaliza los nombres de los empleados, cursos e instructores (ej. eliminando acentos usando `vocales_acentos`, espacios extra), y **homologa** los nombres de los cursos a categorías estándar (ej. "SAT(Rampa)", "AVSEC", "SMS").
*   **Parseo de Fechas (`parse_fecha_inicio`):** Extrae y normaliza las fechas de los cursos, incluso manejando diferentes formatos y rangos (usando `mapeo_meses`), para calcular la fecha de vigencia y asignar un `estatus_vigencia` (Vigente/Vencido).
*   **Integración con HC (`procesar_y_mergear_constancias`):** Realiza un proceso de **doble merge** con una tabla maestra de empleados (cargada con `cargar_data_hc` desde `hc_table_path`) para asociar cada constancia a un número de empleado (`#emp`) y su estatus. Se implementan estrategias de coincidencia robustas para nombres, intentando múltiples formatos para maximizar las coincidencias.
*   **Filtrado de Negocio:** Aplica reglas de negocio para descartar constancias específicas (ej. por instructor, nombre de archivo, prefijos de grupo) o eliminar duplicados, garantizando la calidad de los datos finales.
*   **Generación de Nombres Estándar:** Crea nombres de archivo estandarizados y limpios (`nombre_archivo_nuevo`) para las constancias procesadas (ej. `CURSO_DD-MM-YYYY_NOMBRE_COMPLETO.pdf`).
*   **Organización Automática de Archivos (`organizar_archivos_pdf`):** Copia los PDFs procesados a una estructura de carpetas `[Número de Empleado]` dentro de `onedrive_certs_active` o `onedrive_certs_bajas`, según el estatus del empleado. El `original_source_path` del archivo fuente original (sea agrupado o standalone) se registra para evitar futuros reprocesamientos.
*   **Reporte de No Coincidencias (`identificar_y_reportar_constancias_sin_coincidencia`):** Identifica y exporta las constancias que no pudieron ser asociadas a un número de empleado a archivos específicos (`datos_constancias_sin_emp.xlsx` y `datos_constancias_sin_emp.csv`), facilitando la revisión manual.
*   **Intelligent Export/Consolidation (`exportar_resultados`):** Esta función ha sido mejorada para cargar datos existentes (`datos_constancias.xlsx` y `datos_constancias.csv`), concatenar los nuevos registros, y luego **eliminar duplicados** basándose en un subconjunto de columnas clave. Esto asegura que los archivos de salida estén siempre actualizados, contengan un historial completo y estén libres de entradas redundantes, incluso después de múltiples ejecuciones. Incluye formato avanzado para Excel.
*   **Output:** Exporta el historial consolidado de constancias a archivos `datos_constancias.xlsx` y `datos_constancias.csv` con formato. Mantiene actualizado el `registro_archivos_procesados.txt`. Realiza limpieza de la carpeta de PDFs temporales (`temp_split_pdfs_folder`) al inicio y al final de la ejecución.

### 3. Preparación de Tablas Maestras para Dashboards (`etl_bd_hc.py`)

Este script se encarga de procesar y estructurar diversas fuentes de datos de Recursos Humanos, generando tablas limpias y desnormalizadas, listas para ser consumidas directamente por un dashboard de inteligencia de negocios. Al igual que los otros scripts, utiliza el objeto `Config` para acceder a todas las rutas de archivos, nombres de hojas de cálculo (`hc_etl_sheets_names`) y mapeos de acentos (`vocales_acentos`).
*   **Carga y Limpieza Genérica:** Utiliza funciones genéricas (`cargar_transformar_excel`, `cargar_transformar_csv`) para cargar y limpiar datos de archivos Excel y CSV, normalizando nombres de columnas y eliminando duplicados. Incluye funciones específicas para `limpiar_columna_texto`, `limpiar_columna_id` y `limpiar_columna_fecha`.
*   **Procesamiento de Datos Maestros:**
    *   **`hc_table`**: Carga y limpia la base de datos maestra de capital humano desde `FILE_MAESTRO_HC`, creando una columna de `nombre_completo` estandarizada y normalizando campos como IDs y fechas.
    *   **`hc_bajas_table`**: Procesa los registros de empleados dados de baja desde la hoja 'BAJAS_HC'.
    *   **`puestos_table`**: Crea una tabla de dimensiones para cargos/puestos homologados (desde `FILE_PUESTOS`), incluyendo detalles como área y horas diarias.
    *   **`cursos_table`**: Genera una tabla de dimensiones para los cursos de entrenamiento a partir de `FILE_ENTRENAMIENTO`.
    *   **`asistencia_table`**: Consolida los registros de asistencia a entrenamientos a partir de la hoja 'Programacion' de `FILE_ENTRENAMIENTO`.
    *   **`ausentismo_table`**: Procesa los datos de faltas y ausentismo del reloj checador, iterando sobre los archivos CSV en `FOLDER_RELOJ_CHECADOR`.
    *   **`cobertura_table`**: Prepara los datos relacionados con la cobertura de personal y requerimientos de puestos desde `FILE_COBERTURA`.
*   **Integración de Datos:** Realiza merges clave para enriquecer las tablas (ej. uniendo el maestro HC con los datos adicionales y los puestos homologados).
*   **Output:** Exporta múltiples archivos CSV a la carpeta `dashboard_tables_folder` (definida en `Config`), listos para ser conectados a herramientas como Power BI o Tableau, siguiendo los nombres de archivo especificados en `hc_etl_out_filenames`.

## 🛠️ Tecnologías Utilizadas

*   **Python 3.x:** Lenguaje principal de desarrollo.
*   **Pandas:** Para manipulación y análisis de datos en DataFrames.
*   **PyMuPDF (fitz):** Para extracción eficiente de texto de documentos PDF.
*   **`re` (Regular Expressions):** Para patrones de búsqueda avanzados y extracción de datos en texto.
*   **`os`, `shutil`, `datetime`, `unicodedata`, `numpy`, `stat`:** Módulos estándar de Python para operaciones de sistema, manejo de archivos/carpetas (incluyendo cambios de permisos), fechas y numéricas.
*   **XlsxWriter (a través de `pandas.ExcelWriter`):** Para la exportación de DataFrames a Excel con formato personalizado (encabezados, anchos de columna, autofiltros).

<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python Badge"/>
  <img src="https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white" alt="Pandas Badge"/>
  <img src="https://img.shields.io/badge/PyMuPDF-000000?style=for-the-badge&logo=pdf&logoColor=white" alt="PyMuPDF Badge"/>
  <img src="https://img.shields.io/badge/Regular%20Expressions-FF9900?style=for-the-badge&logo=regex&logoColor=white" alt="Regex Badge"/>
  <img src="https://img.shields.io/badge/Git-F05032?style=for-the-badge&logo=git&logoColor=white" alt="Git Badge"/>
</p>

## 🚀 Estado Actual y Futuro

El proyecto se encuentra en una **fase avanzada de proceso y refinamiento**. Actualmente, los scripts son funcionales y demuestran la capacidad de automatizar de manera efectiva la extracción, transformación y carga de datos. Las recientes mejoras en la configuración centralizada, el manejo de archivos de bajas, la deduplicación inteligente de datos y el reporte detallado han aumentado significativamente su robustez y fiabilidad.

**Próximos Pasos:**
*   **Empaquetamiento:** Se planea empaquetar el proyecto para facilitar su despliegue y uso en diferentes entornos.
*   **Interfaz Gráfica de Usuario (GUI):** La futura implementación de una GUI permitirá a usuarios no técnicos interactuar con el pipeline de forma intuitiva, facilitando la configuración de rutas y la ejecución de los procesos con solo unos clics. Esto mejorará significativamente la usabilidad y accesibilidad del sistema.

## 📁 Estructura del Proyecto
* 
* ├── data/
* │ ├── processed/
* │ │ ├── dashboard_tables/ # Tablas limpias para dashboards (CSV)
* │ │ │ ├── fact_table.csv
* │ │ │ ├── hc_table.csv
* │ │ │ ├── hc_bajas_table.csv
* │ │ │ ├── puestos_table.csv
* │ │ │ ├── cursos_table.csv
* │ │ │ ├── asistencia_table.csv
* │ │ │ ├── ausentismo_table.csv
* │ │ │ └── cobertura_table.csv
* │ │ ├── temp_split_pdfs/ # PDFs temporales generados al dividir agrupados (limpiada en cada ejecución)
* │ │ ├── datos_constancias.xlsx # Historial consolidado de constancias
* │ │ ├── datos_constancias.csv # Historial consolidado de constancias
* │ │ ├── datos_constancias_sin_emp.xlsx # Constancias sin #emp asignado (para revisión)
* │ │ ├── datos_constancias_sin_emp.csv # Constancias sin #emp asignado (para revisión)
* │ │ └── registro_archivos_procesados.txt # Log de archivos fuente procesados
* │ └── raw/ # Fuentes de datos originales (no generada por el script)
* ├── src/
* │ ├── config.py # Clase de configuración centralizada
* │ ├── etl_bd_hc.py # Script para la preparación de tablas de HC para dashboards
* │ ├── etl_pdf_entrenamiento.py # Script principal ETL de constancias PDF
* │ ├── generador_lista_no_excluidos.py # Script para identificar y filtrar nuevos PDFs
* │ └── init.py # Archivo de inicialización del paquete src
* ├── main.py # Orquestador principal del pipeline ETL
* └── README.md

## 🤝 Contribución

Actualmente, el proyecto se mantiene de forma individual. Si estás interesado en contribuir o tienes sugerencias, no dudes en contactarme.

## 📞 Contacto

Puedes conectar conmigo a través de mi perfil de LinkedIn:

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/bryan-betancur-420103255/)