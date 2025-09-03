import os # manejo de paths
import fitz # manejo y extraccion de texto(pdf)
import re # uso de regular expretion
import pandas as pd
from datetime import datetime # uso de tiempos
import shutil # copiar archivos / pegar archivos
import unicodedata # manejo y transformacion de tipo de texto
import numpy as np

class Config:
    """
    Clase para centralizar y gestionar todas las configuraciones y rutas ETL. 
    """
    def __init__(self):
        """
        Inicializa los atributos de configuracion
        """
        # Nombres de outfiles
        self.nombre_archivo_xlsx = 'datos_constancias.xlsx'
        self.nombre_archivo_csv = 'datos_constancias.csv'
        self.nombre_archivo_registro_archivos_procesados = 'registro_archivos_procesados.txt'
        # Nombre del archivo que contendrá las rutas de los PDFs NUEVOS no excluidos
        self.nombre_archivo_lista_pdfs_nuevos_no_excluidos = 'lista_pdfs_nuevos_no_excluidos.txt'

        # Nombre de outfolder Constancias(pdf's)
        self.NOMBRE_FOLDER_CONSTANCIAS = 'Certificados Entrenamiento Viva Handling'
        self.NOMBRE_FOLDER_ONEDRIVE_CONSTANCIAS = '2.Constancias_actual'
        self.NOMBRE_FOLDER_CONSTANCIAS_BAJAS = 'BAJAS'

        # Folders paths
        self.folder_data_processed = r'.\data\processed'
        self.folder_data_processed_dashboard = r'.\data\processed\dashboard_tables'
        self.folder_data_raw = r'.\data\raw'
        self.folder_archivos_compartidos = r'C:\Users\bryan.betancur\OneDrive - Vivaaerobus\archivos_compartidos\Certificados Entrenamiento Viva Handling - Certficados'

        # Files paths
        self.file_hc_table = os.path.join(self.folder_data_processed_dashboard, 'hc_table.csv')
        self.file_lista_pdfs_nuevos_no_excluidos = os.path.join(self.folder_data_processed, self.nombre_archivo_lista_pdfs_nuevos_no_excluidos)

        # Outpaths
        self.outpath_processed_files_log = os.path.join(self.folder_data_processed, self.nombre_archivo_registro_archivos_procesados)
        self.outpath_xlsx = os.path.join(self.folder_data_processed, self.nombre_archivo_xlsx)
        self.outpath_csv = os.path.join(self.folder_data_processed, self.nombre_archivo_csv)
        self.outpath_constancias_pdfs = os.path.join(self.folder_data_processed, self.NOMBRE_FOLDER_CONSTANCIAS)
        self.outpath_onedrive_constancias_pdfs = os.path.join(self.folder_archivos_compartidos, self.NOMBRE_FOLDER_ONEDRIVE_CONSTANCIAS)
        self.outpath_constancias_bajas_pdfs = os.path.join(self.outpath_constancias_pdfs, self.NOMBRE_FOLDER_CONSTANCIAS_BAJAS)
        self.outpath_onedrive_constancias_bajas_pdfs = os.path.join(self.folder_archivos_compartidos, self.NOMBRE_FOLDER_ONEDRIVE_CONSTANCIAS)

        # Textos a buscar dentro de cada archivo para identificar el tipo de constancia
        self.nombres_archivos_sat = ['instructor sat', '2025-T', 'apoyo en tierra', 'sat.']
        self.nombres_archivos_avsec = ['AVSEC', 'AVSEC-2024', 'AVSEC-2025', 'AVSE ', 'seguridad de la aviación', 'seguridad de la aviacion']
        self.nombres_archivos_sms = ['SMS', 'SAFETY MANAGEMENT SYSTEM']

        # Mapeo de vocales con acento o sin acento para normalización
        self.vocales_acentos = {
            'á': 'a', 'Á': 'A', 'é': 'e', 'É': 'E', 'í': 'i', 'Í': 'I',
            'ó': 'o', 'Ó': 'O', 'ú': 'u', 'Ú': 'U'
        }

        # Mapeo de meses abreviado y completo
        self.mapeo_meses = {
            'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4, 'may': 5, 'jun': 6,
            'jul': 7, 'ago': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12,
            'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'junio': 6,
            'julio': 7, 'agosto': 8, 'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12,
            'jaan': 1, # Caso específico "Jaan"
        }

        # Atributo para el set de archivos procesados en memoria
        self.processed_files_set_in_memory = set()

        self.temp_split_pdfs_folder = os.path.join(self.folder_data_processed, 'temp_split_pdfs') # pdf temporales

        # Asegurar que las carpetas existan
        # Rutas definidas
        self._create_output_folders()

        # Script: 'generador_lista_no_excluidos.py'
        #  Rutas de carpetas fuente
        self.carpetas_fuente = [
            # r"C:\Users\bryan.betancur\Projects\Viva-handling\data\raw\prueba",
            r"C:\Users\bryan.betancur\OneDrive - Vivaaerobus\archivos_compartidos\Certificados Entrenamiento Viva Handling - Certficados\1.Constancias_agrupadas",
            r"C:\Users\bryan.betancur\OneDrive - Vivaaerobus\archivos_compartidos\Capacitación SAT Pronomina MTY - 2025"
            ,
            r"C:\Users\bryan.betancur\OneDrive - Vivaaerobus\archivos_compartidos\Certificados Entrenamiento Viva Handling - Certficados\3.Constancias_anterior",
            r"C:\Users\bryan.betancur\OneDrive - Vivaaerobus\archivos_compartidos\Aeropuertos - AUTOPRESTACION MTY"
        ]                
        # Año mínimo para la fecha de modificación (inclusive)
        self.año_minimo_modificacion = 2024

        # Años específicos a excluir si aparecen en el nombre del archivo
        self.años_no_vigentes = ['2018', '2019', '2020', '2021', '2022', '2023', 'P.P.2022', 'P.P.2023']

        # Prefijos y sufijos a excluir (insensibles a mayúsculas/minúsculas)
        self.prefijos_excluidos = [
            'bitcora', 'bitacora ', 'fori ', 'ojt ', '2024 rtar ', 'ef-', 'ef ', 'ex-', 'ex ',
            'id ', 'id-', 'la ', 'la-', 'l.a.', 'l.a. ', 'l.a.-', 'ro-', 'ro ', 'sat-ro', 'pb',
            'laf', '2025-r', 'dif ', 'dif-', 'td', 'green', '09 bajas', 'bajas', 'bitacora'
        ]
        self.sufijos_excluidos = ['cun', 'gc-25', 'gp-25', ' ro']

        # Nombre y ruta del archivo de salida:
        # --- Archivo log, guarda todas las rutas de los archivos procesados
        self.ruta_registro_archivos_procesados = os.path.join(self.folder_data_processed, 'registro_archivos_procesados.txt')
        # --- Archivo lista de rutas pdfs(NUEVOS)
        self.ruta_nuevo_archivo_no_excluidos = os.path.join(self.folder_data_processed, 'lista_pdfs_nuevos_no_excluidos.txt')
        
    # Método privado
    def _create_output_folders(self):
        """Crea las carpetas de salida si NO existen."""
        # Se usa self. para acceder a los atributos de ruta definidos arriba
        os.makedirs(self.folder_data_processed, exist_ok=True)
        os.makedirs(self.folder_data_raw, exist_ok=True)
        os.makedirs(self.outpath_constancias_pdfs, exist_ok=True)
        os.makedirs(self.outpath_constancias_bajas_pdfs, exist_ok=True)
        os.makedirs(os.path.dirname(self.outpath_processed_files_log), exist_ok=True)
        # Asegurar también para el archivo de lista de nuevos
        os.makedirs(os.path.dirname(self.file_lista_pdfs_nuevos_no_excluidos), exist_ok=True)
        os.makedirs(self.temp_split_pdfs_folder, exist_ok=True)

def _añadir_set_procesado_en_memoria(file_path: str, config: Config):
    """
    Añade una ruta de archivo al conjunto de archivos procesados en memoria.
    El set automáticamente maneja la unicidad.
    """
    config.processed_files_set_in_memory.add(file_path)

def _guardar_registro_procesado_a_disco(config: Config):
    """
    Guarda toas las rutas unicas del set en memoria al archivo de registro. 
    Este archivo se reescribe completamente
    """
    try:
        with open(config.outpath_processed_files_log, 'w', encoding='utf-8') as f:
          for path in sorted(list(config.processed_files_set_in_memory)):
              f.write(f"{path}\n")
        print(f"Registro de archivo procesados actualizado con {len(config.processed_files_set_in_memory)} rutas unicas en: '{config.outpath_processed_files_log}'")
    except Exception as e:
        print(f"ERROR: No se pudo guardar el registro de archivos procesados en: '{config.outpath_processed_files_log}'. Error: {e}")  

def cargar_rutas_archivos_desde_archivo(file_name):
    """
    Cargar una lista de rutas de archivos desde un archivo de texto,
    donde cada línea contiene una ruta de archivo.
    """
    loaded_paths = []
    try:
        if not os.path.exists(file_name):
            print(f"Advertencia: El archivo {file_name} no se encontro. No hay archivos para cargar.")
            return []  # Retorna una lista vacía si el archivo no existe
        with open(file_name, 'r', encoding='utf-8') as f:
            for line in f:
                path = line.strip()  # Elimina espacios en blanco al inicio y al final  
                if path:  # Verifica que la línea no esté vacía
                    loaded_paths.append(path)
        print(f"Se cargaron {len(loaded_paths)} rutas de archivos desde {file_name}.")
    except Exception as e:
        print(f"Error al cargar rutas de archivos desde {file_name}: {e}")
    return loaded_paths
        
def extraer_datos_constancia(ruta_pdf, config: Config):
    """
    Esta función recibe la ruta de un archivo PDF y extrae los datos relevantes de la constancia,
    (SAT - SMS - AVSEC) utilizando los patrones definidos en la configuración.
    Determina el tipo de constancia y luego aplica la extracción específica,
    evitando retornos anticipados.
    """
    file_name = os.path.basename(ruta_pdf)
    datos = {
        "nombre_archivo" : file_name,
        "ruta_original" : ruta_pdf,
        "Nombre" : "Nombre no encontrado",
        "Curso" : "Curso no encontrado",
        "Fecha" : "Fecha no encontrada",
        "Instructor" : "Instructor no encontrado",
        "Grupo" : "Grupo no encontrado"
    }

    texto_extraido = ''
    try:
        doc = fitz.open(ruta_pdf)
        for p in doc:
            texto_extraido += p.get_text()
        doc.close()
    except Exception as e:
        print(f"Error al leer el pdf '{file_name}'. Error: {e}")
        return datos

    # --- Lógica para determinar el tipo de constancia ---
    # Prioridad: SAT -> SMS -> AVSEC
    constancia_type = "UNKNOWN"

    # 1. Verificar si es SAT
    for n in config.nombres_archivos_sat:
        if n.lower() in texto_extraido.lower():
            constancia_type = "SAT"
            break # Encontró palabra clave SAT, asumimos que es SAT

    # 2. Si no es SAT, verificar si es SMS
    if constancia_type == "UNKNOWN":
        for n in config.nombres_archivos_sms:
            if n.lower() in texto_extraido.lower():
                constancia_type = "SMS"
                break # Encontró palabra clave SMS, asumimos que es SMS

    # 3. Si no es SAT ni SMS, verificar si es AVSEC
    if constancia_type == "UNKNOWN":
        for n in config.nombres_archivos_avsec:
            if n.lower() in texto_extraido.lower():
                constancia_type = "AVSEC"
                break # Encontró palabra clave AVSEC, asumimos que es AVSEC

    # --- Procesar datos basándose en el tipo identificado ---
    if constancia_type == "SAT":
        datos['Curso'] = 'SAT'

        # Nombre Colaborador / Empleado
        patron_nombre = r"(?:Otorga la presente constancia a:|Otorga el presente reconocimiento a:)\s*\n*(.*?)\s*\n*(?:Por haber concluido satisfactoriamente el curso|POR HABER CONCLUIDO SATISFACTORIAMENTE EL CURSO)"
        coincidencia_nombre = re.search(patron_nombre, texto_extraido, re.DOTALL | re.IGNORECASE)
        if coincidencia_nombre:
            datos['Nombre'] = coincidencia_nombre.group(1).strip()

        # Curso (mantenemos la lógica de tu script original que podría sobrescribir 'SAT' si encuentra otro curso)
        patron_curso = r"Por haber concluido satisfactoriamente el curso\s*\n*(.*?)(?=\s*[\s•]*CONTENIDO TEMÁTICO:?|\s*\n*Impartido en)"
        coincidencia_curso = re.search(patron_curso, texto_extraido, re.DOTALL | re.IGNORECASE)
        if coincidencia_curso:
            datos['Curso'] = coincidencia_curso.group(1).strip()

        # Fecha / Curso
        patron_fecha = r"Impartido en .*?(?:el;?|del)\s*(.*?)(?=\n(?:[A-Z][a-zA-ZáéíóúÁÉÍÓÚüÜñÑ\s]+)?(?:Duración|Modalidad)|$)"
        coincidencia_fecha = re.search(patron_fecha, texto_extraido, re.DOTALL | re.IGNORECASE)
        if coincidencia_fecha:
            datos['Fecha'] = coincidencia_fecha.group(1).strip()
        if 'contenido' in datos['Fecha'].lower():
            patron_fecha_alt = r"Impartido en.*?el\s*(\d{1,2}\s*de\s*[a-zñáéíóúü]+\s*\d{4})(?=\s*CONTENIDO TEMATICO)"
            coincidencia_fecha_alt = re.search(patron_fecha_alt, texto_extraido, re.IGNORECASE)
            if coincidencia_fecha_alt:
                datos['Fecha'] = coincidencia_fecha_alt.group(1).strip()

        # Instructor / Curso
        patron_instructor = r"(.+?)\s*\n*Instructor"
        coincidencia_instructor = re.findall(patron_instructor, texto_extraido, re.DOTALL | re.IGNORECASE)
        if coincidencia_instructor:
            last_candidate = coincidencia_instructor[-1].strip()
            lines = [line.strip() for line in last_candidate.split('\n') if line.strip()]
            if lines:
                datos["Instructor"] = lines[-1]

        # Grupo / Curso
        patron_grupo = r"Grupo:\s*([A-Za-z0-9.]+(?:[\s-][A-Za-z0-9.]+)*[\s-]*\d{2})"
        coincidencia_grupo = re.search(patron_grupo, texto_extraido, re.DOTALL | re.IGNORECASE)
        if coincidencia_grupo:
            datos['Grupo'] = coincidencia_grupo.group(1).strip()
        else:
            # Revisa si este patrón AVSEC es realmente para SAT, o si necesitas un patrón SAT más específico.
            patron_grupo_alt = r"\bAVSEC-\d{4}-\d{2}\b"
            coincidencia_grupo_alt = re.search(patron_grupo_alt, texto_extraido)
            if coincidencia_grupo_alt:
                datos['Grupo'] = coincidencia_grupo_alt.group(0).strip()

    elif constancia_type == "SMS":
        datos['Curso'] = 'SMS'
        # print(f"\nImprimiento texto extraido de la constancia: {file_name}\n")
        # print(texto_extraido) # Pruebas

        # Nombre: Prioridad 1 - después de "Grants this recognition to:"
        patron_nombre_grants = r"Grants\s+this\s+recognition\s+to:\s*\n*(.*?)(?:\n|$)"
        coincidencia_nombre_grants = re.search(patron_nombre_grants, texto_extraido, re.IGNORECASE)
        if coincidencia_nombre_grants and coincidencia_nombre_grants.group(1).strip():
            nombre_limpio = re.sub(r'\s+', ' ', coincidencia_nombre_grants.group(1))
            datos['Nombre'] = nombre_limpio.strip()
        else:
            patron_nombre_inicio = r"^\s*([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑa-záéíóúñ]+)+)\s*\n+Impartido\s+"
            coincidencia_nombre_inicio = re.search(patron_nombre_inicio, texto_extraido, re.MULTILINE)
            if coincidencia_nombre_inicio and coincidencia_nombre_inicio.group(1).strip():
                datos['Nombre'] = re.sub(r'\s+', ' ', coincidencia_nombre_inicio.group(1)).strip()
            else: # Prioridad 2 - después de "Seguridad Aérea"
                patron_nombre_sms = r"Seguridad\s+Aérea\s*\n+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)+)"
                coincidencia_nombre_sms = re.search(patron_nombre_sms, texto_extraido, re.IGNORECASE)
                if coincidencia_nombre_sms:
                    datos['Nombre'] = coincidencia_nombre_sms.group(1).strip()

        if "(sms)" in datos['Nombre'].lower(): # Si "(sms)" está en el nombre extraído, buscar alternativa
            patron_nombre_inicio = r"^\s*([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑa-záéíóúñ]+)+)\s*\n+Impartido\s+"
            coincidencia_nombre_inicio = re.search(patron_nombre_inicio, texto_extraido, re.MULTILINE)
            if coincidencia_nombre_inicio and coincidencia_nombre_inicio.group(1).strip():
                datos['Nombre'] = re.sub(r'\s+', ' ', coincidencia_nombre_inicio.group(1)).strip()
            else: # Prioridad 3 - primera línea del texto si no hay encabezado
                first_line_text = texto_extraido.split('\n')[0] if texto_extraido else ''
                patron_nombre_primera_linea = r"^\s*([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)+)\s*$"
                coincidencia_nombre_primera_linea = re.search(patron_nombre_primera_linea, first_line_text)
                if coincidencia_nombre_primera_linea:
                    nombre_limpio_3 = re.sub(r'\s+', ' ', coincidencia_nombre_primera_linea.group(1))
                    datos['Nombre'] = nombre_limpio_3.strip()

        # Curso: robusto para espacios extra
        patron_curso = r"(inicial\s+de\s+Safety\s+Management\s+System\s+\(SMS\)|recurrente\s+de\s+Safety\s+Management\s+System\s+\(SMS\)|Safety\s+Management\s+System\s+\(SMS\))"
        coincidencia_curso = re.search(patron_curso, texto_extraido, re.IGNORECASE)
        if coincidencia_curso:
            curso_limpio = re.sub(r'\s+', ' ', coincidencia_curso.group(0))
            datos['Curso'] = curso_limpio.replace('.', '').strip().capitalize()
        
        # Patrón 1: Busca "Impartido el DD (de|del) MES (de|del)? YYYY"
        # Este patrón espera 'Impartido el' de forma más directa.
        patron_fecha_1 = re.compile(
            r"Impartido\s+el\s+(\d{1,2}\s+(?:de|del)\s+[a-zñáéíóúü]+\s+(?:de|del)?\s*\d{4})",
            re.IGNORECASE
            )
        coincidencia_fecha = patron_fecha_1.search(texto_extraido)

        if coincidencia_fecha:
            # Limpia espacios extra y normaliza "del" a "de"
            datos['Fecha'] = re.sub(r'\s+', ' ', coincidencia_fecha.group(1)).replace('del', 'de').strip()
        else:
            # Patrón 2: Busca "Impartido en [cualquier texto] el DD (de|del) MES (de|del)? YYYY"
            # Usa '.*?' (cualquier caracter, no voraz) para ser flexible con el texto intermedio.
            patron_fecha_2 = re.compile(
                r"Impartido\s+en.*?el\s+(\d{1,2}\s+(?:de|del)\s+[a-zñáéíóúü]+\s+(?:de|del)?\s*\d{4})",
                re.IGNORECASE
            )
            coincidencia_fecha = patron_fecha_2.search(texto_extraido)
            if coincidencia_fecha:
                # Limpia espacios extra y normaliza "del" a "de"
                fecha_limpia = re.sub(r'\s+', ' ', coincidencia_fecha.group(1)).replace('del', 'de').strip()
                datos['Fecha'] = fecha_limpia

        # Grupo: robusto para varios formatos
        patron_grupo = r"(SMS[\s-]N-\d{3,4}-\d{2})"
        coincidencia_grupo = re.search(patron_grupo, texto_extraido)
        if coincidencia_grupo:
            datos['Grupo'] = coincidencia_grupo.group(1).strip()
        else:
            patron_grupo_alt = r"(SMS-SAC-\d{3,4}-\d{2})"
            coincidencia_grupo_alt = re.search(patron_grupo_alt, texto_extraido)
            if coincidencia_grupo_alt:
                datos['Grupo'] = coincidencia_grupo_alt.group(1).strip()
            else:
                patron_grupo = r"(SMS\s*–\s*[A-Z]+\s*–\s*\d+\s*-\s*\d+|SMS[\s-]?N-\d+-\d+|SMS-SAC-\d+-\d+)"
                coincidencia_grupo = re.search(patron_grupo, texto_extraido)
                if coincidencia_grupo:
                    datos['Grupo'] = coincidencia_grupo.group(1).strip()
                else:
                    patron_sin_sms = r"Grupo:\s*(\d+-\d+|[A-Z]+-[A-Z]+-[A-Z]-\d+-\d+)"
                    coincidencia_sin_sms = re.search(patron_sin_sms, texto_extraido)
                    if coincidencia_sin_sms:
                        datos['Grupo'] = coincidencia_sin_sms.group(1).strip()

        # Instructor: nombre antes de "Instructor"
        patron_instructor = r"([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)+)\s*\n*Instructor"
        coincidencia_instructor = re.search(patron_instructor, texto_extraido, re.DOTALL)
        if coincidencia_instructor:
            instructor_limpio = coincidencia_instructor.group(1).strip()
            datos["Instructor"] = re.sub(r'\s*Instructor$', '', instructor_limpio, flags=re.IGNORECASE).strip()
        else: # Busca el nombre antes de "Coordinador de Entrenamiento de Seguridad Aérea"
            patron_coordinador = r"([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)+)\s*\n*Coordinador de Entrenamiento"
            coincidencia_coordinador = re.search(patron_coordinador, texto_extraido, re.DOTALL | re.IGNORECASE)
            if coincidencia_coordinador:
                datos["Instructor"] = coincidencia_coordinador.group(1).strip()

    elif constancia_type == "AVSEC":
        datos['Curso'] = 'AVSEC'

        # Nombre Colaborador / Empleado - PATRÓN ROBUSTECIDO
        patron_nombre_avsec = r"^(.*?)\s+(?:Impartido en (?:la )?Ciudad de|Por haber concluido satisfactoriamente el curso|CONTENIDO TEMATICO|Curso:|Folio:|Viva Aerobus|Duración de:)"
        coincidencia_nombre = re.search(patron_nombre_avsec, texto_extraido, re.DOTALL | re.IGNORECASE)
        if coincidencia_nombre:
            datos['Nombre'] = coincidencia_nombre.group(1).strip()

        # Curso - PATRÓN ROBUSTECIDO
        patron_curso_avsec = r"Por haber concluido satisfactoriamente el curso\s*\n*(.*?)(?:\s*Calificación obtenida:?|\s*Duración de:)"
        coincidencia_curso = re.search(patron_curso_avsec, texto_extraido, re.DOTALL | re.IGNORECASE)
        if coincidencia_curso:
            datos['Curso'] = coincidencia_curso.group(1).strip()

        # Fecha / Curso
        patron_fecha_avsec = r"Impartido en .*?\s*el\s*(.*?)(?=\n|Duración|Modalidad)"
        coincidencia_fecha = re.search(patron_fecha_avsec, texto_extraido, re.DOTALL | re.IGNORECASE)
        if coincidencia_fecha:
            datos['Fecha'] = coincidencia_fecha.group(1).strip()

        # Instructor / Curso - PATRÓN ROBUSTECIDO
        patron_instructor_avsec = r"(.+?)\s*\n*Instructor(?: Autorizado)?\.?"
        coincidencia_instructor = re.findall(patron_instructor_avsec, texto_extraido, re.DOTALL | re.IGNORECASE)
        if coincidencia_instructor:
            last_candidate = coincidencia_instructor[-1].strip()
            lines = [line.strip() for line in last_candidate.split('\n') if line.strip()]
            if lines:
                datos["Instructor"] = lines[-1]
        else:
            instructor_por_grupo = ['AVSEC-0010-24', 'AVSEC-0011-24', 'AVSEC-0140-24']
            for instructor in instructor_por_grupo:
                if instructor in texto_extraido:
                    datos['Instructor'] = 'Oscar Monzalvo Martinez'

        # Grupo / Curso / 2.0
        patron_grupo_avsec = r"(?:Grupo:\s*|Curso:\s*\d{1,2}-\d{1,2}\s*\n*|\b)((?:PRO-)?AVSEC-\d{3,4}-\d{2}\b)"
        coincidencia_grupo = re.search(patron_grupo_avsec, texto_extraido, re.DOTALL | re.IGNORECASE)
        if coincidencia_grupo:
            datos['Grupo'] = coincidencia_grupo.group(1).strip()

    # Si llega aquí y el tipo es UNKNOWN, o si los patrones no encontraron nada,
    # el diccionario 'datos' contendrá los valores por defecto "No encontrado".
    return datos 

def procesar_archivos_constancias(lista_rutas_archivos, config: Config):
    """
    Procesa una lista especifica de archivos(rutas de constancias), utilizando la funcion 'extraccion de datos'.
    """
    datos_cojunto_excluidos = []
    total_procesados = 0
    total_errores = 0

    if not lista_rutas_archivos:
        print(f"\nNo hay archivos para procesar.\n")
        return []
    
    print(f"\nIniciando el procesamiento de {len(lista_rutas_archivos)} archivos de constancias...\n")

    for full_pdf_path in lista_rutas_archivos:
        if not os.path.exists(full_pdf_path):
            print(f"Advertencia: El archivo no existe y será excluido: {full_pdf_path}")
            total_errores += 1
            continue
        try:
            print(f"Procesando archivo: {os.path.basename(full_pdf_path)}")
            # Pasamos la función extraer_datos_constancia y la class Config
            datos_extraidos = extraer_datos_constancia(full_pdf_path, config)
            datos_cojunto_excluidos.append(datos_extraidos)
            total_procesados += 1
        except Exception as e:
            print(f"Error al procesar el archivo '{os.path.basename(full_pdf_path)}'. Error: {e}")
            total_errores += 1

    print(f"\nProcesamiento completado. Total de archivos procesados: {total_procesados}. Total de errores: {total_errores}.\n")
    print(f"   Total de registros utiles generados: {len(datos_cojunto_excluidos)}\n")

    return datos_cojunto_excluidos

def limpiar_partes_archivo(text, vocales_acentos_map: dict):
    """
    Limpia una cadena de texto para ser usada como parte de un nombre de archivo.
    Elimina caracteres inválidos y reemplaza espacios con guiones bajos.
    Normaliza acentos.
    """
    if not isinstance(text, str):
        return "" # Retorna cadena vacía si no es string
    
    # Primero, normalizar los acentos para que el nombre de archivo no los contenga
    text_normalized_accents = normalizar_acentos(text, vocales_acentos_map)

    # Lista de caracteres inválidos en nombres de archivo de Windows/Linux
    invalid_chars = r'[<>:"/\\|?*\']'
    # Reemplazar caracteres inválidos con un guion bajo o removerlos
    cleaned_text = re.sub(invalid_chars, '', text)
    # Reemplazar espacios y múltiples guiones con un solo guion bajo
    cleaned_text = re.sub(r'\s+', '_', cleaned_text).strip('_')

    return cleaned_text

def normalizar_acentos(texto, vocales_acentos_map: dict):
    """
    Normaliza acentos en una cadena de texto.
    """
    if not isinstance(texto, str):
        return texto

    # Normalizar el texto de entrada a la forma NFC (Normalization Form Canonical Composition).
    # Esto convierte los caracteres a su representación precompuesta de un solo punto de código,
    # lo que asegura que coincidan con las claves en 'vocales_acentos'.
    texto_procesado = unicodedata.normalize('NFC', texto) # [1, 2, 4]

    for acento, sin_acento in vocales_acentos_map.items():
        # Aquí, 'acento' es ya la clave de tu diccionario, que asumimos está en NFC.
        # Al haber normalizado 'texto_procesado' a NFC, las coincidencias ahora funcionarán.
        texto_procesado = texto_procesado.replace(acento, sin_acento)

    return texto_procesado

def homologar_curso(curso_raw: str):
    """Homologa el nombre de un curso a una de las categorias predefinidas: 'SAT(Rampa)', 'SAT(Operador)', 'SAT(ASC)', 'AVSEC', 'SMS'
    """

    if not isinstance(curso_raw, str):
        return "OTRO" # Indica que no se pudo procesar
    
    curso_lower = curso_raw.lower()

    # --- Lógica para determinar el 'curso homologado' ---
    # Prioridad: SAT -> SMS -> AVSEC

    # Patrones para cursos SAT
    if 'servicio de apoyo en tierra' in curso_lower:
        if 'rampa' in curso_lower or 'agente de rampa' in curso_lower:
            return 'SAT(Rampa)'
        if 'operador' in curso_lower:
            return 'SAT(Operador)'
        if 'asesor de servicio al cliente' in curso_lower or 'asesor de servicio a cliente' in curso_lower or 'asc' in curso_lower:
            return 'SAT(ASC)'
        return 'SAT(General)' # Un nuevo tipo para SAT que no encaja en Rampa, Operador, ASC

    # Patrones para AVSEC
    if 'avsec' in curso_lower or 'seguridad de la aviacion' in curso_lower:
        return 'AVSEC'
    
    # Patrones para SMS
    if 'safety management system' in curso_lower or 'sms' in curso_lower:
        return 'SMS'
    
    return 'OTRO' # Categoría para cursos que no encajan en ninguna de las anteriores
    
    # # Adicionales (Posibles errores en constancias)
    # if 'sat' in curso_lower:
    #     return 'SAT(General)' # Un nuevo tipo para SAT que no encaja en Rampa, Operador, ASC
    
def dividir_pdf_constancia_agrupado(grouped_pdf_path: str, config: Config):
    """
    Dividir PDF de constancias agrupadas en PDFs individuales. Devuelve una lista de rutas a los archivos PDF temporales de una sola página. Se omiten las páginas no identificadas como certificados.
    """
    ruta_constancia_temp = []
    try:
        doc = fitz.open(grouped_pdf_path)
        total_pages = doc.page_count
        print(f"INFO: Analizando PDF agrupado: '{os.path.basename(grouped_pdf_path)}' con {total_pages} paginas para division.")

        patron_otorgamiento_curso = r"(?:Otorga la presente constancia a:|Por haber concluido satisfactoriamente el curso|Seguridad de la Aviación Civil)"

        patron_avsec_footer = r"Curso:\s*VH-AVSEC-\d+-\d+"

        for i in range(total_pages):
            page = doc.load_page(i)
            text = page.get_text()

            is_certificate_page = False

            # Criterio de deteccion 1: Frase de otorgamiento/curso
            if re.search(patron_otorgamiento_curso, text, re.DOTALL | re.IGNORECASE):
                is_certificate_page = True
            
            # Criterio de detección 2: Patrón específico de AVSEC en el pie de página
            if not is_certificate_page and re.search(patron_avsec_footer, text, re.IGNORECASE):
                is_certificate_page = True

            if not is_certificate_page:
                print(f"DEBUG: Página {i+1} de '{os.path.basename(grouped_pdf_path)}' no parece ser una constancia válida. Saltando.")
                continue

            # Si es una constancia, guarda esta página individual como un nuevo PDF temporal
            output_pdf = fitz.open()
            output_pdf.insert_pdf(doc, from_page=i, to_page=i) # Inserta solo esta página

            original_base_name = os.path.splitext(os.path.basename(grouped_pdf_path))[0]
            temp_filename_base = f"temp_{original_base_name}_page_{i+1}.pdf"
            temp_filepath = os.path.join(config.temp_split_pdfs_folder, temp_filename_base)
            
            # Asegura un nombre de archivo temporal único para evitar sobrescrituras accidentales
            unique_temp_filepath = temp_filepath
            count = 1
            while os.path.exists(unique_temp_filepath):
                temp_filename_base_without_ext, temp_ext = os.path.splitext(temp_filename_base)
                unique_temp_filepath = os.path.join(config.temp_split_pdfs_folder, f"{temp_filename_base_without_ext}_{count}{temp_ext}")
                count += 1

            output_pdf.save(unique_temp_filepath)
            output_pdf.close()
            ruta_constancia_temp.append(unique_temp_filepath)
            print(f"DEBUG: Extraída página {i+1} de '{os.path.basename(grouped_pdf_path)}' como constancia temporal: '{os.path.basename(unique_temp_filepath)}'")

        doc.close()
        return ruta_constancia_temp

    except Exception as e:
        print(f"ERROR: No se pudo dividir el PDF agrupado '{os.path.basename(grouped_pdf_path)}'. Error: {e}")
        return []


    except:
        pass
    pass

def normalizar_mes(mes_str, mapeo_meses_map: dict):
    """Normaliza el nombre del mes (en español) a su número de mes."""
    return mapeo_meses_map.get(mes_str.lower(), None) # Retorna None si no se encuentra mapeo

def parse_fecha_inicio(fecha_texto, mapeo_meses_map: dict):
    """
    Extrae y parsea la fecha de inicio de una cadena de texto de fecha,
    manejando diferentes formatos y rangos.
    """
    if pd.isna(fecha_texto) or not isinstance(fecha_texto, str):
        return pd.NaT # Retorna 'Not a Time' para valores nulos o no-string

    fecha_texto_original = fecha_texto.strip()
    fecha_texto_lower = fecha_texto_original.lower()

    # --- Paso 1: Extraer el año de forma robusta de la cadena completa ---
    # Busca 4 dígitos que representen un año (e.g., 2024, 2025).
    # Se busca el último año encontrado en la cadena, ya que suele ser el más relevante.
    year_str = None
    years_found = re.findall(r"(\d{4})", fecha_texto_original)
    if years_found:
        year_str = years_found[-1] # Toma el último año encontrado
    else:
        # Si no se encuentra un año de 4 dígitos, no podemos formar una fecha completa.
        return pd.NaT

    dia_str = None
    mes_str_raw = None

    # --- Paso 2: Extraer Día y Mes con patrones priorizados ---

    # Patrón A (Alta prioridad): "DD de MES de YYYY" o "DD de MES YYYY"
    # Ejemplos: "08 de marzo de 2025", "04 de julio 2024", "31 de julio de 2025"
    patron_full_date_con_de = re.compile(
        r"(\d{1,2})\s*de\s*([a-zñáéíóúü]+)(?:\s*de)?", # Captura día, mes, y permite 'de' opcional antes del año
        re.IGNORECASE
    )
    match = patron_full_date_con_de.search(fecha_texto_lower)
    if match:
        dia_str = match.group(1)
        mes_str_raw = match.group(2)
    else:
        # Patrón B: "DD al DD MES" (para rangos, ejemplo: "29 enero al 01 febrero Enero-")
        # Captura el primer día y el mes del rango.
        patron_al_rango = re.compile(
            r"^(\d{1,2})\s*(?:al|a)\s*\d{1,2}\s*([a-zñáéíóúü]+)",
            re.IGNORECASE
        )
        match = patron_al_rango.search(fecha_texto_lower)
        if match:
            dia_str = match.group(1)
            mes_str_raw = match.group(2)
        else:
            # Patrón C: "DD-DD MES" o "DD-MES" (para rangos o fechas simples con guion)
            # Ejemplos: "19-21 Marzo", "19-Marzo"
            patron_guion_rango = re.compile(
                r"^(\d{1,2})(?:[-\s]?\d{1,2})?[-\s]?([a-zñáéíóúü]+)",
                re.IGNORECASE
            )
            match = patron_guion_rango.search(fecha_texto_lower)
            if match:
                dia_str = match.group(1)
                mes_str_raw = match.group(2)
            else:
                # Patrón D: "DiaAbrev-DD-MesAbrev" (para formatos con día de la semana abreviado)
                # Ejemplo: "lun-20-ene"
                patron_dia_mes_abreviado = re.compile(
                    r"^(?:[a-zñáéíóúü]{2,4}[-\s]?)?(\d{1,2})[-\s]?([a-zñáéíóúü]{3,})",
                    re.IGNORECASE
                )
                match = patron_dia_mes_abreviado.search(fecha_texto_lower)
                if match:
                    dia_str = match.group(1)
                    mes_str_raw = match.group(2)
                else:
                    # Patrón E (Menor prioridad): "DD MES" (cuando no hay "de" ni rangos específicos, pero sí día y mes)
                    # Ejemplo: "19 Marzo", "29 Enero" (sin el "al" o "de")
                    patron_simple_no_de = re.compile(
                        r"(\d{1,2})\s*([a-zñáéíóúü]+)",
                        re.IGNORECASE
                    )
                    match = patron_simple_no_de.search(fecha_texto_lower)
                    if match:
                        dia_str = match.group(1)
                        mes_str_raw = match.group(2)

    # --- Paso 3: Combinar el día, mes extraídos y el año global para formar la fecha ---
    if dia_str and mes_str_raw:
        mes_num = normalizar_mes(mes_str_raw, mapeo_meses_map)
        if mes_num is not None: # Solo si el mes se pudo normalizar a un número
            try:
                # Usamos '%d %m %Y' para crear el objeto datetime
                return datetime.strptime(f"{dia_str} {mes_num} {year_str}", '%d %m %Y')
            except ValueError:
                pass # Si la combinación día/mes/año no es una fecha válida (ej. 31 de Febrero)

    return pd.NaT # Si ningún patrón coincide o el parsing final falla

def cargar_data_hc(path_hc_table: str, vocales_acentos_map: dict):
    """
    Carga la tambla de empleado (HC), aplica normalizaciones y crea una columna
    adicional con el nombre en formato "NOMBRE APELLIDO(P) APELLIDO(M)" para mejorar las coincidencias.
    """
    # Define antes por si hay error
    df_hc = pd.DataFrame(columns=['#emp', 'nombre_completo', 'nombre', 'paterno', 'materno', 'estatus'])

    try:
        df_hc = pd.read_csv(path_hc_table, encoding='utf-8')
        df_hc = df_hc.apply(lambda col: normalizar_acentos(col, vocales_acentos_map)
                            if col.dtype == 'object' else col)
        for col in ['nombre_completo', 'nombre', 'paterno', 'materno', 'estatus']:
            if col in df_hc.columns and df_hc[col].dtype == 'object':
                df_hc[col] = df_hc[col].astype('string').fillna('').str.strip().str.upper().apply(lambda x: normalizar_acentos(x, vocales_acentos_map))
            elif col not in df_hc.columns:
                print(f"Advertencia: La columna '{col}' no se encuentra en el archivo HC. No se podra usar para el merge 'invertido'.")
                df_hc[col] = ''
        
        # Creamos la columna de nombre 'invertido' para que coincida con "NOMBRE, APELLIDO(P), APELLIDO(M)"
        df_hc['nombre_completo_invertido'] = df_hc['nombre'] + ' ' + df_hc['paterno'] + ' ' + df_hc['materno']
        df_hc['nombre_completo_invertido'] = df_hc['nombre_completo_invertido'].str.replace(r'\s+', ' ', regex=True).str.strip()

        # Revisar que 'nombre_completo' (original) tambien este normalizada
        if 'nombre_completo' in df_hc.columns:
            df_hc['nombre_completo'] = df_hc['nombre_completo'].astype('string').fillna('').str.strip().str.upper().apply(lambda x: normalizar_acentos(x, vocales_acentos_map))
        else:
            print("Advertencia: La columna 'nombre_completo' no se encuentra en el archivo HC. Se utilizará el formato invertido como principal.")
            # Si no hay 'nombre_completo' original, la columna 'nombre_completo_invertido' podría ser la principal
            df_hc['nombre_completo'] = df_hc['nombre_completo_invertido']

        df_hc['#emp'] = df_hc['#emp'].astype('string').str.strip()
        print(f"\nTabla de empleados cargada exitosamente desde: {path_hc_table}\n")
    except FileNotFoundError:
        print(f"\nAdverencia: El archivo de empleados '{path_hc_table}' no fue encontrado. El proceso continuara sin datos de empleados para merge.\n")
    except Exception as e:
        print(f"\nAdvertencia: No se pudo cargar la tabla de empleados desde {path_hc_table}. Error: {e}\n")
        df_hc = pd.DataFrame(columns=['#emp', 'nombre_completo', 'estatus'])
    return df_hc

def procesar_y_mergear_constancias(datos_conjunto_excluidos: list, df_hc: pd.DataFrame, vocales_acentos_map: dict):
    """
    Convierte la lista de datos extraidos en un DataFrame, o limpia, aplica filtros y lo une con la tabla de empleados(HC) utilizando un doble merge para nombres 'invertidos'.
    """
    if not datos_conjunto_excluidos:
        print("No hay datos de constancias para procesar.")
        return pd.DataFrame() # Retorna DataFrame vacio
    
    df_constancias = pd.DataFrame(datos_conjunto_excluidos)
    df_constancias.columns = df_constancias.columns.str.lower()
    df_constancias.columns = df_constancias.columns.str.replace(' ', '_', regex=False)
    df_constancias = df_constancias.rename(columns={'nombre' : 'nombre_completo'})

    columns_text = ['nombre_completo', 'fecha', 'curso', 'instructor', 'grupo']

    for c in columns_text:
        if c in df_constancias.columns:
            df_constancias[c] = df_constancias[c].astype('string').fillna('').str.replace(',', '', regex=False).str.replace(r'\s+', ' ', regex=True).str.strip().apply(lambda x: normalizar_acentos(x, vocales_acentos_map))
        else:
            df_constancias[c] = '' 

        # Asegurarse que 'nombre_archivo' y 'ruta_original' siempre sean string y no sea NaN, sin normalización de acentos.
        if 'nombre_archivo' in df_constancias.columns:
            df_constancias['nombre_archivo'] = df_constancias['nombre_archivo'].astype('string').fillna('')
        else:
            df_constancias['nombre_archivo'] = ''

        if 'ruta_original' in df_constancias.columns:
            df_constancias['ruta_original'] = df_constancias['ruta_original'].astype('string').fillna('')
        else:
            df_constancias['ruta_original'] = '' # Si la columna no existe, crearla vacía

    # upper - nombre_completo - merge
    df_constancias['nombre_completo'] = df_constancias['nombre_completo'].str.upper().str.strip() 

    # Asegurar consistencia con df_hc
    df_constancias['nombre_completo'] = df_constancias['nombre_completo'].apply(lambda x: normalizar_acentos(x, vocales_acentos_map))

    # Recuento de filas sin filtros
    recuento_filas_inicial = len(df_constancias)
    print(f"\[INFO PROCESAMIENTO] Registros antes de filtros de negocio: {recuento_filas_inicial}")

    # --- Aplicar filtros y contar descartados ---
    # Instructor excluido
    df_filtrado = df_constancias[df_constancias['instructor'] != 'Enrique Ortiz Hernandez']
    eliminado_por_instructor = recuento_filas_inicial - len(df_filtrado)
    if eliminado_por_instructor > 0:
        print(f"  - Descartados por 'instructor' (Enrique Ortiz Hernandez): {eliminado_por_instructor}")
    df_constancias = df_filtrado
    recuento_filas_actuales = len(df_constancias)

    # Nombres de archivo específicos excluidos
    archivos_expecificos_a_excluir = [
        'SAT 2024 MUÑOZ TEJERO ALEX ROMARIO.pdf',
        'AVSEC 2025 BONILLA ESQUIVEL GERSON ALEXANDER.pdf',
        'SAT 2024 BONILLA ESQUIVEL GERSON ALEXANDER.pdf',
        'BITCORA 2025 OP B CASTILLO ORTEGA JOEL ALBERTO.PDF',
        'RUIZ CARDONA MAYELA.pdf',
        'NIÑO PLASCENCIA ALFREDO.pdf',
        'CRUZ SANTIAGO SARA.pdf',
        'OP 2024 PORTOS GAMEZ HECTOR ABRAHAM (1) (1).pdf',
        'OP 2024 PORTOS GAMEZ HECTOR ABRAHAM (1).pdf'
    ]
    df_filtrado = df_constancias[~df_constancias['nombre_archivo'].isin(archivos_expecificos_a_excluir)]
    eliminados_por_nombres_especificos = recuento_filas_actuales - len(df_filtrado)
    if eliminados_por_nombres_especificos > 0:
        print(f"  - Descartados por 'nombre_archivo' específico: {eliminados_por_nombres_especificos}")
    df_constancias = df_filtrado
    recuento_filas_actuales = len(df_constancias)

    # Grupos que empiezan con 'RO' excluidos
    df_filtrado = df_constancias[~df_constancias['grupo'].str.startswith('RO')]
    eliminados_por_grupo_ro = recuento_filas_actuales - len(df_filtrado)
    if eliminados_por_grupo_ro > 0:
        print(f"  - Descartados por 'grupo' que inicia con 'RO': {eliminados_por_grupo_ro}")
    df_constancias = df_filtrado
    recuento_filas_actuales = len(df_constancias)

    # Eliminar duplicados
    df_constancias_deduplicated = df_constancias.drop_duplicates(keep='first') # `keep='first'` es el default, pero es bueno ser explícito
    eliminados_por_duplicados = recuento_filas_actuales - len(df_constancias_deduplicated)
    if eliminados_por_duplicados > 0:
        print(f"  - Descartados por ser registros duplicados: {eliminados_por_duplicados}")
    df_constancias = df_constancias_deduplicated
    recuento_filas_actuales = len(df_constancias)

    print(f"[INFO PROCESAMIENTO] Registros después de filtros de negocio: {recuento_filas_actuales}")

    # Homologación (esto no debería descartar filas)
    df_constancias['instructor'] = df_constancias['instructor'].str.replace("Ing. ", "", regex=False).str.strip()
    df_constancias['instructor'] = df_constancias['instructor'].str.replace("Lic. ", "", regex=False).str.strip()
    df_constancias['curso'] = df_constancias['curso'].str.replace(": Recurrente", "Recurrente:", regex=False).str.strip()
    df_constancias['curso'] = df_constancias['curso'].str.replace("recurrente:", "Recurrente:", regex=False).str.strip()
    df_constancias['curso'] = df_constancias['curso'].str.replace("Recurrente :", "Recurrente:", regex=False).str.strip()
    df_constancias['curso'] = df_constancias['curso'].str.replace(": Inicial", "Inicial:", regex=False).str.strip()
    df_constancias['curso'] = df_constancias['curso'].str.replace("Inicial :", "Inicial:", regex=False).str.strip()
    df_constancias['curso'] = df_constancias['curso'].str.replace("inicial :", "Inicial:", regex=False).str.strip()
    df_constancias['curso'] = df_constancias['curso'].str.replace("inicial:", "Inicial:", regex=False).str.strip()
    df_constancias['curso'] = df_constancias['curso'].str.replace(": PRESCREENING", "PRESCREENING", regex=False).str.strip()

    df_constancias['nombre_archivo'] = df_constancias['nombre_archivo'].str.replace(r'\b' + "AVSE" + r'\b', "AVSEC", regex=True).str.strip()
    df_constancias['nombre_archivo'] = df_constancias['nombre_archivo'].str.replace(r'\b' + "AVASE" + r'\b', "AVSEC", regex=True).str.strip()
    df_constancias['grupo'] = df_constancias['grupo'].str.replace(" -25", "-25", regex=False).str.strip()

    # Modificacion de nombre manual por error en constancia.
    df_constancias['nombre_completo'] = df_constancias['nombre_completo'].str.replace('RAUL LUNA UIZAR', 'RAUL LUNA HUIZAR', regex=False)

    # --- DOBLE MERGE PARA MEJORAR COINCIDENCIAS DE NOMBRES

    # Primer merge con el formato "NOMBRE APELLIDO(P) APELLIDO(M)" en 'df_constancias' vs "NOMBRE APELLIDO(P) APELLIDO(M)" en 'df_hc'
    print("\nRealizando primer merge (Constancias: Apellido(P) Apellido(M) Nombre)")

    # #### MERGE 'Nombre'
    df_constancias_primer_merge = pd.merge(df_constancias,
                                     df_hc[['nombre_completo_invertido', '#emp', 'estatus']],
                                     left_on=['nombre_completo'], # Columna de constancias (Nombre Apellido),
                                     right_on=['nombre_completo_invertido'], # Columna invertida de HC (Nombre Apellido)
                                     how='left',
                                     suffixes=('', '_hc_pass1')) # Sufijo para evitar colisiones si hubiera otras columnas '#emp'
    
    # Renombrar '#emp_hc_pass1' a '#emp' para el primer pase
    df_constancias_primer_merge = df_constancias_primer_merge.rename(columns={'#emp_hc_pass1': '#emp', 'estatus_hc_pass1': 'estatus'})
    # Eliminar la columna de merge usada de df_hc
    df_constancias_primer_merge = df_constancias_primer_merge.drop(columns=['nombre_completo_invertido'], errors='ignore')

    # Identificar registros que no se encontraron en el primer merge (donde #emp es NaN)
    # Rellenamos con 0 temporalmente para el merge y luego lo limpiaremos
    registros_sin_coincidencia = df_constancias_primer_merge[df_constancias_primer_merge['#emp'].isnull()]
    registros_coincidentes = df_constancias_primer_merge[df_constancias_primer_merge['#emp'].notnull()]

    print(f"  - Registros encontrados en el primer merge {len(registros_coincidentes)}")
    print(f"  - Registros NO encontrados en el primer merge {len(registros_sin_coincidencia)}")

    # Paso 2: Segundo merge para los registros no encontrados en el primer pase
    # Ahora intentamos con el formato "APELLIDOP APELLIDOM NOMBRE" (df_constancias) vs "APELLIDOP APELLIDOM NOMBRE" (df_hc)

    if not registros_sin_coincidencia.empty:
        print("\nRealizando segundo merge (Constancias: Apellido(P) Apellido(M) Nombre)")
        # Trabajar con una copia para evitar SettingWithCopyWarning
        df_para_segundo_merge = registros_sin_coincidencia.copy() 

        df_constancias_segundo_merge = pd.merge(df_para_segundo_merge,
        df_hc[['nombre_completo', '#emp', 'estatus']], # Usar la columna original 'nombre_completo' de 'df_hc'
        left_on=['nombre_completo'], # Columna de Constancias (Apellidos-Nombre)
        right_on=['nombre_completo'], # Columna original de 'df_hc'
        how='left',
        suffixes=('_pass1', '_pass2'))

        # Ahora necesitamos consolidar los '#emp'
        # Donde '#emp_pass1' es nulo, usamos '#emp_pass2'
        df_constancias_segundo_merge['#emp'] = df_constancias_segundo_merge['#emp_pass1'].fillna(df_constancias_segundo_merge['#emp_pass2'])
        # df_constancias_segundo_merge['estatus'] = df_constancias_segundo_merge['estatus_pass1'].fillna(df_constancias_segundo_merge['estatus_pass2'])
        df_constancias_segundo_merge = df_constancias_segundo_merge.drop(columns=['#emp_pass1', '#emp_pass2'])
        # df_constancias_segundo_merge = df_constancias_segundo_merge.drop(columns=['#emp_pass1', '#emp_pass2', 'estatus_pass1', 'estatus_pass2'])

        # Los registros que se encuentren en el segundo pase tendrán un '#emp' aquí
        # Necesitamos unir estos resultados con los que ya se encontraron en el primer pase
        
        registros_coincidentes_2 = df_constancias_segundo_merge[df_constancias_segundo_merge['#emp'].notnull()]
        registros_sin_coincidencia_final = df_constancias_segundo_merge[df_constancias_segundo_merge['#emp'].isnull()]

        print(f"\n  - Registros encontrados en el segundo merge: {len(registros_coincidentes_2)}")
        print(f"\n  - Registros no encontrados despues del segundo merge: {len(registros_sin_coincidencia_final)}")

        # Combinar todos los resultados
        df_constancias_merged = pd.concat([registros_coincidentes, registros_coincidentes_2, registros_sin_coincidencia_final], ignore_index=True)
    else:
        df_constancias_merged = registros_coincidentes # Si no hubo nada sin coincidencia en el primer pase, este es el resultado final
            
    df_constancias_merged['#emp'] = df_constancias_merged['#emp'].fillna(0).astype(int)
    df_constancias_merged['estatus'] = df_constancias_merged['estatus'].fillna('DESCONOCIDO').astype('string') 
    column_emp = df_constancias_merged.pop('#emp')
    df_constancias_merged.insert(2, '#emp', column_emp)
    df_constancias_merged = df_constancias_merged.sort_values(['#emp', 'nombre_completo'])

    for c in columns_text:
        if c in df_constancias_merged:
            df_constancias_merged[c] = df_constancias_merged[c].astype('string')

    # Aquí también puedes imprimir info para verificar el conteo de filas
    print("\nDataFrame de constancias después del doble merge con HC:\n")
    df_constancias_merged.info()
    print(f"Total de filas en df_constancias_merged después de doble merge: {len(df_constancias_merged)}")
    print("\nConteo de empleados después del doble merge (0 = sin coincidencia):\n")
    print(df_constancias_merged['#emp'].value_counts(dropna=False))

    final_text_columns = ['nombre_archivo', 'ruta_original', 'nombre_completo', 'curso', 'fecha', 'instructor', 'grupo']
    for c in final_text_columns:
        if c in df_constancias_merged.columns:
            df_constancias_merged[c] = df_constancias_merged[c].astype('string')

    # Asegurarse de que 'ruta_original' es siempre string y no NaN después de todos los merges
    if 'ruta_original' in df_constancias_merged.columns:
        df_constancias_merged['ruta_original'] = df_constancias_merged['ruta_original'].astype('string').fillna('')
    else:
        # Esto debería ser un caso extremo, pero se añade por robustez
        df_constancias_merged['ruta_original'] = pd.Series([''] * len(df_constancias_merged), dtype='string')

    return df_constancias_merged

def identificar_y_reportar_constancias_sin_coincidencia(df_constancias_merged: pd.DataFrame, folder_data_processed):
    """
    Identifica constancias sin numero de empleado(#emp) asociado y las exporta a archivos.
    """
    constancias_sin_emp = df_constancias_merged[df_constancias_merged['#emp'] == 0]
    if not constancias_sin_emp.empty:
        print(f"\nAdvertencia: {len(constancias_sin_emp)} constancia(s) no pudieron ser asociadas a un numero de empleado '#emp'\n")
        for index, row in constancias_sin_emp.iterrows():
            print(f"Archivo: {row['nombre_archivo']} \nNombre empleado: {row['nombre_completo']}\n")

        # Outputs sin "#emp"
        output_excel_path = os.path.join(folder_data_processed, 'datos_constancias_sin_emp.xlsx')
        output_csv_path = os.path.join(folder_data_processed, 'datos_constancias_sin_emp.csv')
        try:
            # Almacenar y exportar registros sin coincidencias '#emp'
            constancias_sin_emp.to_excel(output_excel_path, index=False)
            print(f"Registros sin '#emp' exportados a: {output_excel_path}")
        except Exception as e:
            print(f"Error al exportar constancias sin '#emp' a Excel: {e}")
        try:
            constancias_sin_emp.to_csv(output_csv_path, index=False, encoding='utf-8')
            print(f"Registros sin '#emp' exportados a: {output_csv_path}")
        except Exception as e:
            print(f"Error al exportar constancias sin '#emp' a CSV: {e}")
    else:
        print(f"\nTodas las constancias se asociaron correctamente\n")

def organizar_archivos_pdf(df_constancias_merged: pd.DataFrame, outpath_final: str, config: Config):
    """
    Organiza los archivos PDF copiandolos a carpetas individuales por numero de empleado(#emp) y renombrandolos segun la columna 'nombre_archivo_nuevo'.
    Evita sobrescribir archivos existentes añadiendo un sufijo numerico.
    """
    print(f"Iniciando organizacion de archivos en {outpath_final}")
    print(f"Destino para BAJAS: {config.outpath_constancias_bajas_pdfs}")
    # print(f"Destino para BAJAS: {config.outpath_onedrive_constancias_bajas_pdfs}")
    pdfs_organizados = 0
    pdfs_no_organizados_error_copia = 0
    pdfs_sin_num_emp_count = 0 # Renombrada para evitar confusión con el contador de la función
    pdfs_bajas_organizados = 0
    pdfs_activos_organizados = 0

    if df_constancias_merged.empty:
        print("No hay constancias para organizar (DataFrame vacío).")
        print(f"\nOrganización de archivos terminada.\n")
        print(f"Total de PDFs organizados en carpetas con numero de empleado: {pdfs_organizados}")
        print(f"Total de archivos NO organizados (sin '#emp'): 0 (DataFrame estaba vacío)")
        print(f"Total de archivos que fallaron al copiar (errores FileNotFoundError/Otros): 0\n")
        return

    # Contar los PDFs que no tienen un número de empleado asignado (== 0)
    pdfs_sin_num_emp_count = (df_constancias_merged['#emp'] == 0).sum()

    for index, row in df_constancias_merged.iterrows():
        num_emp = str(row['#emp']) # Sera '0' si no hay coincidencia de '#emp'
        original_pdf_path = row['ruta_original']
        base_new_file_name_with_ext = row['nombre_archivo_nuevo'] # Este es el nombre base, sin sufijo aún
        estatus_empleado = row['estatus'].upper()

        # Determinar la carpeta destino basada en el estatus.
        if estatus_empleado == 'BAJA' and num_emp == 0:
            target_base_folder = config.outpath_constancias_bajas_pdfs
            # target_base_folder = config.outpath_onedrive_constancias_bajas_pdfs
            pdfs_bajas_organizados += 1
        elif estatus_empleado == 'BAJA':
            target_base_folder = config.outpath_constancias_bajas_pdfs
            # target_base_folder = config.outpath_onedrive_constancias_bajas_pdfs
            pdfs_bajas_organizados += 1
        else:
            target_base_folder = config.outpath_constancias_pdfs
            # target_base_folder = config.outpath_onedrive_constancias_pdfs

        # Verificar si la 'ruta_original' existe antes de intentar crear la carpeta y copiar
        if not os.path.exists(original_pdf_path):
            print(f"ADVERTENCIA: Archivo de origen no encontrado en '{original_pdf_path}'. Se salta.")
            pdfs_no_organizados_error_copia += 1
            continue

        # Crear la carpeta de destino si no existe
        folder_emp = os.path.join(target_base_folder, num_emp)
        os.makedirs(folder_emp, exist_ok=True) # exist_ok=True evita errores si ya existe

        # Logica para evitar sobrescritura y añadir sufijo numerico
        destino_pdf_path = os.path.join(folder_emp, base_new_file_name_with_ext)

        try:
            # Copiar el archivo. shutil.copy2 copia también metadatos como la fecha de modificación.
            shutil.copy2(original_pdf_path, destino_pdf_path)
            pdfs_organizados += 1
            # IMPORTANTE: Añadir PATH ORIGINAL al archivo "registro de archivos procesados".
            _añadir_set_procesado_en_memoria(original_pdf_path, config)
        except FileNotFoundError:
            print(f"\nERROR: Archivo no encontrado en origen para copiar: '{original_pdf_path}'\n")
            pdfs_no_organizados_error_copia += 1
        except Exception as e:
            print(f"\nERROR al copiar: '{os.path.join(original_pdf_path)}' a '{destino_pdf_path}': {e}\n")
            pdfs_no_organizados_error_copia += 1

    print(f"\nOrganización de archivos terminada.\n")
    print(f"Total de PDFs organizados (incluye Activos, Bajas y sin #emp): {pdfs_organizados}")
    print(f"  - PDFs de empleados ACTIVOS organizados: {pdfs_activos_organizados}")
    print(f"  - PDFs de empleados BAJAS organizados: {pdfs_bajas_organizados}")
    print(f"  - PDFs sin número de empleado (en carpeta '0'): {pdfs_sin_num_emp_count}")
    print(f"Total de archivos que fallaron al copiar (errores FileNotFoundError/Otros): {pdfs_no_organizados_error_copia}\n")

def normalizar_y_categorizar_fechas(df_constancias_merged: pd.DataFrame, mapeo_meses_map, vocales_acentos_map: dict):
    """
    Normaliza las fechas de las constancias, calcula la fecha de vigencia y asigna un estatus(Vigente/Vencido).
    Tambien crea la columna 'nombre_archivo_nuevo' con el formato "CURSO_DD-MM-YYY_NOMBRE COMPLETO" y la columna 'curso_homologado'.
    """
    if df_constancias_merged.empty:
        print("DataFrame vacio, saltando normalizacion de fechas.")
        return df_constancias_merged

    # parsear 'fecha'
    df_constancias_merged['fecha_asignada'] = df_constancias_merged['fecha'].apply(
        lambda x: parse_fecha_inicio(x, mapeo_meses_map))
    # fecha sin la hora, normalizarla
    df_constancias_merged['fecha_asignada'] = pd.to_datetime(df_constancias_merged['fecha_asignada'], errors='coerce').dt.normalize()

    # fecha_vigencia' (un año posterior a 'fecha normalizada')
    df_constancias_merged['fecha_vigencia'] = df_constancias_merged['fecha_asignada'] + pd.DateOffset(years=1)
    
    # Obtener la fecha actual (solo la fecha, sin la hora, para una comparación justa))
    fecha_hoy = pd.to_datetime(datetime.now().date())

    # condicional 'Vigente' , 'Vencido'
    df_constancias_merged['estatus_vigencia'] = np.where(
        df_constancias_merged['fecha_vigencia'] < fecha_hoy,
        'Vencido',
        'Vigente'
    )

    # Crear columna 'curso-homologado'
    df_constancias_merged['curso_homologado'] = df_constancias_merged['curso'].apply(homologar_curso)

    # Crear columna 'nombre_archivo_nuevo'

    def generate_new_filename(row, vocales_acentos_map):
        # Limpiar y formatear cada parte para el nombre del archivo
        curso_parte = limpiar_partes_archivo(row['curso_homologado'], vocales_acentos_map) # Usar el curso homologado
        nombre_completo_parte = limpiar_partes_archivo(row['nombre_completo'], vocales_acentos_map)

        fecha_parte = ''
        if pd.notna(row['fecha_asignada']):
            fecha_parte = row['fecha_asignada'].strftime('%d-%m-%Y')

        # Combinar las partes. Eliminar posibles guiones bajas dobles o al inicio/fin.

        new_name = f"{curso_parte}_{fecha_parte}_{nombre_completo_parte}.pdf"
        return re.sub(r'_{2,}', '_', new_name).strip('_')  # Eliminar guiones bajos dobles y en los extremos
    
    df_constancias_merged['nombre_archivo_nuevo'] = df_constancias_merged.apply(lambda row: generate_new_filename(row, vocales_acentos_map), axis=1)

    # Organizar columnas e incluir 'nombre_archivo_nuevo'
    df_final = df_constancias_merged[['nombre_archivo', 'nombre_archivo_nuevo', '#emp', 'nombre_completo', 'estatus', 'curso_homologado','curso', 'instructor', 'grupo', 'fecha', 'fecha_asignada', 'fecha_vigencia', 'estatus_vigencia', 'ruta_original']]
    df_final = df_final.reset_index(drop=True)

    return df_final

def exportar_resultados(df_final: pd.DataFrame, outpath_xlsx, outpath_csv):
    """
    Exporta el DataFrame final a archivos Excel y CSV con formato.
    """

    if df_final.empty:
        print("Data Frame vacio, no hay resultados para exportar.")
        return

    # Exportacion a Excel
    try:
        writer = pd.ExcelWriter(outpath_xlsx, engine='xlsxwriter')
        df_final.to_excel(writer, sheet_name='Historial Constancias', index=False)
        # df_errores_constancias.to_excel(writer, sheet_name='Errores Constancias', index=False)
        # Acceder al objeto workbook y worksheet de xlsxwriter para aplicar formato
        workbook = writer.book
        worksheet = writer.sheets['Historial Constancias']

        # --- Definir Formatos ---

        # Formato para los encabezados
        header_format = workbook.add_format({
            'bold': True,
            'font_size': 11,
            'text_wrap': True,
            'valign': 'vcenter',
            'border': 1,
            'align' : 'center',
            'bg_color': '#9CEF00'
        })

        # Formato para los datos generales
        data_format = workbook.add_format({
            'font_size': 11,
            'text_wrap': True,
            'valign': 'top'
            #'border': 1
        })

        # --- Formato para las celdas de fecha ---
        date_format = workbook.add_format({
            'font_size': 11,
            'text_wrap': True,
            'valign': 'top',
            #'border' : 1,
            'num_format': 'dd/mm/yyyy' # Puedes cambiar 'dd/mm/yyyy' a 'yyyy-mm-dd', 'm/d/yy', etc.
        })

        # --- Aplicar Formatos y Ancho de Columnas ---
        for i, col in enumerate(df_final.columns):
            # Calcular el ancho máximo necesario para la columna
            header_len = len(col)
            col_series_str = df_final[col].astype(str)
            max_data_len = col_series_str.map(len).max() if not col_series_str.empty else 0
            max_len = max(header_len, max_data_len) + 5

            # --- Aplicar formato de fecha condicionalmente ---
            if col in ['fecha_asignada', 'fecha_vigencia']:
                current_data_format = date_format # Aplicar el formato de fecha
            else:
                current_data_format = data_format # Aplicar el formato de datos general

            # APLICAR ANCHO Y EL FORMATO DE DATOS PREDETERMINADO (o de fecha) PARA LA COLUMNA
            worksheet.set_column(i, i, max_len, current_data_format)

            # SOBRESCRIBIR el formato del ENCABEZADO de esta columna con su formato específico
            worksheet.write(0, i, col, header_format)

        # --- Autofiltros a los encabezados ---
        # La función autofilter toma (fila_inicio, columna_inicio, fila_fin, columna_fin)
        num_columns = len(df_final.columns)
        worksheet.autofilter(0, 0, 0, num_columns - 1)

        writer.close()
        print(f"\nListo, datos consolidados a excel: {outpath_xlsx}\n")

        # abrir archivo / validar cambios
        try:
            os.startfile(outpath_xlsx)
            print(f"\nArchivo abierto: {outpath_xlsx}, validar cambios\n")
        except Exception:
            pass # No se puede abrir en sistemas que no sean Windows

    except Exception as e:
        print(f"Error al exportar a excel: {e}")

    # Exportacion a CSV
    try:
        df_final.to_csv(outpath_csv, index=False, encoding='utf-8')
        print(f"\nListo, datos consolidados a csv: {outpath_csv}\n")
    except Exception as e:
        print(f"\nError al exportar a csv: {e}\n")

def main():
    """
    Funcion principal que orquesta el proceso de ETL de las constancias.
    """
    config = Config()

    def _cargar_set_registros_procesados(config: Config):
        if os.path.exists(config.outpath_processed_files_log):
            try:
                with open(config.outpath_processed_files_log, 'r', encoding='utf-8') as f:
                    for line in f:
                        path = line.strip()
                        if path:
                            config.processed_files_set_in_memory.add(path)
                print(f"Cargadas {len(config.processed_files_set_in_memory)} rutas iniciales en memoria desde: '{config.outpath_processed_files_log}'")
            except Exception as e:
                print(f"Advertencia: No se pudo cargar el log de archivos procesados inicial desde '{config.outpath_processed_files_log}'. Error: {e}")
        else:
            print(f"No se encontró el log de archivos inicial en '{config.outpath_processed_files_log}'. Se inicia con un set vacío.")

    _cargar_set_registros_procesados(config)

    # 1. Cargar la lista de archivos excluidos generada por el script no diario
    lista_archivos_para_procesar = cargar_rutas_archivos_desde_archivo(config.file_lista_pdfs_nuevos_no_excluidos)

    # 2. Procesar los archivos de constancias para extraer datos
    # Pasar la config a extraer_datos_constancia a través de procesar_archivos_constancias
    datos_conjunto_excluidos = procesar_archivos_constancias(lista_archivos_para_procesar, config)

    # 3. Cargar datos de empleados (HC)
    df_hc = cargar_data_hc(config.file_hc_table, config.vocales_acentos)

    # 4. Convertir datos extraídos a DataFrame, limpiar y fusionar con HC
    df_constancias_merged = procesar_y_mergear_constancias(datos_conjunto_excluidos, df_hc, config.vocales_acentos)

    if df_constancias_merged.empty:
        print("El DataFrame resultante está vacío. Terminando el proceso.")
        return

    # 5. Identificar y reportar constancias sin número de empleado
    identificar_y_reportar_constancias_sin_coincidencia(df_constancias_merged, config.folder_data_processed)

    # 6. Normalizar fechas y asignar estado de vigencia, y crear 'nombre_archivo_nuevo'
    df_final = normalizar_y_categorizar_fechas(df_constancias_merged, config.mapeo_meses, config.vocales_acentos)

    # 7. Organizar los archivos PDF en carpetas por empleado
    organizar_archivos_pdf(df_final, config.outpath_constancias_pdfs, config)

    # 8. Exportar resultados
    exportar_resultados(df_final, config.outpath_xlsx, config.outpath_csv)

    # Guardar el set único de archivos procesados a disco
    _guardar_registro_procesado_a_disco(config)

if __name__ == "__main__":
    main()