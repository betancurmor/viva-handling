import os
import stat
import time
import fitz
import re
import pandas as pd
import shutil
import unicodedata
import numpy as np
from datetime import datetime

from .config import Config
from .generador_lista_no_excluidos import _cargar_set_registros_procesados

def _añadir_set_procesado_en_memoria(file_path: str, config: Config):
    """
    Añade una ruta de archivo al conjunto de archivos procesados en memoria.
    El set automáticamente maneja la unicidad.
    """
    config.processed_files_set_in_memory.add(file_path)

def _guardar_registro_procesado_a_disco(config: Config):
    """
    Guarda todas las rutas unicas del set en memoria al archivo de registro.
    Este archivo se reescribe completamente.
    """
    try:
        with open(config.outpath_processed_files_log, 'w', encoding='utf-8') as f:
          for path in sorted(list(config.processed_files_set_in_memory)):
              f.write(f"{path}\n")
        print(f"Registro de archivo procesados actualizado con {len(config.processed_files_set_in_memory)} rutas unicas en: '{config.outpath_processed_files_log}'")
    except Exception as e:
        print(f"ERROR: No se pudo guardar el registro de archivos procesados en: '{config.outpath_processed_files_log}'. Error: {e}")

def rmtree_onerror_retry(func, path, exc_info):
    """
    Función de manejo de errores para shutil.rmtree.
    Si el error es por PermissionError, intenta cambiar los permisos del archivo
    a escribible y reintenta la operación. Si no es PermissionError, propaga la excepción.
    """
    ex_type, ex_value, ex_traceback = exc_info
    if ex_type is PermissionError:
        print(f"  - DEBUG: Permiso denegado para '{path}'. Intentando cambiar permisos...")
        try:
            os.chmod(path, stat.S_IWUSR)
            func(path)
            print(f"  - DEBUG: Permisos cambiados y operación reintentada en '{path}'.")
        except Exception as retry_e:
            print(f"  - ADVERTENCIA: Falló el reintento después de cambiar permisos en '{path}': {retry_e}")
            raise
    else:
        raise

def mover_carpetas_bajas(config: Config): # Acepta el objeto Config
    """
    Identifica las carpetas de empleados en la ruta de activos que corresponden a
    empleados con estatus 'BAJA' según hc_table.csv, y las mueve a la carpeta de bajas.
    Esta función debe ejecutarse antes de procesar nuevas constancias para evitar duplicados.
    """
    print("\n[SCRIPT NO DIARIO] Iniciando la verificación y movimiento de carpetas de empleados BAJA...")

    df_hc = pd.DataFrame()
    try:
        df_hc = pd.read_csv(config.hc_table_path, encoding='utf-8') # Usa config.hc_table_path
        df_hc['#emp'] = df_hc['#emp'].astype('string').str.strip()
        df_hc['estatus'] = df_hc['estatus'].astype('string').str.upper().str.strip()
    except FileNotFoundError:
        print(f"Advertencia: No se encontró 'hc_table.csv' en '{config.hc_table_path}'. No se moverán carpetas de bajas.")
        return
    except Exception as e:
        print(f"Error al cargar 'hc_table.csv' para mover carpetas de bajas: {e}. No se moverán carpetas.")
        return

    baja_emp_set = set(df_hc[df_hc['estatus'] == 'BAJA']['#emp'].unique())
    if not baja_emp_set:
        print("No se encontraron empleados con estatus 'BAJA' en 'hc_table.csv'. Saltando movimiento de carpetas.")
        return

    source_root_active = config.onedrive_certs_active # Usa config.onedrive_certs_active
    destination_root_bajas = config.onedrive_certs_bajas # Usa config.onedrive_certs_bajas

    os.makedirs(destination_root_bajas, exist_ok=True)

    moved_count = 0
    skipped_count = 0
    error_count = 0

    if not os.path.exists(source_root_active):
        print(f"Advertencia: La carpeta de certificados activos '{source_root_active}' no existe. No hay carpetas para mover.")
        return

    for folder_name in os.listdir(source_root_active):
        current_folder_path = os.path.join(source_root_active, folder_name)

        if not os.path.isdir(current_folder_path):
            continue

        if current_folder_path == destination_root_bajas:
            continue

        emp_id_str = None
        try:
            if folder_name.isdigit() and int(folder_name) != 0:
                emp_id_str = folder_name.strip()
            else:
                skipped_count += 1
                continue
        except ValueError:
            skipped_count += 1
            continue

        if emp_id_str in baja_emp_set:
            target_folder_path = os.path.join(destination_root_bajas, folder_name)

            # --- Fase 1: Intentar eliminar la carpeta existente en BAJAS con reintentos ---
            max_retries = 5
            current_retry = 0
            deletion_succeeded = False

            if os.path.exists(target_folder_path):
                print(f"  - INFO: Carpeta '{folder_name}' ya existe en destino de BAJAS. Intentando eliminarla para reemplazarla.")
                while current_retry < max_retries and not deletion_succeeded:
                    try:
                        shutil.rmtree(target_folder_path, onerror=rmtree_onerror_retry)
                        print(f"  - INFO: Carpeta '{folder_name}' eliminada exitosamente del destino de BAJAS.")
                        deletion_succeeded = True
                    except PermissionError as e_perm:
                        current_retry += 1
                        print(f"  - ADVERTENCIA: Permiso denegado al eliminar '{target_folder_path}' (Intento {current_retry}/{max_retries}). Reintentando en 0.5 segundos...")
                        time.sleep(0.5)
                    except Exception as e_rmtree:
                        print(f"  - ERROR: Falló la eliminación de la carpeta existente en BAJAS '{target_folder_path}': {e_rmtree}. Este error probablemente impide el movimiento.")
                        error_count += 1
                        break

                if not deletion_succeeded:
                    print(f"  - ERROR: No se pudo eliminar la carpeta '{target_folder_path}' después de {max_retries} intentos. Saltando el movimiento para este empleado.")
                    error_count += 1
                    continue

            # --- Fase 2: Intentar mover la carpeta del activo a Bajas ---
            try:
                shutil.move(current_folder_path, destination_root_bajas)
                print(f"  - MOVIO: Carpeta de empleado '{folder_name}' a '{destination_root_bajas}'.")
                moved_count += 1
            except FileNotFoundError:
                print(f"  - ERROR: Carpeta de origen no encontrada para mover: '{current_folder_path}'. Saltando.")
                error_count += 1
            except Exception as e_move:
                print(f"  - ERROR: Falló el movimiento de la carpeta activa '{current_folder_path}' a '{destination_root_bajas}': {e_move}. Esto podría ocurrir si la carpeta de origen también está bloqueada.")
                error_count += 1
        else:
            skipped_count += 1

    print(f"\n[SCRIPT NO DIARIO] Verificación y movimiento de carpetas BAJA completado:")
    print(f"  - Total de carpetas movidas: {moved_count}")
    print(f"  - Total de carpetas saltadas (no BAJA o no numéricas): {skipped_count}")
    print(f"  - Total de errores durante el movimiento: {error_count}\n")

def cargar_rutas_archivos_desde_archivo(file_name):
    """
    Cargar una lista de rutas de archivos desde un archivo de texto,
    donde cada línea contiene una ruta de archivo y su tipo (standalone/grouped).
    Retorna una lista de tuplas `(ruta, es_agrupado)`.
    """
    loaded_paths_with_flags = []
    try:
        if not os.path.exists(file_name):
            print(f"Advertencia: El archivo {file_name} no se encontro. No hay archivos para cargar.")
            return []
        with open(file_name, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('|')
                if len(parts) == 2:
                    path = parts[0]
                    is_grouped = (parts[1].lower() == 'grouped')
                    loaded_paths_with_flags.append((path, is_grouped))
                elif len(parts) == 1 and parts[0]:
                    print(f"Advertencia: Formato de línea '{line.strip()}' inesperado. Asumiendo standalone.")
                    loaded_paths_with_flags.append((parts[0], False))
        print(f"Se cargaron {len(loaded_paths_with_flags)} rutas de archivos desde {file_name}.")
    except Exception as e:
        print(f"Error al cargar rutas de archivos desde {file_name}: {e}")
    return loaded_paths_with_flags

def extraer_datos_constancia(ruta_pdf, config: Config, original_source_path: str = None): # Acepta el objeto Config
    """
    Esta función recibe la ruta de un archivo PDF y extrae los datos relevantes de la constancia.
    `original_source_path` es la ruta del archivo fuente original (agrupado o standalone)
    del cual se deriva esta `ruta_pdf` (que puede ser un archivo temporal).
    """

    if original_source_path is None:
        original_source_path = ruta_pdf

    file_name = os.path.basename(ruta_pdf)
    datos = {
        "nombre_archivo" : file_name,
        "ruta_original" : ruta_pdf,
        "original_source_path": original_source_path,
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

    constancia_type = "UNKNOWN"

    for n in config.nombres_archivos_sat: # Usa config.nombres_archivos_sat
        if n.lower() in texto_extraido.lower():
            constancia_type = "SAT"
            break

    if constancia_type == "UNKNOWN":
        for n in config.nombres_archivos_sms: # Usa config.nombres_archivos_sms
            if n.lower() in texto_extraido.lower():
                constancia_type = "SMS"
                break

    if constancia_type == "UNKNOWN":
        for n in config.nombres_archivos_avsec: # Usa config.nombres_archivos_avsec
            if n.lower() in texto_extraido.lower():
                constancia_type = "AVSEC"
                break

    # --- Procesar datos basándose en el tipo identificado ---
    if constancia_type == "SAT":
        datos['Curso'] = 'SAT'

        patron_nombre = r"(?:Otorga la presente constancia a:|Otorga el presente reconocimiento a:)\s*\n*(.*?)\s*\n*(?:Por haber concluido satisfactoriamente el curso|POR HABER CONCLUIDO SATISFACTORIAMENTE EL CURSO)"
        coincidencia_nombre = re.search(patron_nombre, texto_extraido, re.DOTALL | re.IGNORECASE)
        if coincidencia_nombre:
            datos['Nombre'] = coincidencia_nombre.group(1).strip()

        patron_curso = r"Por haber concluido satisfactoriamente el curso\s*\n*(.*?)(?=\s*[\s•]*CONTENIDO TEMÁTICO:?|\s*\n*Impartido en)"
        coincidencia_curso = re.search(patron_curso, texto_extraido, re.DOTALL | re.IGNORECASE)
        if coincidencia_curso:
            datos['Curso'] = coincidencia_curso.group(1).strip()

        patron_fecha = r"Impartido en .*?(?:el;?|del)\s*(.*?)(?=\n(?:[A-Z][a-zA-ZáéíóúÁÉÍÓÚüÜñÑ\s]+)?(?:Duración|Modalidad)|$)"
        coincidencia_fecha = re.search(patron_fecha, texto_extraido, re.DOTALL | re.IGNORECASE)
        if coincidencia_fecha:
            datos['Fecha'] = coincidencia_fecha.group(1).strip()
        if 'contenido' in datos['Fecha'].lower():
            patron_fecha_alt = r"Impartido en.*?el\s*(\d{1,2}\s*de\s*[a-zñáéíóúü]+\s*\d{4})(?=\s*CONTENIDO TEMATICO)"
            coincidencia_fecha_alt = re.search(patron_fecha_alt, texto_extraido, re.IGNORECASE)
            if coincidencia_fecha_alt:
                datos['Fecha'] = coincidencia_fecha_alt.group(1).strip()

        patron_instructor = r"(.+?)\s*\n*Instructor"
        coincidencia_instructor = re.findall(patron_instructor, texto_extraido, re.DOTALL | re.IGNORECASE)
        if coincidencia_instructor:
            last_candidate = coincidencia_instructor[-1].strip()
            lines = [line.strip() for line in last_candidate.split('\n') if line.strip()]
            if lines:
                datos["Instructor"] = lines[-1]

        patron_grupo = r"Grupo:\s*([A-Za-z0-9.]+(?:[\s-][A-Za-z0-9.]+)*[\s-]*\d{2})"
        coincidencia_grupo = re.search(patron_grupo, texto_extraido, re.DOTALL | re.IGNORECASE)
        if coincidencia_grupo:
            datos['Grupo'] = coincidencia_grupo.group(1).strip()
        else:
            patron_grupo_alt = r"\bAVSEC-\d{4}-\d{2}\b"
            coincidencia_grupo_alt = re.search(patron_grupo_alt, texto_extraido)
            if coincidencia_grupo_alt:
                datos['Grupo'] = coincidencia_grupo_alt.group(0).strip()

    elif constancia_type == "SMS":
        datos['Curso'] = 'SMS'

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
            else:
                patron_nombre_sms = r"Seguridad\s+Aérea\s*\n+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)+)"
                coincidencia_nombre_sms = re.search(patron_nombre_sms, texto_extraido, re.IGNORECASE)
                if coincidencia_nombre_sms:
                    datos['Nombre'] = coincidencia_nombre_sms.group(1).strip()

        if "(sms)" in datos['Nombre'].lower():
            patron_nombre_inicio = r"^\s*([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑa-záéíóúñ]+)+)\s*\n+Impartido\s+"
            coincidencia_nombre_inicio = re.search(patron_nombre_inicio, texto_extraido, re.MULTILINE)
            if coincidencia_nombre_inicio and coincidencia_nombre_inicio.group(1).strip():
                datos['Nombre'] = re.sub(r'\s+', ' ', coincidencia_nombre_inicio.group(1)).strip()
            else:
                first_line_text = texto_extraido.split('\n')[0] if texto_extraido else ''
                patron_nombre_primera_linea = r"^\s*([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)+)\s*$"
                coincidencia_nombre_primera_linea = re.search(patron_nombre_primera_linea, first_line_text)
                if coincidencia_nombre_primera_linea:
                    nombre_limpio_3 = re.sub(r'\s+', ' ', coincidencia_nombre_primera_linea.group(1))
                    datos['Nombre'] = nombre_limpio_3.strip()

        patron_curso = r"(inicial\s+de\s+Safety\s+Management\s+System\s+\(SMS\)|recurrente\s+de\s+Safety\s+Management\s+System\s+\(SMS\)|Safety\s+Management\s+System\s+\(SMS\))"
        coincidencia_curso = re.search(patron_curso, texto_extraido, re.IGNORECASE)
        if coincidencia_curso:
            curso_limpio = re.sub(r'\s+', ' ', coincidencia_curso.group(0))
            datos['Curso'] = curso_limpio.replace('.', '').strip().capitalize()

        patron_fecha_1 = re.compile(
            r"Impartido\s+el\s+(\d{1,2}\s+(?:de|del)\s+[a-zñáéíóúü]+\s+(?:de|del)?\s*\d{4})",
            re.IGNORECASE
            )
        coincidencia_fecha = patron_fecha_1.search(texto_extraido)

        if coincidencia_fecha:
            datos['Fecha'] = re.sub(r'\s+', ' ', coincidencia_fecha.group(1)).replace('del', 'de').strip()
        else:
            patron_fecha_2 = re.compile(
                r"Impartido\s+en.*?el\s+(\d{1,2}\s+(?:de|del)\s+[a-zñáéíóúü]+\s+(?:de|del)?\s*\d{4})",
                re.IGNORECASE
            )
            coincidencia_fecha = patron_fecha_2.search(texto_extraido)
            if coincidencia_fecha:
                fecha_limpia = re.sub(r'\s+', ' ', coincidencia_fecha.group(1)).replace('del', 'de').strip()
                datos['Fecha'] = fecha_limpia

        patron_grupo_n = r"(SMS[\s-]N-\d{3,4}-\d{2})"
        coincidencia_grupo = re.search(patron_grupo_n, texto_extraido)
        if coincidencia_grupo:
            datos['Grupo'] = coincidencia_grupo.group(1).strip()
        else:
            patron_grupo_sac = r"(SMS-SAC-\d{3,4}-\d{2})"
            coincidencia_grupo_sac = re.search(patron_grupo_sac, texto_extraido)
            if coincidencia_grupo_sac:
                datos['Grupo'] = coincidencia_grupo_sac.group(1).strip()
            else:
                patron_grupo_sms_directo = r"(SMS-\d{3,4}-\d{2})"
                coincidencia_grupo_sms_directo = re.search(patron_grupo_sms_directo, texto_extraido)
                if coincidencia_grupo_sms_directo:
                    datos['Grupo'] = coincidencia_grupo_sms_directo.group(1).strip()
                else:
                    patron_grupo_general = r"(SMS\s*–\s*[A-Z]+\s*–\s*\d+\s*-\s*\d+|SMS[\s-]?N-\d+-\d+|SMS-SAC-\d+-\d+)"
                    coincidencia_grupo_general = re.search(patron_grupo_general, texto_extraido)
                    if coincidencia_grupo_general:
                        datos['Grupo'] = coincidencia_grupo_general.group(1).strip()
                    else:
                        patron_sin_sms = r"Grupo:\s*(\d+-\d+|[A-Z]+-[A-Z]+-[A-Z]-\d+-\d+)"
                        coincidencia_sin_sms = re.search(patron_sin_sms, texto_extraido)
                        if coincidencia_sin_sms:
                            datos['Grupo'] = coincidencia_sin_sms.group(1).strip()
                        else:
                            patron_grupo_avsec_fallback_1 = r"Grupo:\s*((?:VH-)?(?:PRO-)?AVSEC-\d{3,4}-\d{2}\b)"
                            coincidencia_avsec_fallback = re.search(patron_grupo_avsec_fallback_1, texto_extraido, re.IGNORECASE)
                            if coincidencia_avsec_fallback:
                                datos['Grupo'] = coincidencia_avsec_fallback.group(1).strip()
                            else:
                                patron_grupo_avsec_fallback_2 = r"((?:VH-)?(?:PRO-)?AVSEC-\d{3,4}-\d{2}\b)"
                                coincidencia_avsec_fallback = re.search(patron_grupo_avsec_fallback_2, texto_extraido, re.IGNORECASE)
                                if coincidencia_avsec_fallback:
                                    datos['Grupo'] = coincidencia_avsec_fallback.group(1).strip()

        patron_instructor = r"([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)+)\s*\n*Instructor"
        coincidencia_instructor = re.search(patron_instructor, texto_extraido, re.DOTALL)
        if coincidencia_instructor:
            instructor_limpio = coincidencia_instructor.group(1).strip()
            datos["Instructor"] = re.sub(r'\s*Instructor$', '', instructor_limpio, flags=re.IGNORECASE).strip()
        else:
            patron_coordinador = r"([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)+)\s*\n*Coordinador de Entrenamiento"
            coincidencia_coordinador = re.search(patron_coordinador, texto_extraido, re.DOTALL | re.IGNORECASE)
            if coincidencia_coordinador:
                datos["Instructor"] = coincidencia_coordinador.group(1).strip()

    elif constancia_type == "AVSEC":
        datos['Curso'] = 'AVSEC'

        patron_nombre_avsec = r"^(.*?)\s+(?:Impartido en (?:la )?Ciudad de|Por haber concluido satisfactoriamente el curso|CONTENIDO TEMATICO|Curso:|Folio:|Viva Aerobus|Duración de:)"
        coincidencia_nombre = re.search(patron_nombre_avsec, texto_extraido, re.DOTALL | re.IGNORECASE)
        if coincidencia_nombre:
            datos['Nombre'] = coincidencia_nombre.group(1).strip()

        patron_curso_avsec = r"Por haber concluido satisfactoriamente el curso\s*\n*(.*?)(?:\s*Calificación obtenida:?|\s*Duración de:)"
        coincidencia_curso = re.search(patron_curso_avsec, texto_extraido, re.DOTALL | re.IGNORECASE)
        if coincidencia_curso:
            datos['Curso'] = coincidencia_curso.group(1).strip()

        patron_fecha_avsec = r"Impartido en .*?\s*el\s*(.*?)(?=\n|Duración|Modalidad)"
        coincidencia_fecha = re.search(patron_fecha_avsec, texto_extraido, re.DOTALL | re.IGNORECASE)
        if coincidencia_fecha:
            datos['Fecha'] = coincidencia_fecha.group(1).strip()

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

        patron_grupo_avsec = r"(?:Grupo:\s*|Curso:\s*\d{1,2}-\d{1,2}\s*\n*|\b)((?:PRO-)?AVSEC-\d{3,4}-\d{2}\b)"
        coincidencia_grupo = re.search(patron_grupo_avsec, texto_extraido, re.DOTALL | re.IGNORECASE)

        if coincidencia_grupo:
            datos['Grupo'] = coincidencia_grupo.group(1).strip()

    return datos

def procesar_archivos_constancias(lista_rutas_archivos, config: Config): # Acepta el objeto Config
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
            datos_extraidos = extraer_datos_constancia(full_pdf_path, config) # Pasa config a extraer_datos_constancia
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
        return ""

    text_normalized_accents = normalizar_acentos(text, vocales_acentos_map)

    invalid_chars = r'[<>:"/\\|?*\']'
    cleaned_text = re.sub(invalid_chars, '', text_normalized_accents)
    cleaned_text = re.sub(r'\s+', '_', cleaned_text).strip('_')

    return cleaned_text

def normalizar_acentos(texto, vocales_acentos_map: dict):
    """
    Normaliza acentos en una cadena de texto.
    """
    if not isinstance(texto, str):
        return texto

    texto_procesado = unicodedata.normalize('NFC', texto)

    for acento, sin_acento in vocales_acentos_map.items():
        texto_procesado = texto_procesado.replace(acento, sin_acento)

    return texto_procesado

def homologar_curso(curso_raw: str):
    """Homologa el nombre de un curso a una de las categorias predefinidas: 'SAT(Rampa)', 'SAT(Operador)', 'SAT(ASC)', 'AVSEC', 'SMS'
    """

    if not isinstance(curso_raw, str):
        return "OTRO"

    curso_lower = curso_raw.lower()

    if 'cabin search' in curso_lower:
        return 'Cabin Search'

    if 'prescreening of passengers (trafico)' in curso_lower:
        return 'P.P(Trafico)'

    if 'servicio de apoyo en tierra' in curso_lower or 'servicios de apoyo en tierra' in curso_lower:
        if 'rampa' in curso_lower or 'agente de rampa' in curso_lower:
            return 'SAT(Rampa)'
        if 'operador autoprestacion' in curso_lower:
            return 'SAT(Operador Autoprestacion)'
        if 'operador' in curso_lower:
            return 'SAT(Operador)'
        if 'asesor de servicio al cliente' in curso_lower or 'asesor de servicio a cliente' in curso_lower or 'asc' in curso_lower:
            return 'SAT(ASC)'
        return 'SAT(General)'

    if 'avsec' in curso_lower or 'seguridad de la aviacion' in curso_lower:
        return 'AVSEC'

    if 'safety management system' in curso_lower or 'sms' in curso_lower:
        return 'SMS'

    if 'personal perteneciente' in curso_lower or 'personal permaneciente' in curso_lower:
        if 'rampa autoprestacion' in curso_lower:
            return 'SAT(Rampa Autoprestacion)'
        if 'rampa' in curso_lower:
            return 'SAT(Rampa)'
        if 'trafico' in curso_lower:
            return 'SAT(Trafico)'
        if 'csa autoprestacion' in curso_lower:
            return 'SAT(CSA Autoprestacion)'

    return 'OTRO'

def dividir_pdf_constancia_agrupado(grouped_pdf_path: str, config: Config): # Acepta el objeto Config
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

            if re.search(patron_otorgamiento_curso, text, re.DOTALL | re.IGNORECASE):
                is_certificate_page = True

            if not is_certificate_page and re.search(patron_avsec_footer, text, re.IGNORECASE):
                is_certificate_page = True

            if not is_certificate_page:
                print(f"DEBUG: Página {i+1} de '{os.path.basename(grouped_pdf_path)}' no parece ser una constancia válida. Saltando.")
                continue

            output_pdf = fitz.open()
            output_pdf.insert_pdf(doc, from_page=i, to_page=i)

            original_base_name = os.path.splitext(os.path.basename(grouped_pdf_path))[0]
            temp_filename_base = f"temp_{original_base_name}_page_{i+1}.pdf"
            temp_filepath = os.path.join(config.temp_split_pdfs_folder, temp_filename_base) # Usa config.temp_split_pdfs_folder

            unique_temp_filepath = temp_filepath
            count = 1
            while os.path.exists(unique_temp_filepath):
                temp_filename_base_without_ext, temp_ext = os.path.splitext(temp_filename_base)
                unique_temp_filepath = os.path.join(config.temp_split_pdfs_folder, f"{temp_filename_base_without_ext}_{count}{temp_ext}") # Usa config.temp_split_pdfs_folder
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

def normalizar_mes(mes_str, mapeo_meses_map: dict):
    """Normaliza el nombre del mes (en español) a su número de mes."""
    return mapeo_meses_map.get(mes_str.lower(), None)

def parse_fecha_inicio(fecha_texto, mapeo_meses_map: dict):
    """
    Extrae y parsea la fecha de inicio de una cadena de texto de fecha,
    manejando diferentes formatos y rangos.
    """
    if pd.isna(fecha_texto) or not isinstance(fecha_texto, str):
        return pd.NaT

    fecha_texto_original = fecha_texto.strip()
    fecha_texto_lower = fecha_texto_original.lower()

    year_str = None
    years_found = re.findall(r"(\d{4})", fecha_texto_original)
    if years_found:
        year_str = years_found[-1]
    else:
        return pd.NaT

    dia_str = None
    mes_str_raw = None

    patron_full_date_con_de = re.compile(
        r"(\d{1,2})\s*de\s*([a-zñáéíóúü]+)(?:\s*de)?",
        re.IGNORECASE
    )
    match = patron_full_date_con_de.search(fecha_texto_lower)
    if match:
        dia_str = match.group(1)
        mes_str_raw = match.group(2)
    else:
        patron_al_rango = re.compile(
            r"^(\d{1,2})\s*(?:al|a)\s*\d{1,2}\s*([a-zñáéíóúü]+)",
            re.IGNORECASE
        )
        match = patron_al_rango.search(fecha_texto_lower)
        if match:
            dia_str = match.group(1)
            mes_str_raw = match.group(2)
        else:
            patron_guion_rango = re.compile(
                r"^(\d{1,2})(?:[-\s]?\d{1,2})?[-\s]?([a-zñáéíóúü]+)",
                re.IGNORECASE
            )
            match = patron_guion_rango.search(fecha_texto_lower)
            if match:
                dia_str = match.group(1)
                mes_str_raw = match.group(2)
            else:
                patron_dia_mes_abreviado = re.compile(
                    r"^(?:[a-zñáéíóúü]{2,4}[-\s]?)?(\d{1,2})[-\s]?([a-zñáéíóúü]{3,})",
                    re.IGNORECASE
                )
                match = patron_dia_mes_abreviado.search(fecha_texto_lower)
                if match:
                    dia_str = match.group(1)
                    mes_str_raw = match.group(2)
                else:
                    patron_simple_no_de = re.compile(
                        r"(\d{1,2})\s*([a-zñáéíóúü]+)",
                        re.IGNORECASE
                    )
                    match = patron_simple_no_de.search(fecha_texto_lower)
                    if match:
                        dia_str = match.group(1)
                        mes_str_raw = match.group(2)

    if dia_str and mes_str_raw:
        mes_num = normalizar_mes(mes_str_raw, mapeo_meses_map)
        if mes_num is not None:
            try:
                return datetime.strptime(f"{dia_str} {mes_num} {year_str}", '%d %m %Y')
            except ValueError:
                pass

    return pd.NaT

def cargar_data_hc(path_hc_table: str, vocales_acentos_map: dict):
    """
    Carga la tambla de empleado (HC), aplica normalizaciones y crea una columna
    adicional con el nombre en formato "NOMBRE APELLIDO(P) APELLIDO(M)" para mejorar las coincidencias.
    """
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

        df_hc['nombre_completo'] = df_hc['nombre_completo'].str.replace('REYES nan ALEJANDRO', 'REYES ALEJANDRO', regex=False)


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

        if 'original_source_path' in df_constancias.columns:
            df_constancias['original_source_path'] = df_constancias['original_source_path'].astype('string').fillna('')
        else:
            df_constancias['original_source_path'] = ''

    # upper - nombre_completo - merge
    df_constancias['nombre_completo'] = df_constancias['nombre_completo'].str.upper().str.strip()

    # Asegurar consistencia con df_hc
    df_constancias['nombre_completo'] = df_constancias['nombre_completo'].apply(lambda x: normalizar_acentos(x, vocales_acentos_map))

    # Recuento de filas sin filtros
    recuento_filas_inicial = len(df_constancias)
    print(f"\[INFO PROCESAMIENTO] Registros antes de filtros de negocio: {recuento_filas_inicial}")

    # --- Aplicar filtros y contar descartados ---
    # Instructor excluido
    df_filtrado = df_constancias[df_constancias['instructor'] != 'ENRIQUE ORTIZ HERNANDEZ'] # Asegura mayúsculas para la comparación
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
        'OP 2024 PORTOS GAMEZ HECTOR ABRAHAM (1).pdf',
        'OP-0011-25.pdf',
        'PRUDENCIO CAPACIDAD RTAR.pdf',
        'TTT 2024 GUERRERO DE LA GARZA FRANCISCO.pdf',
        'TTT 2024 GONZALEZ ESCALANTE EDUARDO SILVANO.pdf',
        'AVSEC SALOMON CASTILLO ANA KAREN.pdf'
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
    df_constancias_deduplicated = df_constancias.drop_duplicates(keep='first')
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
    df_constancias['nombre_completo'] = df_constancias['nombre_completo'].str.replace('MENDOZA GAONA ROCIO YAMILETH', 'MENDOZA GAONA ROCIO YAMILET', regex=False)
    df_constancias['nombre_completo'] = df_constancias['nombre_completo'].str.replace('DULCE MARTINEZ ORTIZ', 'DULCE AMADA MARTINEZ ORTIZ')
    df_constancias['nombre_completo'] = df_constancias['nombre_completo'].str.replace('SOTO MORSLES JUANA MARIA', 'SOTO MORALES JUANA MARIA', regex=False)
    df_constancias['nombre_completo'] = df_constancias['nombre_completo'].str.replace('OFELIA CLEMENTINA CORONADO CARRIZALEZ', 'OFELIA CLEMENTINA CORONADO CARRIZALES', regex=False)
    df_constancias['nombre_completo'] = df_constancias['nombre_completo'].str.replace('SAGRARIO NUNEZ TOVAR', 'SAGRARIO NUÑEZ TOVAR', regex=False)
    df_constancias['nombre_completo'] = df_constancias['nombre_completo'].str.replace('REYES O ALEJANDRO', 'REYES ALEJANDRO', regex=False)
    df_constancias['nombre_completo'] = df_constancias['nombre_completo'].str.replace('MONTREAL SALAS HUGO HUMBERTO', 'MONRREAL SALAS HUGO HUMBERTO', regex=False)
    df_constancias['nombre_completo'] = df_constancias['nombre_completo'].str.replace('ABELDAÑO LEAL REGINA SAORI', 'ALBELDAÑO LEAL REGINA SAORI', regex=False)
    df_constancias['nombre_completo'] = df_constancias['nombre_completo'].str.replace('IBARRA TREVIÑO BRAYAN ARTURO', 'IBARRA TREVIO BRAYAN ARTURO', regex=False)
    df_constancias['nombre_completo'] = df_constancias['nombre_completo'].str.replace('IBARRA TREVINO BRAYAN ARTURO', 'IBARRA TREVIO BRAYAN ARTURO', regex=False)
    df_constancias['nombre_completo'] = df_constancias['nombre_completo'].str.replace('MICHELE ALFARO PALOMEQUE', 'MICHELLE ALFARO PALOMEQUE', regex=False)
    df_constancias['nombre_completo'] = df_constancias['nombre_completo'].str.replace('KEVEIN ENRIQUE MAAS ANAYA', 'KEVIN ENRIQUE MAAS ANAYA', regex=False)
    df_constancias['nombre_completo'] = df_constancias['nombre_completo'].str.replace('FLOR ALEXANDRA CRUZ PEREZ', 'FLOR ALEXSANDRA CRUZ PEREZ', regex=False)
    df_constancias['nombre_completo'] = df_constancias['nombre_completo'].str.replace('JESUS YAIR ORTA SAUCEDA', 'JESUS YAHIR ORTA SAUCEDA', regex=False)


    # Modificacion de fecha manual por error en constancia.
    df_constancias.loc[
        (df_constancias['nombre_archivo'] == 'OP 2024 SERRATO VELAZQUEZ VANESSA ESMERALDA.pdf') & (df_constancias['fecha'] == '29 JUNIO-2029.'),
        'fecha'
        ] = '29 JUNIO-2024.'

    df_constancias.loc[
        (df_constancias['nombre_archivo'] == 'OP 2024 PORTOS GAMEZ HECTOR ABRAHAM.pdf') & (df_constancias['fecha'] == '26 JUNIO-2026.'),
        'fecha'
        ] = '26 JUNIO-2024.'

    df_constancias.loc[
        (df_constancias['nombre_archivo'] == 'OP 2024 AGUILAR CORONADO JOSE ANGEL DE JESUS.pdf') & (df_constancias['fecha'] == '27 JUNIO-2027.'),
        'fecha'
        ] = '27 JUNIO-2024.'

    df_constancias.loc[
        (df_constancias['nombre_archivo'] == 'OP 2025 MONTREAL SALAS HUGO HUMBERTO.pdf') & (df_constancias['fecha'] == 'MONTREAL SALAS Hugo Humberto Febrero-2025.'),
        'fecha'
        ] = '25 FEBRERO-2025.'
    
    # --- DOBLE MERGE PARA MEJORAR COINCIDENCIAS DE NOMBRES

    # Primer merge con el formato "NOMBRE APELLIDO(P) APELLIDO(M)" en 'df_constancias' vs "NOMBRE APELLIDO(P) APELLIDO(M)" en 'df_hc'
    print("\nRealizando primer merge (Constancias: Nombre Apellido(P) Apellido(M) contra HC: Nombre Apellido(P) Apellido(M))")

    df_constancias_primer_merge = pd.merge(df_constancias,
                                     df_hc[['nombre_completo_invertido', '#emp', 'estatus']],
                                     left_on=['nombre_completo'], # Columna de constancias (Nombre Apellido),
                                     right_on=['nombre_completo_invertido'], # Columna invertida de HC (Nombre Apellido)
                                     how='left',
                                     suffixes=('', '_hc_pass1')) # Sufijo para evitar colisiones si hubiera otras columnas '#emp'

    # Renombrar '#emp_hc_pass1' a '#emp' para el primer pase y estatus
    df_constancias_primer_merge = df_constancias_primer_merge.rename(columns={'#emp_hc_pass1': '#emp', 'estatus_hc_pass1': 'estatus'})
    # Eliminar la columna de merge usada de df_hc
    df_constancias_primer_merge = df_constancias_primer_merge.drop(columns=['nombre_completo_invertido'], errors='ignore')

    # Identificar registros que no se encontraron en el primer merge (donde #emp es NaN)
    registros_sin_coincidencia = df_constancias_primer_merge[df_constancias_primer_merge['#emp'].isnull()]
    registros_coincidentes = df_constancias_primer_merge[df_constancias_primer_merge['#emp'].notnull()]

    print(f"  - Registros encontrados en el primer merge: {len(registros_coincidentes)}")
    print(f"  - Registros NO encontrados en el primer merge: {len(registros_sin_coincidencia)}")

    # Paso 2: Segundo merge para los registros no encontrados en el primer pase
    # Ahora intentamos con el formato "APELLIDOP APELLIDOM NOMBRE" (de df_constancias, si el nombre del PDF lo trae así)
    # vs "APELLIDOP APELLIDOM NOMBRE" (df_hc['nombre_completo'])

    if not registros_sin_coincidencia.empty:
        print("\nRealizando segundo merge (Constancias: Apellido(P) Apellido(M) Nombre contra HC: Apellido(P) Apellido(M) Nombre)")
        # Trabajar con una copia para evitar SettingWithCopyWarning
        df_para_segundo_merge = registros_sin_coincidencia.copy()

        df_constancias_segundo_merge = pd.merge(df_para_segundo_merge,
                                                df_hc[['nombre_completo', '#emp', 'estatus']], # Usar la columna original 'nombre_completo' de 'df_hc'
                                                left_on=['nombre_completo'], # Columna de Constancias (Apellidos-Nombre)
                                                right_on=['nombre_completo'], # Columna original de 'df_hc'
                                                how='left',
                                                suffixes=('_pass1', '_pass2'))

        # Ahora necesitamos consolidar los '#emp'
        # Donde '#emp_pass1' es nulo (es decir, no se encontró en el primer merge), usamos '#emp_pass2'
        df_constancias_segundo_merge['#emp'] = df_constancias_segundo_merge['#emp_pass1'].fillna(df_constancias_segundo_merge['#emp_pass2'])
        df_constancias_segundo_merge['estatus'] = df_constancias_segundo_merge['estatus_pass1'].fillna(df_constancias_segundo_merge['estatus_pass2'])

        # Eliminar las columnas temporales de los pases
        df_constancias_segundo_merge = df_constancias_segundo_merge.drop(columns=['#emp_pass1', '#emp_pass2', 'estatus_pass1', 'estatus_pass2'])

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

    for c in columns_text: # Utiliza las columnas de texto definidas previamente
        if c in df_constancias_merged.columns: # Asegurarse de que la columna existe
            df_constancias_merged[c] = df_constancias_merged[c].astype('string')

    print("\nDataFrame de constancias después del doble merge con HC:\n")
    df_constancias_merged.info()
    print(f"Total de filas en df_constancias_merged después de doble merge: {len(df_constancias_merged)}")
    print("\nConteo de empleados después del doble merge (0 = sin coincidencia):\n")
    print(df_constancias_merged['#emp'].value_counts(dropna=False))

    final_text_columns = ['nombre_archivo', 'ruta_original', 'nombre_completo', 'curso', 'fecha', 'instructor', 'grupo']
    for c in final_text_columns:
        if c in df_constancias_merged.columns:
            df_constancias_merged[c] = df_constancias_merged[c].astype('string')

    if 'ruta_original' in df_constancias_merged.columns:
        df_constancias_merged['ruta_original'] = df_constancias_merged['ruta_original'].astype('string').fillna('')
    else:
        df_constancias_merged['ruta_original'] = pd.Series([''] * len(df_constancias_merged), dtype='string')

    return df_constancias_merged

def identificar_y_reportar_constancias_sin_coincidencia(df_constancias_merged: pd.DataFrame, config: Config): # Acepta el objeto Config
    """
    Identifica constancias sin numero de empleado(#emp) asociado y las exporta a archivos.
    """
    constancias_sin_emp = df_constancias_merged[df_constancias_merged['#emp'] == 0]
    if not constancias_sin_emp.empty:
        print(f"\nAdvertencia: {len(constancias_sin_emp)} constancia(s) no pudieron ser asociadas a un numero de empleado '#emp'\n")
        for index, row in constancias_sin_emp.iterrows():
            print(f"Archivo: {row['nombre_archivo']} \nNombre empleado: {row['nombre_completo']}\n")

        # Outputs sin "#emp" - Usando rutas de config
        output_excel_path = config.outpath_xlsx_constancias_sin_emp
        output_csv_path = config.outpath_csv_constancias_sin_emp
        try:
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

def organizar_archivos_pdf(df_constancias_merged: pd.DataFrame, config: Config): # Solo recibe config
    """
    Organiza los archivos PDF copiándolos a carpetas individuales por número de empleado(#emp).
    Los empleados 'BAJA' van a una subcarpeta 'BAJAS'.
    Sobrescribe archivos existentes (no crea duplicados con sufijos).
    """
    outpath_base_activos = config.onedrive_certs_active # Obtiene de config
    outpath_base_bajas = config.onedrive_certs_bajas # Obtiene de config

    print(f"Iniciando organización de archivos. Destino base para ACTIVOS: {outpath_base_activos}")
    print(f"Destino para BAJAS: {outpath_base_bajas}")

    pdfs_organizados = 0
    pdfs_no_organizados_error_copia = 0
    pdfs_sin_num_emp_count = 0
    pdfs_bajas_organizados = 0
    pdfs_activos_organizados = 0

    if df_constancias_merged.empty:
        print("No hay constancias para organizar (DataFrame vacío).")
        print("\nOrganización de archivos terminada.\n")
        print("Total de PDFs organizados: 0")
        print("Total de archivos que fallaron al copiar: 0\n")
        return

    # Contar los PDFs que no tienen un número de empleado asignado (== 0)
    # y que no tienen estatus 'BAJA' para el reporte de carpeta '0'
    pdfs_sin_num_emp_count = len(df_constancias_merged[(df_constancias_merged['#emp'] == 0) & (df_constancias_merged['estatus'].str.upper() != 'BAJA')])

    for index, row in df_constancias_merged.iterrows():
        num_emp = str(row['#emp']) # Sera '0' si no hay coincidencia de '#emp'
        original_pdf_to_copy_path = row['ruta_original'] # Esta es la ruta del archivo a COPIAR (temporal o standalone)
        base_new_file_name_with_ext = row['nombre_archivo_nuevo'] # Este es el nombre base, sin sufijo aún
        estatus_empleado = row['estatus'].upper()

        # --- Ruta para el registro de archivos procesados (siempre el archivo fuente original) ---
        original_source_file_for_log = row['original_source_path']

        # Determinar la carpeta de destino basada en el estatus
        if estatus_empleado == 'BAJA':
            target_base_folder = outpath_base_bajas
            # Si es BAJA, incrementa este contador, independientemente de si tiene #emp=0
            pdfs_bajas_organizados += 1
        else: # Incluye 'ALTA' y 'DESCONOCIDO'. Los '#emp == 0' también caen aquí, a menos que sean 'BAJA'.
            target_base_folder = outpath_base_activos
            if num_emp != '0': # Solo contar activos si tienen un #emp válido
                pdfs_activos_organizados += 1
            # else: pdfs_sin_num_emp_count ya se cuenta arriba de forma más precisa.

        # Verificar si la 'ruta_original' existe antes de intentar crear la carpeta y copiar
        if not os.path.exists(original_pdf_to_copy_path):
            print(f"ADVERTENCIA: Archivo de origen no encontrado en '{original_pdf_to_copy_path}'. Se salta.")
            pdfs_no_organizados_error_copia += 1
            continue

        # Crear la carpeta de destino (ej. 'Certificados Entrenamiento Viva Handling/12345' o 'BAJAS/54321')
        folder_emp = os.path.join(target_base_folder, num_emp)
        os.makedirs(folder_emp, exist_ok=True)

        # El nombre del archivo final es simplemente el 'nombre_archivo_nuevo'
        destino_pdf_path = os.path.join(folder_emp, base_new_file_name_with_ext)

        try:
            # Copiar el archivo. shutil.copy2 copia también metadatos como la fecha de modificación.
            shutil.copy2(original_pdf_to_copy_path, destino_pdf_path)
            pdfs_organizados += 1
            # IMPORTANTE: Añadir PATH ORIGINAL del documento FUENTE (agrupado o standalone) al log.
            _añadir_set_procesado_en_memoria(original_source_file_for_log, config)
        except FileNotFoundError:
            print(f"\nERROR: Archivo no encontrado en origen para copiar: '{original_pdf_to_copy_path}'\n")
            pdfs_no_organizados_error_copia += 1
        except Exception as e:
            print(f"\nERROR al copiar: '{original_pdf_to_copy_path}' a '{destino_pdf_path}': {e}\n")
            pdfs_no_organizados_error_copia += 1

    print(f"\nOrganización de archivos terminada.\n")
    print(f"Total de PDFs organizados (incluye Activos, Bajas y sin #emp): {pdfs_organizados}")
    print(f"  - PDFs de empleados ACTIVOS organizados: {pdfs_activos_organizados}")
    print(f"  - PDFs de empleados BAJAS organizados: {pdfs_bajas_organizados}")
    print(f"  - PDFs sin número de empleado (en carpeta '0' de Activos): {pdfs_sin_num_emp_count}")
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
    df_constancias_merged['fecha_constancia'] = df_constancias_merged['fecha'].apply(
        lambda x: parse_fecha_inicio(x, mapeo_meses_map))
    # fecha sin la hora, normalizarla
    df_constancias_merged['fecha_constancia'] = pd.to_datetime(df_constancias_merged['fecha_constancia'], errors='coerce').dt.normalize()

    # fecha_vigencia' (un año posterior a 'fecha normalizada')
    df_constancias_merged['fecha_vigencia'] = df_constancias_merged['fecha_constancia'] + pd.DateOffset(years=1)

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
        if pd.notna(row['fecha_constancia']):
            fecha_parte = row['fecha_constancia'].strftime('%d-%m-%Y')

        # Combinar las partes. Eliminar posibles guiones bajas dobles o al inicio/fin.

        new_name = f"{curso_parte}_{fecha_parte}_{nombre_completo_parte}.pdf"
        return re.sub(r'_{2,}', '_', new_name).strip('_')  # Eliminar guiones bajos dobles y en los extremos

    df_constancias_merged['nombre_archivo_nuevo'] = df_constancias_merged.apply(lambda row: generate_new_filename(row, vocales_acentos_map), axis=1)

    # Organizar columnas e incluir 'nombre_archivo_nuevo'
    df_final = df_constancias_merged[['nombre_archivo', 'nombre_archivo_nuevo', '#emp', 'nombre_completo', 'estatus', 'curso_homologado','curso', 'instructor', 'grupo', 'fecha', 'fecha_constancia', 'fecha_vigencia', 'estatus_vigencia', 'ruta_original', 'original_source_path']]

    # Eliminar duplicados basándose solo en las columnas especificadas
    columns_to_consider_for_duplicates = [
    'nombre_archivo_nuevo', '#emp', 'nombre_completo', 'estatus',
    'curso_homologado', 'curso', 'instructor', 'grupo', 'fecha_constancia']
    df_final = df_final.drop_duplicates(subset=columns_to_consider_for_duplicates)

    df_final = df_final.drop_duplicates()
    df_final = df_final.reset_index(drop=True)

    return df_final

def exportar_resultados(df_final: pd.DataFrame, config: Config): # Solo recibe config
    """
    Exporta el DataFrame final a archivos Excel y CSV. Si los archivos existen,
    concatena los nuevos datos, elimina duplicados y luego guarda el DataFrame combinado.
    """
    outpath_xlsx = config.outpath_xlsx_constancias # Obtiene de config
    outpath_csv = config.outpath_csv_constancias # Obtiene de config

    if df_final.empty:
        print("Data Frame vacío, no hay resultados para exportar.")
        return

    # Columnas a considerar para la deduplicación (identificador único de una constancia)
    deduplication_subset_cols = [
        'nombre_archivo_nuevo', '#emp', 'nombre_completo', 'curso_homologado', 'fecha_constancia'
    ]

    # Definir tipos de datos comunes para asegurar consistencia al leer archivos existentes.
    # Leer '#emp' como string para evitar problemas con valores mixtos o nulos durante la concatenación,
    # luego se convertirá a int al final.
    common_dtypes = {
        '#emp': 'string',
        'nombre_archivo': 'string',
        'nombre_archivo_nuevo': 'string',
        'nombre_completo': 'string',
        'estatus': 'string',
        'curso_homologado': 'string',
        'curso': 'string',
        'instructor': 'string',
        'grupo': 'string',
        'fecha': 'string',
        'estatus_vigencia': 'string',
        'ruta_original': 'string',
        'original_source_path': 'string'
    }
    date_cols = ['fecha_constancia', 'fecha_vigencia']

    # Función auxiliar para procesar y guardar datos en Excel o CSV
    def _process_and_save(df_new_data: pd.DataFrame, file_path: str, is_excel: bool):
        df_combined = df_new_data.copy()
        file_type = "Excel" if is_excel else "CSV"

        if os.path.exists(file_path):
            print(f"Cargando datos existentes de {file_type}: {file_path}")
            try:
                if is_excel:
                    df_existing = pd.read_excel(file_path, dtype=common_dtypes, parse_dates=date_cols)
                else: # CSV
                    df_existing = pd.read_csv(file_path, dtype=common_dtypes, parse_dates=date_cols, encoding='utf-8')

                # Asegurar que las columnas de fecha en los datos existentes estén en formato datetime
                for col in date_cols:
                    if col in df_existing.columns:
                        df_existing[col] = pd.to_datetime(df_existing[col], errors='coerce')

                # Convertir '#emp' a string en df_new_data para una concatenación consistente
                if '#emp' in df_new_data.columns:
                    df_new_data['#emp'] = df_new_data['#emp'].astype('string')

                # Alinear columnas y concatenar
                # Asegurar que todas las columnas de df_new_data estén presentes en df_existing,
                # añadiendo las que falten con valores NaN para evitar errores de concatenación.
                missing_cols_in_existing = set(df_new_data.columns) - set(df_existing.columns)
                for col in missing_cols_in_existing:
                    df_existing[col] = pd.NA # O un valor predeterminado adecuado

                df_combined = pd.concat([df_existing, df_new_data], ignore_index=True)

                print(f"Combinando con {len(df_existing)} registros existentes.")

                # Eliminar duplicados del DataFrame combinado
                initial_rows = len(df_combined)
                df_combined_deduplicated = df_combined.drop_duplicates(subset=deduplication_subset_cols, keep='first')
                rows_removed = initial_rows - len(df_combined_deduplicated)
                print(f"Eliminados {rows_removed} registros duplicados de {file_type}.")

                df_combined = df_combined_deduplicated

            except Exception as e:
                print(f"ADVERTENCIA: Error al cargar y combinar el archivo {file_type} existente '{file_path}': {e}. Se exportarán solo los nuevos datos.")
                # En caso de error, se procede solo con los nuevos datos
                df_combined = df_new_data.copy()

        # Asegurar que la columna '#emp' sea de tipo entero antes de la exportación final
        if '#emp' in df_combined.columns:
            # Primero, limpiar cualquier valor no numérico que pueda haber, reemplazándolos con NaN
            df_combined['#emp'] = pd.to_numeric(df_combined['#emp'], errors='coerce')
            # Luego, llenar NaN con 0 y convertir a entero
            df_combined['#emp'] = df_combined['#emp'].fillna(0).astype(int)

        # Ordenar el DataFrame final para una salida consistente
        try:
            sort_cols = [col for col in ['#emp', 'fecha_constancia', 'nombre_completo'] if col in df_combined.columns]
            if sort_cols:
                df_combined = df_combined.sort_values(by=sort_cols, ascending=[True, False, True], ignore_index=True)
        except Exception as sort_e:
            print(f"Advertencia: No se pudo ordenar el DataFrame antes de guardar: {sort_e}")

        # Exportar a Excel o CSV
        try:
            if is_excel:
                writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
                df_combined.to_excel(writer, sheet_name='Historial Constancias', index=False)

                workbook = writer.book
                worksheet = writer.sheets['Historial Constancias']

                # Definir formatos de celda
                header_format = workbook.add_format({
                    'bold': True, 'font_size': 11, 'text_wrap': True,
                    'valign': 'vcenter', 'border': 1, 'align' : 'center',
                    'bg_color': '#9CEF00'
                })
                data_format = workbook.add_format({
                    'font_size': 11, 'text_wrap': True, 'valign': 'top'
                })
                date_format = workbook.add_format({
                    'font_size': 11, 'text_wrap': True, 'valign': 'top',
                    'num_format': 'dd/mm/yyyy' # Formato de fecha
                })

                # Aplicar formatos y ancho de columnas
                for i, col in enumerate(df_combined.columns):
                    header_len = len(col)
                    col_series_str = df_combined[col].astype(str)
                    max_data_len = col_series_str.map(len).max() if not col_series_str.empty else 0
                    max_len = max(header_len, max_data_len) + 5

                    current_data_format = date_format if col in date_cols else data_format
                    worksheet.set_column(i, i, max_len, current_data_format)
                    worksheet.write(0, i, col, header_format)

                # Aplicar autofiltros
                num_columns = len(df_combined.columns)
                worksheet.autofilter(0, 0, 0, num_columns - 1)
                writer.close()
                print(f"\nListo, datos consolidados a {file_type}: {file_path}\n")

                # Intentar abrir el archivo (solo en sistemas Windows)
                try:
                    os.startfile(file_path)
                    print(f"Archivo abierto: {file_path}, validar cambios\n")
                except Exception:
                    pass # Ignorar error si no es Windows
            else: # CSV
                df_combined.to_csv(file_path, index=False, encoding='utf-8')
                print(f"\nListo, datos consolidados a {file_type}: {file_path}\n")
        except Exception as e:
            print(f"\nError al exportar a {file_type}: {e}\n")

    # Exportación a Excel
    _process_and_save(df_final, outpath_xlsx, is_excel=True)

    # Exportación a CSV
    _process_and_save(df_final, outpath_csv, is_excel=False)

def run_pdf_etl(config: Config):
    """
    Función principal que orquesta el proceso de ETL de las constancias.
    """
    print("\n--- INICIANDO ETL DE CONSTANCIAS PDF ---")
    config.processed_files_set_in_memory = _cargar_set_registros_procesados(config.outpath_processed_files_log) # Carga el log de archivos procesados en memoria

    # Limpiar la carpeta temporal al inicio de la ejecución
    if os.path.exists(config.temp_split_pdfs_folder):
        try:
            shutil.rmtree(config.temp_split_pdfs_folder)
            print(f"INFO: Carpeta temporal de PDFs divididos '{config.temp_split_pdfs_folder}' limpiada.")
        except Exception as e:
            print(f"ADVERTENCIA: No se pudo limpiar la carpeta temporal '{config.temp_split_pdfs_folder}' al inicio. Error: {e}")
    os.makedirs(config.temp_split_pdfs_folder, exist_ok=True) # Asegurarse de que exista después de limpiar o si no existía

    # Mover carpetas de empleados 'BAJA' ANTES de procesar nuevas constancias ---
    mover_carpetas_bajas(config)

    # 1. Cargar la lista de archivos (path, is_grouped_flag) desde el generador
    list_of_source_files_with_flags = cargar_rutas_archivos_desde_archivo(config.outpath_list_new_non_excluded_pdfs)

    all_extracted_data = [] # Recopila datos de todos los PDFs procesados (standalone o páginas divididas)

    print(f"\nIniciando procesamiento de {len(list_of_source_files_with_flags)} archivos fuente (incluyendo agrupados)...\n")
    total_files_processed_for_data_extraction = 0 # Cuenta los archivos fuente procesados (originales, no las páginas)
    total_grouped_pdfs_split = 0
    total_extracted_certificates = 0 # Cuenta las constancias individuales (páginas) extraídas

    for source_pdf_path, is_grouped in list_of_source_files_with_flags:
        if not os.path.exists(source_pdf_path):
            print(f"Advertencia: Archivo fuente no encontrado '{source_pdf_path}'. Saltando.")
            continue

        if is_grouped:
            print(f"Procesando PDF agrupado: {os.path.basename(source_pdf_path)}. Dividiendo...")
            temp_split_certs_paths = dividir_pdf_constancia_agrupado(source_pdf_path, config)
            total_grouped_pdfs_split += 1
            if temp_split_certs_paths:
                print(f"Extraídas {len(temp_split_certs_paths)} páginas de '{os.path.basename(source_pdf_path)}'.")
                for temp_path in temp_split_certs_paths:
                    try:
                        # Pasa el `source_pdf_path` original al extraer datos de las páginas temporales
                        extracted_datum = extraer_datos_constancia(temp_path, config, original_source_path=source_pdf_path)
                        all_extracted_data.append(extracted_datum)
                        total_extracted_certificates += 1
                    except Exception as e:
                        print(f"Error al extraer datos de la página temporal '{os.path.basename(temp_path)}': {e}")
            else:
                print(f"ADVERTENCIA: No se pudieron extraer constancias válidas de '{os.path.basename(source_pdf_path)}'.")
        else: # PDF Standalone
            print(f"Procesando PDF standalone: {os.path.basename(source_pdf_path)}")
            try:
                # Para archivos standalone, el `original_source_path` es el mismo `source_pdf_path`
                extracted_datum = extraer_datos_constancia(source_pdf_path, config, original_source_path=source_pdf_path)
                all_extracted_data.append(extracted_datum)
                total_extracted_certificates += 1
            except Exception as e:
                print(f"Error al extraer datos de '{os.path.basename(source_pdf_path)}': {e}")

        total_files_processed_for_data_extraction += 1

    print(f"\nProcesamiento de archivos fuente completado. Total de archivos fuente procesados: {total_files_processed_for_data_extraction}.")
    print(f"  - PDFs agrupados divididos: {total_grouped_pdfs_split}")
    print(f"  - Total de constancias individuales extraídas: {total_extracted_certificates}\n")

    # 3. Cargar datos de empleados (HC)
    df_hc = cargar_data_hc(config.hc_table_path, config.vocales_acentos)

    # 4. Convertir datos extraídos a DataFrame, limpiar y fusionar con HC
    df_constancias_merged = procesar_y_mergear_constancias(all_extracted_data, df_hc, config.vocales_acentos)

    if df_constancias_merged.empty:
        print("El DataFrame resultante está vacío. Terminando el proceso.")
        return

    # Asegurar que 'original_source_path' sea de tipo string antes de pasarlo a otras funciones
    if 'original_source_path' in df_constancias_merged.columns:
        df_constancias_merged['original_source_path'] = df_constancias_merged['original_source_path'].astype('string').fillna('')
    else:
        df_constancias_merged['original_source_path'] = '' # Fallback, no debería ocurrir si extraer_datos_constancia funciona bien

    # 5. Identificar y reportar constancias sin número de empleado
    identificar_y_reportar_constancias_sin_coincidencia(df_constancias_merged, config)

    # 6. Normalizar fechas y asignar estado de vigencia, y crear 'nombre_archivo_nuevo'
    df_final = normalizar_y_categorizar_fechas(df_constancias_merged, config.mapeo_meses, config.vocales_acentos)

    # 7. Organizar los archivos PDF en carpetas por empleado
    organizar_archivos_pdf(df_final, config)

    # 8. Exportar resultados
    exportar_resultados(df_final, config)

    # Guardar el set único de archivos procesados a disco
    _guardar_registro_procesado_a_disco(config)

    # Limpiar la carpeta temporal al final de la ejecución
    if os.path.exists(config.temp_split_pdfs_folder):
        try:
            shutil.rmtree(config.temp_split_pdfs_folder)
            print(f"INFO: Carpeta temporal de PDFs divididos '{config.temp_split_pdfs_folder}' eliminada.")
        except Exception as e:
            print(f"ADVERTENCIA: No se pudo eliminar la carpeta temporal '{config.temp_split_pdfs_folder}' al final. Error: {e}")