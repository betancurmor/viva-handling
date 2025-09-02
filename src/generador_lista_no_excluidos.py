import os
from datetime import datetime
from etl_pdf_entrenamiento import Config, dividir_pdf_constancia_agrupado, normalizar_acentos, fitz

# --- 1. FUNCIÓN CARGAR SET REGISTROS(log) PROCESADOS ---
def _cargar_set_registros_procesados(log_file_path):
    """"Carga el log de archivos procesados en un conjunto para busquedas eficientes. Retorna un set vacio si el archivo no existe o esta vacio."""
    processed_paths = set()
    if os.path.exists(log_file_path):
        try:
            with open(log_file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    path = line.strip()
                    if path:
                        processed_paths.add(path)
            print(f"Cargadas {len(processed_paths)} rutas de archivos procesadas desde: '{log_file_path}'")
        except Exception as e:
            print(f"Advertencia: No se pudo cargar el log de archivos procesados desde: '{log_file_path}'. Error: {e}")
    else:
        print(f"No se encontro el log de archivos en '{log_file_path}'. Se asume que no hay archivos procesados previamente.")
    return processed_paths
# --- 2. FUNCIÓN PRINCIPAL DE GENERACIÓN DE LISTA ---
def generador_lista_archivos_no_excluidos(config: Config):
    """
    Recorre las carpetas fuente, aplicando reglas de exclusión para directorios y archivos PDF.
    Identifica los archivos PDF que no fueron excluidos, los divide si son agrupados,
    y guarda esta lista de "archivos nuevos no excluidos" en un archivo de texto.
    """
    new_non_excluded_file_paths = []
    total_carpetas_saltadas = 0
    total_archivos_encontrados = 0
    total_pdfs_excluidos_por_regla = 0
    total_pdfs_excluidos_por_fecha = 0
    total_pdfs_ya_procesados = 0
    total_pdfs_agrupados_divididos = 0 # Contador para el reporte

    print("\n[SCRIPT NO DIARIO] Iniciando búsqueda de archivos NO excluidos...\n")

    # Cargar el log de archivos ya procesados una sola vez al inicio
    set_archivos_procesados = _cargar_set_registros_procesados(config.ruta_registro_archivos_procesados)

    # Limpiar la carpeta temporal al inicio de cada ejecución
    if os.path.exists(config.temp_split_pdfs_folder):
        for f in os.listdir(config.temp_split_pdfs_folder):
            os.remove(os.path.join(config.temp_split_pdfs_folder, f))
        print(f"INFO: Carpeta temporal de PDFs divididos '{config.temp_split_pdfs_folder}' limpiada.")
    os.makedirs(config.temp_split_pdfs_folder, exist_ok=True) # Asegurarse de que exista

    for source_folder in config.carpetas_fuente:
        if not os.path.exists(source_folder):
            print(f"Advertencia: La carpeta fuente '{source_folder}' no existe. Saltando...")
            continue

        # os.walk(topdown=True) es lo habitual y permite modificar 'dirs' in-place.
        for root, dirs, files in os.walk(source_folder, topdown=True):
            # --- Lógica de Exclusión de Carpetas ---
            dirs_to_visit = []
            for dir_name in dirs:
                skip_dir = False
                # Verificar prefijos de carpeta
                for prefix in config.prefijos_excluidos: 
                    if dir_name.lower().startswith(prefix.lower()):
                        # print(f"Saltando prefijo '{prefix}' en carpeta: {os.path.join(root, dir_name)}") 
                        skip_dir = True
                        total_carpetas_saltadas += 1
                        break
                if skip_dir:
                    continue

                # Verificar sufijos de carpeta
                for suffix in config.sufijos_excluidos:
                    if dir_name.lower().endswith(suffix.lower()):
                        # print(f"Saltando sufijo '{suffix}' en carpeta: {os.path.join(root, dir_name)}") 
                        skip_dir = True
                        total_carpetas_saltadas += 1
                        break
                if skip_dir:
                    continue

                dirs_to_visit.append(dir_name)
            dirs[:] = dirs_to_visit # Modifica la lista 'dirs' para que os.walk no descienda en los excluidos

            # --- Iterando en cada archivo ---
            for file_name in files:
                # Solo procesar PDFs
                if not file_name.lower().endswith('.pdf'):
                    continue

                total_archivos_encontrados += 1
                full_pdf_path = os.path.join(root, file_name)
                is_file_excluded = False # Reiniciar bandera para cada archivo

                # --- Lógica de Exclusión de Archivos
                # 1. Verificar prefijos
                for prefix in config.prefijos_excluidos:
                    if file_name.lower().startswith(prefix.lower()):
                        is_file_excluded = True
                        total_pdfs_excluidos_por_regla += 1
                        break
                if is_file_excluded:
                    continue # Archivo excluido, pasar al siguiente

                # 2. Verificar sufijos
                for suffix in config.sufijos_excluidos:
                    if file_name.lower().endswith(suffix.lower()):
                        is_file_excluded = True
                        total_pdfs_excluidos_por_regla += 1
                        break
                if is_file_excluded:
                    continue # Archivo excluido, pasar al siguiente

                # 3. Verificar años no vigentes en el nombre del archivo
                for year in config.años_no_vigentes:
                    if year.lower() in file_name.lower():
                        is_file_excluded = True
                        total_pdfs_excluidos_por_fecha += 1
                        break
                if is_file_excluded:
                    continue # Archivo excluido, pasar al siguiente

                # 4. Lógica de Exclusión por Fecha de Modificación
                try:
                    timestamp_modificacion = os.path.getmtime(full_pdf_path)
                    fecha_modificacion = datetime.fromtimestamp(timestamp_modificacion)
                    if fecha_modificacion.year < config.año_minimo_modificacion:
                        is_file_excluded = True
                        total_pdfs_excluidos_por_fecha += 1
                except Exception as e:
                    # Si no se puede obtener la fecha, no se excluye el archivo por esta razón.
                    pass # No se excluye el archivo por error de fecha, se procesa normalmente

                if is_file_excluded:
                    continue # Archivo excluido por fecha de modificación, pasar al siguiente
                
                if full_pdf_path in set_archivos_procesados:
                    total_pdfs_ya_procesados += 1
                    continue # Archivo ya procesado, no es NUEVO, saltar

                try:
                    doc_check = fitz.open(full_pdf_path)
                    # Heurística para detectar si es un PDF agrupado (ej., > 1 página y no marcado como excluido)
                    if doc_check.page_count > 1:
                        print(f"INFO: Posible PDF agrupado detectado: '{os.path.basename(full_pdf_path)}'. Intentando dividir...")
                        temp_split_certs = dividir_pdf_constancia_agrupado(full_pdf_path, config)
                        if temp_split_certs:
                            new_non_excluded_file_paths.extend(temp_split_certs)
                            total_pdfs_agrupados_divididos += 1
                            print(f"INFO: {len(temp_split_certs)} constancias individuales extraídas de '{os.path.basename(full_pdf_path)}'.")
                            doc_check.close()
                            continue # No procesar el PDF agrupado original, solo sus partes
                        else:
                            print(f"ADVERTENCIA: No se encontraron constancias válidas al dividir '{os.path.basename(full_pdf_path)}'. Se procesará como un solo archivo.")
                    doc_check.close()
                except Exception as e:
                    print(f"ADVERTENCIA: Error al intentar dividir '{os.path.basename(full_pdf_path)}': {e}. Se procesará como un solo archivo.")
                # --------------------------------------------------------------------

                # Si el código llega aquí, el archivo no fue excluido, NO estaba en el log,
                # y no fue un PDF agrupado (o no se pudo dividir), así que se añade directamente.
                new_non_excluded_file_paths.append(full_pdf_path)

    # --- 3. REPORTE FINAL ---
    total_pdfs_excluidos = total_pdfs_excluidos_por_regla + total_pdfs_excluidos_por_fecha
    print(f"\n[SCRIPT NO DIARIO] Reporte de la generación de la lista de archivos NO excluidos:")
    print(f"  Total de archivos PDF encontrados (incluyendo excluidos y agrupados): {total_archivos_encontrados}")
    print(f"  Total de carpetas saltadas por reglas: {total_carpetas_saltadas}")
    print(f"  Total de archivos PDF *EXCLUIDOS* por reglas: {total_pdfs_excluidos}")
    print(f"    - Por prefijo/sufijo en nombre de archivo: {total_pdfs_excluidos_por_regla}")
    print(f"    - Por año en nombre o fecha de modificación: {total_pdfs_excluidos_por_fecha}")
    print(f"  Total de archivos PDF no excluidos *ya procesados anteriormente*: {total_pdfs_ya_procesados}")
    print(f"  Total de PDFs agrupados procesados y divididos: {total_pdfs_agrupados_divididos}")
    print(f"  -------------------------------------------------------------")
    print(f"  Total de archivos PDF *NUEVOS NO EXCLUIDOS* (para procesamiento): {len(new_non_excluded_file_paths)}\n")

    # --- 4. GUARDAR LA LISTA EN ARCHIVO ---
    try:
        with open(config.ruta_nuevo_archivo_no_excluidos, 'w', encoding='utf-8') as f:
            for path in new_non_excluded_file_paths:
                f.write(f"{path}\n")
        print(f"Lista de archivos NUEVOS NO excluidos guardada en: '{config.ruta_nuevo_archivo_no_excluidos}'")
    except Exception as e:
        print(f"Error al guardar la lista de archivos NUEVOS NO excluidos en '{config.ruta_nuevo_archivo_no_excluidos}': {e}")

    return new_non_excluded_file_paths
# --- 5. EJECUCIÓN PRINCIPAL ---
if __name__ == '__main__':
    current_config = Config() # Se usará la Config de etl_pdf_entrenamiento
    archivos_nuevos_no_excluidos_list = generador_lista_archivos_no_excluidos(current_config)
    print(f"\nGeneración de lista de archivos NUEVOS NO excluidos completada. Se encontraron {len(archivos_nuevos_no_excluidos_list)} archivos NUEVOS NO excluidos.")