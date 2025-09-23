import os
import unicodedata
from datetime import datetime

class Config:
    """
    Clase para centralizar y gestionar todas las configuraciones y rutas ETL.
    """
    def __init__(self):
        # Rutas base del usuario y OneDrive
        self.user_home = os.path.expanduser("~")
        self.onedrive_org_name = 'OneDrive - Vivaaerobus'
        self.onedrive_shared_base_path = os.path.join(self.user_home, self.onedrive_org_name, 'archivos_compartidos')

        # Ruta raíz del proyecto (ajusta si tu proyecto 'Viva-handling' está en otro lugar)
        self.project_root = os.path.join(self.user_home, 'Projects', 'Viva-handling')

        # --- Carpetas de Datos Generales (relativas a project_root) ---
        self.data_folder = os.path.join(self.project_root, 'data')
        self.data_raw_folder = os.path.join(self.data_folder, 'raw')
        self.data_processed_folder = os.path.join(self.data_folder, 'processed')
        self.dashboard_tables_folder = os.path.join(self.data_processed_folder, 'dashboard_tables')
        self.temp_split_pdfs_folder = os.path.join(self.data_processed_folder, 'temp_split_pdfs')

        # --- Configuraciones específicas de etl_bd_hc.py ---
        self.hc_etl_files = {
            "FILE_MAESTRO_HC": os.path.join(self.onedrive_shared_base_path, 'GESTION HUMANA', 'BASE DE DATOS.xlsx'),
            "FILE_DATOS_ADICIONALES_HC": os.path.join(self.onedrive_shared_base_path, '12. Compartida', '1. Bryan', 'Archivos_Entrenamiento', 'BASE DE DATOS_ADICIONAL.xlsx'),
            "FILE_PUESTOS": os.path.join(self.onedrive_shared_base_path, '12. Compartida', '1. Bryan', 'Tabla_Homologacion.xlsx'),
            "FILE_ENTRENAMIENTO": os.path.join(self.onedrive_shared_base_path, '12. Compartida', '1. Bryan', 'Registro_Entrenamiento.xlsm'),
            "FILE_COBERTURA": os.path.join(self.onedrive_shared_base_path, '12. Compartida', '1. Bryan', 'Cobertura.xlsx'),
        }
        self.hc_etl_folders = {
            "FOLDER_RELOJ_CHECADOR": os.path.join(self.onedrive_shared_base_path, '12. Compartida', '1. Bryan', 'Faltas'),
            "FOLDER_ROSTER": os.path.join(self.onedrive_shared_base_path, '12. Compartida', '1. Bryan', 'Roster'),
        }
        self.hc_etl_sheets_names = {
            "MAESTRO_HC": 'BASE DE DATOS',
            "DATOS_ADICIONALES_HC": 'Datos',
            "BAJAS_HC": 'BAJAS',
            "ENTRENAMIENTO": 'Base',
            "PROGRAMACION": 'Programacion',
            "ASISTENCIA_SHEETS": ['Asistencias', 'Asistencia SAT'],
            "COBERTURA_REQUERIDO": 'Requerido'
        }
        self.hc_etl_out_filenames = { # Estos se guardarán en dashboard_tables_folder
            "FACT_TABLE": 'fact_table.csv',
            "HC_TABLE": 'hc_table.csv',
            "HC_BAJAS_TABLE": 'hc_bajas_table.csv',
            "PUESTOS_TABLE": 'puestos_table.csv',
            "CURSOS_TABLE": 'cursos_table.csv',
            "ASISTENCIA_TABLE": 'asistencia_table.csv',
            "AUSENTISMO_TABLE": 'ausentismo_table.csv',
            "COBERTURA_TABLE": 'cobertura_table.csv',
        }
        # Ruta a hc_table.csv (salida de etl_bd_hc, entrada para etl_pdf_entrenamiento)
        self.hc_table_path = os.path.join(self.dashboard_tables_folder, self.hc_etl_out_filenames['HC_TABLE'])

        # --- Nombres de archivos de salida específicos de etl_pdf_entrenamiento.py ---
        self.pdf_etl_output_filenames = {
            "XLSX_CONSTANCIAS": 'datos_constancias.xlsx',
            "CSV_CONSTANCIAS": 'datos_constancias.csv',
            "LOG_PROCESSED_FILES": 'registro_archivos_procesados.txt',
            "LIST_NEW_NON_EXCLUDED_PDFS": 'lista_pdfs_nuevos_no_excluidos.txt',
            "XLSX_CONSTANCIAS_SIN_EMP": 'datos_constancias_sin_emp.xlsx',
            "CSV_CONSTANCIAS_SIN_EMP": 'datos_constancias_sin_emp.csv',
        }

        # Rutas para salidas del ETL de PDF (carpeta local de datos procesados)
        self.outpath_xlsx_constancias = os.path.join(self.data_processed_folder, self.pdf_etl_output_filenames['XLSX_CONSTANCIAS'])
        self.outpath_csv_constancias = os.path.join(self.data_processed_folder, self.pdf_etl_output_filenames['CSV_CONSTANCIAS'])
        self.outpath_processed_files_log = os.path.join(self.data_processed_folder, self.pdf_etl_output_filenames['LOG_PROCESSED_FILES'])
        self.outpath_list_new_non_excluded_pdfs = os.path.join(self.data_processed_folder, self.pdf_etl_output_filenames['LIST_NEW_NON_EXCLUDED_PDFS'])
        self.outpath_xlsx_constancias_sin_emp = os.path.join(self.data_processed_folder, self.pdf_etl_output_filenames['XLSX_CONSTANCIAS_SIN_EMP'])
        self.outpath_csv_constancias_sin_emp = os.path.join(self.data_processed_folder, self.pdf_etl_output_filenames['CSV_CONSTANCIAS_SIN_EMP'])
        self.processed_files_set_in_memory = set()
        
        # Carpeta compartida de OneDrive para certificados (donde se organizan los PDFs finales)
        self.onedrive_certs_base = os.path.join(self.onedrive_shared_base_path, 'Certificados Entrenamiento Viva Handling - Certficados')
        self.onedrive_certs_active = os.path.join(self.onedrive_certs_base, '2.Constancias_actual')
        self.onedrive_certs_bajas = os.path.join(self.onedrive_certs_active, '1. BAJAS') # Subcarpeta dentro de '2.Constancias_actual'

        # Patrones de texto para extracción de PDF (de etl_pdf_entrenamiento.py)
        self.nombres_archivos_sat = ['instructor sat', '2025-T', 'apoyo en tierra', 'sat.']
        self.nombres_archivos_avsec = ['AVSEC', 'AVSEC-2024', 'AVSEC-2025', 'AVSE ', 'seguridad de la aviación', 'seguridad de la aviacion']
        self.nombres_archivos_sms = ['SMS', 'SAFETY MANAGEMENT SYSTEM']

        # Mapeos para normalización de texto (de ambos scripts)
        self.vocales_acentos = {
            'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u', 'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U', 'Ñ': 'N', 'ñ': 'n'
        }
        self.mapeo_meses = {
            'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4, 'may': 5, 'jun': 6,
            'jul': 7, 'ago': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12,
            'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'junio': 6,
            'julio': 7, 'agosto': 8, 'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12,
            'jaan': 1,
        }
        self.turnos_roster = { # de etl_bd_hc.py
            '03at': 't1', '12at': 't2', '21at': 't3'
        }

        # --- Configuraciones específicas de generador_lista_no_excluidos.py ---
        # Estas son las carpetas fuente donde se encuentran los PDFs a procesar.
        self.source_folders_pdfs = [
            os.path.join(self.onedrive_certs_base, '1.Constancias_agrupadas'),
            os.path.join(self.onedrive_shared_base_path, 'Capacitación SAT Pronomina MTY - 2025'),
            os.path.join(self.onedrive_certs_base, '3.Constancias_anterior'),
            os.path.join(self.onedrive_shared_base_path, 'Aeropuertos - AUTOPRESTACION MTY')
        ]
        self.min_mod_year = 2024
        self.non_vigentes_years_in_filename = ['2018', '2019', '2020', '2021', '2022', '2023', 'P.P.2022', 'P.P.2023']
        self.excluded_prefixes = [
            'bitcora', 'bitacora ', 'fori ', 'ojt ', '2024 rtar ', 'ef-', 'ef ', 'ex-', 'ex ', 'id ', 'id-', 'la ', 'la-', 'l.a.', 'l.a. ', 'l.a.-', 'ro-', 'ro ', 'sat-ro', 'pb', 'laf', '2025-r', 'dif ', 'dif-', 'td', 'green', 'bajas', 'bitacora'
        ]
        self.excluded_suffixes = ['cun', 'gc-25', 'gp-25']
        # Las siguientes rutas ya apuntan a las definidas arriba:
        # self.ruta_registro_archivos_procesados = self.outpath_processed_files_log
        # self.ruta_nuevo_archivo_no_excluidos = self.outpath_list_new_non_excluded_pdfs

        self._create_output_folders()

    def _create_output_folders(self):
        """Crea las carpetas de salida si NO existen."""
        # Crear carpetas de salida del proyecto local
        os.makedirs(self.data_raw_folder, exist_ok=True)
        os.makedirs(self.data_processed_folder, exist_ok=True)
        os.makedirs(self.dashboard_tables_folder, exist_ok=True)
        os.makedirs(self.temp_split_pdfs_folder, exist_ok=True)
        # Asegurar que existan los directorios padre para los archivos de log/lista
        os.makedirs(os.path.dirname(self.outpath_processed_files_log), exist_ok=True)
        os.makedirs(os.path.dirname(self.outpath_list_new_non_excluded_pdfs), exist_ok=True)

        # Crear carpetas de salida específicas de OneDrive si no existen
        os.makedirs(self.onedrive_certs_active, exist_ok=True)
        os.makedirs(self.onedrive_certs_bajas, exist_ok=True)