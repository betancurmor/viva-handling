import sys
import os
from datetime import datetime

# Obtener la ruta de la carpeta del script actual (src/)
script_dir =os.path.abspath(os.path.dirname(__file__))
# Obtener la ruta de la carpeta raíz del proyecto (Viva-handling/)
project_root = os.path.dirname(script_dir)

# Añadir la carpeta raíz del proyecto a sys.path.
# Esto permite que Python encuentre el paquete 'src' cuando se importa como 'src.modulo'.
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Ahora, usa importaciones ABSOLUTAS referenciando el paquete 'src'
from src.config import Config
from src.etl_bd_hc import run_hc_etl
from src.generador_lista_no_excluidos import generador_lista_archivos_no_excluidos
from src.etl_pdf_entrenamiento import run_pdf_etl

def main_orchestrator():
    """
    Función principal que orquesta la ejecución de todos los procesos ETL.
    """
    print(f"\n--- INICIANDO PROCESO ETL COMPLETO - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

    try:
        # 1. Cargar configuración centralizada
        print("\n[Orquestador] Cargando configuración...")
        config = Config()
        print("[Orquestador] Configuración cargada y carpetas de salida verificadas.")

        # 2. Ejecutar el ETL de la Base de Datos de Capital Humano (etl_bd_hc.py)
        # Este paso genera el 'hc_table.csv' necesario para el siguiente script.
        print("\n[Orquestador] Ejecutando ETL de Base de Datos de Capital Humano (etl_bd_hc.py)...")
        run_hc_etl(config)
        print("[Orquestador] ETL de Base de Datos de Capital Humano completado exitosamente.")

        # 3. Generar la lista de archivos PDF no excluidos (generador_lista_no_excluidos.py)
        # Este paso crea 'lista_pdfs_nuevos_no_excluidos.txt' que es la entrada para el ETL de PDFs.
        print("\n[Orquestador] Generando lista de archivos PDF no excluidos (generador_lista_no_excluidos.py)...")
        generador_lista_archivos_no_excluidos(config)
        print("[Orquestador] Generación de lista de PDFs no excluidos completada exitosamente.")

        # 4. Ejecutar el ETL de Constancias PDF (etl_pdf_entrenamiento.py)
        # Este paso utiliza 'hc_table.csv' y 'lista_pdfs_nuevos_no_excluidos.txt'.
        print("\n[Orquestador] Ejecutando ETL de Constancias PDF (etl_pdf_entrenamiento.py)...")
        run_pdf_etl(config)
        print("[Orquestador] ETL de Constancias PDF completado exitosamente.")

        print(f"\n--- PROCESO ETL COMPLETO FINALIZADO EXITOSAMENTE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

    except Exception as e:
        print(f"\n!!! ERROR CRÍTICO EN EL PROCESO ETL !!! - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Error: {e}")
        print("El proceso ha sido interrumpido.")
        sys.exit(1) # Salir con un código de error

if __name__ == "__main__":
    main_orchestrator()