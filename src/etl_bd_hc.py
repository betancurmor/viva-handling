import pandas as pd
import os
import unicodedata

# --- CONFIGURACION DE RUTAS Y NOMBRES ---
CONFIG = {
    "PATHS": {
        "FILE_MAESTRO_HC": r"C:\Users\bryan.betancur\OneDrive - Vivaaerobus\archivos_compartidos\GESTION HUMANA\BASE DE DATOS.xlsx",
        "FILE_PUESTOS": r"C:\Users\bryan.betancur\OneDrive - Vivaaerobus\archivos_compartidos\12. Compartida\1. Bryan\Tabla_Homologacion.xlsx",
        # "FILE_ENTRENAMIENTO": r"C:\Users\bryan.betancur\OneDrive - Vivaaerobus\archivos_compartidos\12. Compartida\1. Bryan\Entrenamiento.xlsx",
        "FILE_ENTRENAMIENTO": r"C:\Users\bryan.betancur\OneDrive - Vivaaerobus\archivos_compartidos\12. Compartida\1. Bryan\Registro_Entrenamiento.xlsm",
        "FILE_RELOJ_CHECADOR": r"C:\Users\bryan.betancur\OneDrive - Vivaaerobus\archivos_compartidos\12. Compartida\1. Bryan\Faltas.csv",
        "FOLDER_RELOJ_CHECADOR": r"C:\Users\bryan.betancur\OneDrive - Vivaaerobus\archivos_compartidos\12. Compartida\1. Bryan\Faltas",
        "FILE_COBERTURA": r"C:\Users\bryan.betancur\OneDrive - Vivaaerobus\archivos_compartidos\12. Compartida\1. Bryan\Cobertura.xlsx",
        "FILE_ROSTER": r"C:\Users\bryan.betancur\OneDrive - Vivaaerobus\archivos_compartidos\12. Compartida\1. Bryan\Archivos_Entrenamiento\ROSTER",
        "FOLDER_OUTPATH": r".\data\processed",
        "FOLDER_OUTPATH_DASHBOARD": r".\data\processed\dashboard_tables"
    },
    "SHEETS_NAMES": {
        "MAESTRO_HC" : 'BASE DE DATOS',
        "BAJAS_HC": 'BAJAS',
        # "ENTRENAMIENTO_SHEETS": ['AVSEC', 'SMS', 'SAT'],
        "ENTRENAMIENTO": 'Base',
        "ASISTENCIA_SHEETS": ['Asistencias', 'Asistencia SAT'],
        "COBERTURA_REQUERIDO": 'Requerido'
    },
    "OUTPATHS": {
        "FACT_TABLE": 'fact_table.csv',
        "HC_TABLE": 'hc_table.csv',
        "HC_BAJAS_TABLE": 'hc_bajas_table.csv',
        "PUESTOS_TABLE": 'puestos_table.csv',
        "CURSOS_TABLE": 'cursos_table.csv',
        "ASISTENCIA_TABLE": 'asistencia_table.csv',
        "AUSENTISMO_TABLE": 'ausentismo_table.csv',
        "COBERTURA_TABLE": 'cobertura_table.csv',
    }
}

acentos_vocales = {
    'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u', 'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U', 'Ñ': 'N', 'ñ': 'n'
}

turnos_roster = {
    '03at': 't1', '12at': 't2', '21at': 't3'
}

def normalizar_acentos(series, vocales_acentos=acentos_vocales):
    """
    Normaliza acentos y la 'ñ' en una cadena de texto.
    Esta funcion esta diseñada para ser aplicada a cadenas indiviuales, como os nombres de las columnas o valores de texto.
    """
    if not isinstance(series, str):
        return series
    
     # Normalizar el texto de entrada a la forma NFC (Normalization Form Canonical Composition).
    # Esto convierte los caracteres a su representación precompuesta de un solo punto de código,
    # lo que asegura que coincidan con las claves en 'vocales_acentos'.
    
    series_procesadas = unicodedata.normalize('NFC', series)

    for acento, sin_acento in vocales_acentos.items():
        series_procesadas = series_procesadas.replace(acento, sin_acento)

    return series_procesadas

def cargar_transformar_excel(file_path, 
                             sheet_name=None, engine='openpyxl', header=None):
    """
    Cargar un archivo de excel, convierte nombres de columnas en minusculas, elimina espacios al inicio/final y elimina columnas con nombres NaN.
    """

    df = pd.read_excel(file_path, sheet_name=sheet_name, engine=engine, header=header)
    df.columns = [normalizar_acentos(col) for col in df.columns]
    df.columns = df.columns.astype(str).str.lower().str.strip().str.replace(r'\s+', ' ', regex=True).str.replace(' ', '_', regex=False)
    df = df.loc[:, df.columns.notna()]
    df = df.drop_duplicates()
    if 'nombre' in df.columns:
        df = df.dropna(subset=['nombre'], how='any')
    
    return df
    
def cargar_transformar_csv(file_path,
                           header=0,encoding=None):
    """
    Cargar un archivo csv, convierte nombres de columnas en minusculas, elimina espacios al inicio/final y elimina columnas con nombres NaN.
    """

    df = pd.read_csv(file_path, header=header, encoding=encoding)
    df.columns = [normalizar_acentos(col) for col in df.columns]
    df.columns = df.columns.astype(str).str.lower().str.strip().str.replace(r'\s+', ' ', regex=True).str.replace(' ', '_', regex=False)
    df = df.loc[:, df.columns.notna()]
    df = df.drop_duplicates()
    if '#emp' in df.columns:
        df = df.dropna(subset=['#emp'], how='any')
    return df

def limpiar_columna_texto(series, caracteres_a_eliminar=None):
    """
    Limpia una serie(columna) de tipo 'string': convierte a string, elimina caracteres especificos, normaliza espacios, quita espacios al inicio/final, rellena NaNs con string vacio.
    """

    s = series.astype(str).str.strip()
    if caracteres_a_eliminar:
        for c in caracteres_a_eliminar:
            s = s.str.replace(c, '', regex=False)

    return s.str.replace(r'\s+', ' ', regex=True).str.strip().fillna('')

def limpiar_columna_id(series, caracteres_a_eliminar=None):
    """
    Limpia una serie(columna) de ID que pueden contener caracteres no numericos. Convierte a string, quita espacios al inicio/final, elimina caracteres especificos, luego a numerico y finalmente a 'int64', rellanando NaNs con 0.
    """

    s = series.astype(str).str.strip().str.replace(r'\s+', '', regex=True).str.replace('.0', '', regex=False)
    if caracteres_a_eliminar:
        for c in caracteres_a_eliminar:
            s = s.str.replace(c, '', regex=False)

    s = s.str.replace(r'[^\d]', '', regex=True) # Generaliza para eliminar no dígitos
    return pd.to_numeric(s, errors='coerce').fillna(0).astype('int64')

def limpiar_columna_fecha(series, formato_fecha='%d/%m/%Y', errors='coerce'):
    """
    Convierte una serie de fecha: convierte a datetime, maneja errores y rellena NaNs con NaTs
    """

    return pd.to_datetime(series, errors=errors,
                          format=formato_fecha)

def run_hc_etl():
    # --- Dashboard
    # ---- Tabla 'hc_table' 
    # Cargar base de datos HC
    df_hc = cargar_transformar_excel(CONFIG["PATHS"]["FILE_MAESTRO_HC"],sheet_name=CONFIG["SHEETS_NAMES"]["MAESTRO_HC"],header=0)

    # limpieza de columnas de texto
    columnas_texto = ['id', 'paterno','materno', 'nombre', 'rfc', 'curp', 'telefono', 'estatus', 'area', 'puesto', 'novedades/comentarios']
    for col in columnas_texto:
        if col in df_hc.columns:
            df_hc[col] = limpiar_columna_texto(df_hc[col])
        else:
            df_hc[col] = ''

    # Crear y limpiar la columna '#emp'
    df_hc['#emp'] = limpiar_columna_id(df_hc['id'], caracteres_a_eliminar=['H', 'P'])

    # Crear y limpiar la columna 'nombre_completo'
    df_hc['nombre_completo'] = df_hc['paterno'] + ' ' + df_hc['materno'] + ' ' + df_hc['nombre']
    df_hc['nombre_completo'] = limpiar_columna_texto(df_hc['nombre_completo'])

    # Limpieza de 'telefono'
    if 'telefono' in df_hc.columns:
        df_hc['telefono'] = limpiar_columna_id(df_hc['telefono'])
    else:
        df_hc['telefono'] = pd.NA # En lugar de '0'

    # Limpieza de 'fecha'
    columnas_fecha = ['fecha_nacimiento', 'fechaalta', 'fecha_baja', 'fecha_antiguedad']
    for col in columnas_fecha:
        if col in df_hc.columns:
            df_hc[col] = limpiar_columna_fecha(df_hc[col])
        else:
            df_hc[col] = pd.NaT

    # Transformar y reenombrar columnas
    df_hc['puesto'] = df_hc['puesto'].str.upper()
    df_hc = df_hc.rename(columns={'novedades_/_comentarios': 'novedades_comentarios'})
    df_hc = df_hc.rename(columns={'fechaalta': 'fecha_alta'})

    df_hc['nombre_completo'] = df_hc['nombre_completo'].replace('REYES nan ALEJANDRO', 'REYES ALEJANDRO', regex=False)

    # Definir orden de columnas
    df_hc = df_hc[['#emp', 'nombre_completo', 'paterno','materno', 'nombre', 'rfc', 'curp', 'telefono', 'estatus','puesto', 'fecha_alta', 'fecha_antiguedad', 'fecha_baja', 'fecha_nacimiento', 'novedades_comentarios']]

    # --- Dashboar 'Ausentismo'
    # ---- Tabla 'hc_bajas_table'
    df_bajas = cargar_transformar_excel(CONFIG["PATHS"]["FILE_MAESTRO_HC"], sheet_name=CONFIG["SHEETS_NAMES"]["BAJAS_HC"])

    columnas_texto = ['id', 'motivo', 'causa']
    for col in columnas_texto:
        if col in df_bajas:
            df_bajas[col] = limpiar_columna_texto(df_bajas[col])
        else:
            df_bajas[col] = ''
        if col in ['motivo', 'causa']:
            df_bajas[col] = df_bajas[col].str.title()
        else:
            df_bajas[col] = df_bajas[col]

    if 'fecha_de_baja' in df_bajas.columns:
        df_bajas['fecha_de_baja'] = limpiar_columna_fecha(df_bajas['fecha_de_baja'])
    else:
        df_bajas['fecha_de_baja'] = pd.NaT

    df_bajas['#emp'] = limpiar_columna_id(df_bajas['id'], caracteres_a_eliminar=['H', 'P'])

    df_bajas = df_bajas[['#emp', 'fecha_de_baja', 'motivo', 'causa']]

    # --- Nexos
    # ---- Base 'Entrenamiento'
    # dfs = []
    # for sheet in CONFIG['SHEETS_NAMES']['ENTRENAMIENTO_SHEETS']:
    #     df = cargar_transformar_excel(CONFIG['PATHS']['FILE_ENTRENAMIENTO'], sheet_name=sheet)
    #     df = df.rename(columns={'vencimiento': 'l.d'})
    #     df = df.rename(columns={'programacion': 'e.d'})
    #     df['curso'] = sheet.strip()
    #     text_cols = ['#emp', 'curso', 'status']
    #     for col in text_cols:
    #         if col in df.columns:
    #             df[col] = limpiar_columna_texto(df[col], caracteres_a_eliminar=[' '])
    #         else:
    #             df[col] = pd.NA
    #     date_cols = ['l.d', 'e.d']
    #     for col in date_cols:
    #         if col in df.columns:
    #             df[col] = limpiar_columna_fecha(df[col])
    #         else:
    #             df[col] = pd.NaT
    #     if '#emp' in df.columns:
    #         df['#emp'] = limpiar_columna_id(df['#emp'])
    #     else:
    #         df['#emp'] = 0
    #     df = df[['#emp', 'curso', 'l.d', 'status', 'e.d']]
    #     dfs.append(df)
    # df_entrenamiento = pd.concat(dfs, ignore_index=True)
    # df_entrenamiento = df_entrenamiento.drop_duplicates().sort_values(['#emp', 'curso'])
    # df_entrenamiento = df_entrenamiento.rename(columns={'status': 'estatus_vigencia'})
    # df_entrenamiento.to_csv('prueba_entrenamiento.csv', encoding='utf-8', index=False)

    # --- Nexos
    # ---- Base 'Entrenamiento'
    df_entrenamiento = cargar_transformar_excel(CONFIG['PATHS']['FILE_ENTRENAMIENTO'], sheet_name=CONFIG['SHEETS_NAMES']['ENTRENAMIENTO'], header=8)
    text_cols = ['#emp', 'curso', 'estatus_vigencia']
    for col in text_cols:
        if col in df_entrenamiento.columns:
            df_entrenamiento[col] = limpiar_columna_texto(df_entrenamiento[col])
    df_entrenamiento['curso'] = df_entrenamiento['curso'].str.replace('SAT(Op)', 'SAT', regex=False)
    df_entrenamiento = df_entrenamiento.rename(columns={'fecha_vigencia': 'l.d'})
    df_entrenamiento = df_entrenamiento.rename(columns={'fecha_programada': 'e.d'})
    date_cols = ['l.d', 'e.d']
    for col in date_cols:
        if col in df_entrenamiento.columns:
            df_entrenamiento[col] = limpiar_columna_fecha(df_entrenamiento[col])
        else:
            df_entrenamiento[col] = pd.NaT
    if '#emp' in df_entrenamiento.columns:
        df_entrenamiento['#emp'] = limpiar_columna_id(df_entrenamiento['#emp'])
    else:
        df_entrenamiento['#emp'] = 0           
    df_entrenamiento = df_entrenamiento[['#emp', 'curso', 'l.d', 'estatus_vigencia', 'e.d']]
    df_entrenamiento = df_entrenamiento.drop_duplicates().sort_values(['#emp', 'curso'])

    # df_entrenamiento.to_csv('prueba_entrenamiento.csv', encoding='utf-8', index=False)

    # -- Cursos Entrenamiento
    # ---- Tabla 'cursos_table'
    df_cursos = df_entrenamiento[['curso']]
    df_cursos = df_cursos.drop_duplicates().dropna().sort_values(['curso']).reset_index(drop=True)
    df_cursos['id_curso'] = df_cursos.index+1
    df_cursos = df_cursos[['id_curso', 'curso']]

    # --- Estatus Vigencia, entrenamiento (temp_table)
    df_estatus_vigencia = df_entrenamiento[['estatus_vigencia']]
    df_estatus_vigencia = df_estatus_vigencia.drop_duplicates().dropna().sort_values(['estatus_vigencia']).reset_index(drop=True)
    df_estatus_vigencia['id_estatus_vigencia'] = df_estatus_vigencia.index
    df_estatus_vigencia = df_estatus_vigencia[['id_estatus_vigencia', 'estatus_vigencia']]

    # --- Tabla Auxiliar
    # 'Puestos homologados'
    df_puestos = cargar_transformar_excel(CONFIG['PATHS']['FILE_PUESTOS'], sheet_name='Hoja1',header=0)
    df_puestos.columns = df_puestos.columns.str.replace('ó', 'o', regex=False)
    for col in df_puestos.columns:
        if isinstance(col, str):
            df_puestos[col] = limpiar_columna_texto(df_puestos[col])
    df_puestos['posicion_vh'] = df_puestos['posicion_vh'].str.upper()

    # Generar 'id_puesto'
    df_puestos_homologados = df_puestos[['cargo_homologado', 'area', 'horas_diarias']]
    df_puestos_homologados = df_puestos_homologados.drop_duplicates().reset_index(drop=True)
    df_puestos_homologados['id_puesto'] = df_puestos_homologados.index+1
    df_puestos_homologados = df_puestos_homologados[['id_puesto', 'cargo_homologado', 'area', 'horas_diarias']]

    # Merge: 'df_hc', 'df_puestos'
    df_hc_puente = pd.merge(
        df_hc,
        df_puestos[['posicion_vh', 'cargo_homologado']],
        left_on=['puesto'],
        right_on=['posicion_vh'],
        how='left'
    )

    # Merge: 'df_hc_puente', 'df_puestos_homologados'
    df_hc_temp = pd.merge(df_hc_puente,
                        df_puestos_homologados[['cargo_homologado', 'id_puesto']],
                        left_on=['cargo_homologado'],
                        right_on=['cargo_homologado']
    )

    posicion_vh_nulos = df_hc_temp[df_hc_temp['posicion_vh'].isnull()]
    cargo_homologado_nulos = df_hc_temp[df_hc_temp['cargo_homologado'].isnull()]

    if len(posicion_vh_nulos) > 0:
        print(f"\nAdvertencia: tenemos casos sin coincidencia de 'posicion_vh': {len(posicion_vh_nulos)}\n")
        print(posicion_vh_nulos)


    if len(cargo_homologado_nulos) > 0:
        print(f"\nAdvertencia: se encontraron inc tenemos casos sin coincidencia de 'cargo_homologado': {len(cargo_homologado_nulos)}\n")
        print(cargo_homologado_nulos)

    # df_hc

    df_hc = df_hc_temp[['#emp', 'id_puesto', 'nombre_completo', 'paterno', 'materno', 'nombre', 'rfc', 'curp', 'telefono', 'estatus', 'fecha_alta', 'fecha_baja', 'fecha_antiguedad', 'fecha_nacimiento', 'novedades_comentarios']]

    # --- Tabla 'Asistencia Entrenamiento'
    # ---- sheets: AVSEC, SMS, SAT
    # Recorrer sheets de 'asistencia', convertir en df, transformar y limpiar. Concatenar dfs, dividir 'AVSEC/SMS' y eliminar registros. Concatenar df final.
    dfs_asistencia = []
    for s in CONFIG['SHEETS_NAMES']['ASISTENCIA_SHEETS']:
        df = cargar_transformar_excel(CONFIG['PATHS']['FILE_ENTRENAMIENTO'], sheet_name=s, header=0)
        df.columns = df.columns.str.strip()
        for col in df:
            if col != 'fecha de curso':
                df[col] = limpiar_columna_texto(df[col])
            else:
                df[col] = limpiar_columna_fecha(df[col])
        df['curso'] = df['curso'].str.replace(r'\s*', '', regex=True)
        df['#emp'] = limpiar_columna_id(df['#emp'])
        dfs_asistencia.append(df)
    df_asistencia = pd.concat(dfs_asistencia, ignore_index=True)
    df_asistencia_temp = df_asistencia[df_asistencia['curso'] == 'AVSEC/SMS'].copy()
    df_asistencia_avsec = df_asistencia_temp.copy()
    df_asistencia_avsec['curso'] = 'AVSEC'
    df_asistencia_sms = df_asistencia_temp.copy()
    df_asistencia_sms['curso'] = 'SMS'
    df_asistencia = df_asistencia[df_asistencia['curso'] != 'AVSEC/SMS'].copy()
    df_asistencia = pd.concat([df_asistencia_avsec, df_asistencia_sms, df_asistencia], ignore_index=True)
    df_asistencia = df_asistencia.sort_values(['#emp'])

    # df_entrenamiento.to_csv('prueba_entrenamiento.csv', encoding='utf-8', index=False)
    df_asistencia.to_csv('prueba_asistencia.csv', encoding='utf-8', index=False)


    # Merge: 'Entrenamiento', 'Asistencia'
    df_entrenamiento_asistencia = pd.merge(
        df_entrenamiento,
        df_asistencia[['#emp', 'curso', 'fecha_de_curso', 'asistencia', 'motivo']],
        left_on=['#emp', 'curso'],
        right_on=['#emp', 'curso']
    )
    df_entrenamiento_asistencia = df_entrenamiento_asistencia.sort_values(by='#emp', ascending=False)

    df_entrenamiento_asistencia.to_csv('prueba_entrenamiento_asistencia.csv', encoding='utf-8', index=False)

    # Merge: 'df_entrenamiento_asistencia', 'Cursos'
    df_entrenamiento_asistencia_cursos = pd.merge(
        df_entrenamiento_asistencia,
        df_cursos[['curso', 'id_curso']],
        left_on=['curso'],
        right_on=['curso']
    )

    # Merge: 'df_entrenamiento_asistencia_cursos', 'Estatus vigencia'
    df_entrenamiento_asistencia_cursos_status = pd.merge(
        df_entrenamiento_asistencia_cursos,
        df_estatus_vigencia[['estatus_vigencia', 'id_estatus_vigencia']],
        left_on=['estatus_vigencia'],
        right_on=['estatus_vigencia']
    )
    df_hechos = df_entrenamiento_asistencia_cursos_status[['#emp', 'id_curso', 'id_estatus_vigencia', 'l.d', 'e.d', 'fecha_de_curso', 'asistencia', 'motivo']]

    # --- Dashboard: 'Ausentismo'
    # Iterar entre cada archivo individual dentro de la carpete 'Faltas'
    try:
        archivos = [
            f for f in os.listdir(CONFIG['PATHS']['FOLDER_RELOJ_CHECADOR'])
            if f.endswith('.csv')
        ]
    except Exception as e:
        print(f"\nError al iterar, revisa la carpeta 'Faltas': {e}\n")

    dfs = []
    for a in archivos:
        ruta = os.path.join(CONFIG['PATHS']['FOLDER_RELOJ_CHECADOR'], a)
        df = cargar_transformar_csv(ruta, header=3, encoding='ansi')
        dfs.append(df)
    df_ausentismo = pd.concat(dfs, ignore_index=True)
    text_cols = ['trabajador', 'clave', 'concepto']
    for col in text_cols:
        if col in df_ausentismo.columns:
            df_ausentismo[col] = limpiar_columna_texto(df_ausentismo[col])
    if 'fechafalta' in df_ausentismo.columns:
        df_ausentismo = df_ausentismo.rename(columns={'fechafalta': 'fecha_falta'})
        df_ausentismo['fecha_falta'] = limpiar_columna_fecha(df_ausentismo['fecha_falta'])
    if 'trabajador' in df_ausentismo.columns:
        df_ausentismo = df_ausentismo.rename(columns={'trabajador': '#emp'})
        df_ausentismo['#emp'] = limpiar_columna_id(df_ausentismo['#emp'])
    df_ausentismo = df_ausentismo[['#emp', 'fecha_falta', 'clave', 'concepto']]
    df_ausentismo = df_ausentismo.sort_values(['#emp']).reset_index(drop=True)
    df_ausentismo['clave'] = df_ausentismo['clave'].fillna('FIJ')
    df_ausentismo['concepto'] = df_ausentismo['concepto'].fillna('Falta Injustificada')
    df_ausentismo['concepto'] = df_ausentismo['concepto'].str.title()

    # --- Dashboard: 'Cobertura'
    df_cobertura = cargar_transformar_excel(CONFIG['PATHS']['FILE_COBERTURA'], sheet_name=CONFIG['SHEETS_NAMES']['COBERTURA_REQUERIDO'], header=0)
    df_cobertura = df_cobertura.rename(columns={'año': 'ano'})
    for col in df_cobertura.columns:
        df_cobertura[col] = limpiar_columna_texto(df_cobertura[col])
    if 'requerido' in df_cobertura.columns:
        df_cobertura['requerido'] = df_cobertura['requerido'].replace('nan', 0, regex=False)
    if 'mes' and 'ano' in df_cobertura.columns:
        df_cobertura['fecha'] = '1/' + df_cobertura['mes'] + '/' + df_cobertura['ano']
        df_cobertura['fecha'] = limpiar_columna_fecha(df_cobertura['fecha'])
    df_cobertura = df_cobertura[['cargo', 'requerido', 'fecha']]

    # Merge: 'df_cobertura', 'puestos'
    df_cobertura = pd.merge(df_cobertura,
                            df_puestos_homologados[['cargo_homologado', 'id_puesto']],
                            left_on=['cargo'],
                            right_on=['cargo_homologado'])
    df_cobertura = df_cobertura[['id_puesto', 'requerido', 'fecha']]
    df_cobertura = df_cobertura.rename(columns={'cargo': 'puesto'})

    # Turnos: 'Roster'
    # PDTE - Validar con Adriana
    # df_roster = cargar_transformar_csv(CONFIG['PATHS']['FILE_ROSTER'], header=3, encoding='ansi')
    # df_turnos = df_roster.iloc[:, [0, -1]]
    # df_turnos 
    # for c in df_turnos.columns():
    #     df_roster[c] = limpiar_columna_texto(df_roster[c])
    # for turno, abreviatura in df_turnos[]
    


    # ---- Exportar archivos
    ruta_csv_hechos = os.path.join(CONFIG["PATHS"]["FOLDER_OUTPATH_DASHBOARD"], CONFIG["OUTPATHS"]['FACT_TABLE'])
    ruta_csv_hc = os.path.join(CONFIG["PATHS"]["FOLDER_OUTPATH_DASHBOARD"], CONFIG["OUTPATHS"]['HC_TABLE'])
    ruta_csv_hc_bajas = os.path.join(CONFIG["PATHS"]["FOLDER_OUTPATH_DASHBOARD"], CONFIG["OUTPATHS"]['HC_BAJAS_TABLE'])
    ruta_csv_puestos = os.path.join(CONFIG["PATHS"]["FOLDER_OUTPATH_DASHBOARD"], CONFIG["OUTPATHS"]['PUESTOS_TABLE'])
    ruta_csv_cursos = os.path.join(CONFIG["PATHS"]["FOLDER_OUTPATH_DASHBOARD"], CONFIG["OUTPATHS"]['CURSOS_TABLE'])
    ruta_csv_asistencia = os.path.join(CONFIG["PATHS"]["FOLDER_OUTPATH_DASHBOARD"], CONFIG["OUTPATHS"]['ASISTENCIA_TABLE'])
    ruta_csv_ausentismo = os.path.join(CONFIG["PATHS"]["FOLDER_OUTPATH_DASHBOARD"], CONFIG["OUTPATHS"]['AUSENTISMO_TABLE'])
    ruta_csv_cobertura = os.path.join(CONFIG["PATHS"]["FOLDER_OUTPATH_DASHBOARD"], CONFIG["OUTPATHS"]['COBERTURA_TABLE'])

    #
    df_hechos.to_csv(ruta_csv_hechos, index=False, encoding='utf-8')
    df_hc.to_csv(ruta_csv_hc, index=False, encoding='utf-8')
    df_bajas.to_csv(ruta_csv_hc_bajas, index=False, encoding='utf-8')
    df_puestos_homologados.to_csv(ruta_csv_puestos, index=False, encoding='utf-8')
    df_cursos.to_csv(ruta_csv_cursos, index=False, encoding='utf-8')
    df_asistencia.to_csv(ruta_csv_asistencia, index=False, encoding='utf-8')
    df_ausentismo.to_csv(ruta_csv_ausentismo, index=False, encoding='utf-8')
    df_cobertura.to_csv(ruta_csv_cobertura, index=False, encoding='utf-8')

    print("ETL de Base de Datos HC completado.")
    # Opcionalmente, podrías retornar los DataFrames si necesitas pasarlos directamente.
    return df_hc, df_bajas, df_puestos_homologados, df_cursos, df_asistencia, df_ausentismo, df_cobertura

if __name__ == "__main__":
    run_hc_etl()