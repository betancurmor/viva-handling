import pandas as pd
import os
import unicodedata

# --- CONFIGURACION DE RUTAS Y NOMBRES ---
CONFIG = {
    "PATHS": {
        "FILE_MAESTRO_HC": r"C:\Users\bryan.betancur\OneDrive - Vivaaerobus\archivos_compartidos\GESTION HUMANA\BASE DE DATOS.xlsx",
        "FILE_DATOS_ADICIONALES_HC": r"C:\Users\bryan.betancur\OneDrive - Vivaaerobus\archivos_compartidos\12. Compartida\1. Bryan\Archivos_Entrenamiento\BASE DE DATOS_ADICIONAL.xlsx",
        "FILE_PUESTOS": r"C:\Users\bryan.betancur\OneDrive - Vivaaerobus\archivos_compartidos\12. Compartida\1. Bryan\Tabla_Homologacion.xlsx",
        "FILE_ENTRENAMIENTO": r"C:\Users\bryan.betancur\OneDrive - Vivaaerobus\archivos_compartidos\12. Compartida\1. Bryan\Registro_Entrenamiento.xlsm",
        "FILE_COBERTURA": r"C:\Users\bryan.betancur\OneDrive - Vivaaerobus\archivos_compartidos\12. Compartida\1. Bryan\Cobertura.xlsx",
       "FOLDER_RELOJ_CHECADOR": r"C:\Users\bryan.betancur\OneDrive - Vivaaerobus\archivos_compartidos\12. Compartida\1. Bryan\Faltas",
        "FOLDER_ROSTER": r"C:\Users\bryan.betancur\OneDrive - Vivaaerobus\archivos_compartidos\12. Compartida\1. Bryan\Roster",
        "FOLDER_OUTPATH": r".\data\processed",        
        "FOLDER_OUTPATH_DASHBOARD": r".\data\processed\dashboard_tables"
    },
    "SHEETS_NAMES": {
        "MAESTRO_HC" : 'BASE DE DATOS',
        "DATOS_ADICIONALES_HC": 'Datos',
        "BAJAS_HC": 'BAJAS',
        # "ENTRENAMIENTO_SHEETS": ['AVSEC', 'SMS', 'SAT'],
        "ENTRENAMIENTO": 'Base',
        "PROGRAMACION": 'Programacion',
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
    df_bajas = cargar_transformar_excel(CONFIG["PATHS"]["FILE_MAESTRO_HC"], sheet_name=CONFIG["SHEETS_NAMES"]["BAJAS_HC"], header=0)

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
    df_bajas = df_bajas.drop_duplicates().sort_values(by='#emp', ascending=False)

    # Datos Adicionales HC
    df_datos_adicionales_hc = cargar_transformar_excel(CONFIG['PATHS']['FILE_DATOS_ADICIONALES_HC'], sheet_name=CONFIG['SHEETS_NAMES']['DATOS_ADICIONALES_HC'], header=0)
    df_datos_adicionales_hc = df_datos_adicionales_hc[['#emp', 'direccion', 'correo_electronico']]
    cols_text= ['#emp', 'correo_electronico']
    for col in cols_text:
        if col in df_datos_adicionales_hc.columns:
         df_datos_adicionales_hc[col] = limpiar_columna_texto(df_datos_adicionales_hc[col], caracteres_a_eliminar= ' ')
    df_datos_adicionales_hc['#emp'] = limpiar_columna_id(df_datos_adicionales_hc['#emp'])

    # --- Nexos
    # ---- Base 'Entrenamiento'
    df_entrenamiento = cargar_transformar_excel(CONFIG['PATHS']['FILE_ENTRENAMIENTO'], sheet_name=CONFIG['SHEETS_NAMES']['ENTRENAMIENTO'], header=8)
    text_cols = ['#emp', 'curso', 'estatus_vigencia']
    for col in text_cols:
        if col in df_entrenamiento.columns:
            df_entrenamiento[col] = limpiar_columna_texto(df_entrenamiento[col], caracteres_a_eliminar= ' ')
    # df_entrenamiento['curso'] = df_entrenamiento['curso'].str.replace('SAT(Op)', 'SAT', regex=False)
    df_entrenamiento = df_entrenamiento.rename(columns={'fecha_vigencia': 'l.d'})
    df_entrenamiento = df_entrenamiento.rename(columns={'fecha_programada': 'e.d'})
    date_cols = ['fecha_curso', 'l.d', 'e.d']
    for col in date_cols:
        if col in df_entrenamiento.columns:
            df_entrenamiento[col] = limpiar_columna_fecha(df_entrenamiento[col])
        else:
            df_entrenamiento[col] = pd.NaT
    if '#emp' in df_entrenamiento.columns:
        df_entrenamiento['#emp'] = limpiar_columna_id(df_entrenamiento['#emp'])
    else:
        df_entrenamiento['#emp'] = 0           
    df_entrenamiento = df_entrenamiento[['#emp', 'curso', 'fecha_curso', 'l.d', 'estatus_vigencia', 'e.d']]
    df_entrenamiento = df_entrenamiento.drop_duplicates().sort_values(['#emp', 'curso'])

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

    # # Turnos: 'Roster'
    # # PDTE - Validar con Adriana
    try:
        archivos = [
            f for f in os.listdir(CONFIG['PATHS']['FOLDER_ROSTER'])
            if f.endswith('.csv')
        ]
    except Exception as e:
        print(f"\nError: Revisa la carpeta 'Archivos_Entrenamiento', no se encontraron archivos validos.")

    dfs = []
    for a in archivos:
        ruta = os.path.join(CONFIG['PATHS']['FOLDER_ROSTER'], a)
        nombre_archivo = os.path.splitext(a)[0]
        nombre_archivo = nombre_archivo.replace(' ', '').lower()  # Elimina espacios y convierte a minusculas
        mes_archivo = nombre_archivo.split('_')[1]  # Asumiendo formato 'Roster_Mes_Año.csv'
        año_archivo = nombre_archivo.split('_')[2]
        fecha_archivo = '01/' + mes_archivo + '/' + año_archivo
        fecha_limpia = limpiar_columna_fecha(fecha_archivo)
        df = cargar_transformar_csv(ruta, header=3, encoding='ansi')

        # FALTA AÑADIR COLUMNA FECHA AL DF, ADICIONALMENTE COMPARAR FECHA PARA OBTENER ULTIMO ROSTER (DIA-HOY)
        # df['mes'] = mes_archivo
        dfs.append(df)
    df_roster = pd.concat(dfs, ignore_index= True)
    df_roster = df_roster.rename(columns={'id': '#emp'})
    df_turnos = df_roster.iloc[:, [0] + list(range(-8, -1))] # Selecciona la primera columna y las ultimas 7 columnas   

    for c in df_turnos.columns:
        df_turnos[c] = limpiar_columna_texto(df_turnos[c], caracteres_a_eliminar= ' ')
    df_turnos['#emp'] = limpiar_columna_id(df_turnos['#emp'])

    # Merge: 'df_hc', 'df_datos_adicionales_hc'
    df_adicionales_hc = pd.merge(
        df_hc,
        df_datos_adicionales_hc[['#emp', 'direccion', 'correo_electronico']],
        left_on=['#emp'],
        right_on=['#emp'],
        how='left'
    )

    # Merge: 'df_adicionales_hc', 'df_puestos'
    df_hc_puente = pd.merge(
        df_adicionales_hc,
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

    df_hc = df_hc_temp[['#emp', 'id_puesto', 'nombre_completo', 'paterno', 'materno', 'nombre', 'rfc', 'curp', 'telefono', 'direccion', 'correo_electronico', 'estatus', 'fecha_alta', 'fecha_baja', 'fecha_antiguedad', 'fecha_nacimiento', 'novedades_comentarios']]

    # --- Tabla 'Asistencia Entrenamiento'
    # --- Nueva logica para el registro de asistencia
    df_asistencia = cargar_transformar_excel(CONFIG['PATHS']['FILE_ENTRENAMIENTO'], sheet_name=CONFIG['SHEETS_NAMES']['PROGRAMACION'], header=5)
    df_asistencia = df_asistencia[['#emp', 'curso', 'fecha_programada', 'asistencia', 'motivo']]
    text_cols = ['#emp', 'curso', 'asistencia', 'motivo']
    for col in text_cols:
        if col in df_asistencia.columns:
            df_asistencia[col] = limpiar_columna_texto(df_asistencia[col])
    df_asistencia['#emp'] = limpiar_columna_id(df_asistencia['#emp'])
    df_asistencia['fecha_programada'] = limpiar_columna_fecha(df_asistencia['fecha_programada'])
    df_asistencia = df_asistencia.drop_duplicates().dropna(how='all').sort_values(by='#emp', ascending=False)
    df_asistencia = df_asistencia[df_asistencia['#emp'] != 0]

    # Merge: 'df_entrenamiento', 'df_asistencia'
    df_entrenamiento_asistencia = pd.merge(
        df_entrenamiento,
        df_asistencia[['#emp', 'curso', 'fecha_programada', 'asistencia', 'motivo']],
        left_on=['#emp', 'curso', 'e.d'],
        right_on=['#emp', 'curso', 'fecha_programada']
    )
    df_entrenamiento_asistencia = df_entrenamiento_asistencia[['#emp', 'curso', 'fecha_curso', 'l.d', 'estatus_vigencia', 'e.d', 'asistencia', 'motivo']]
    df_entrenamiento_asistencia = df_entrenamiento_asistencia.sort_values(by='#emp', ascending=False)

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
    df_hechos = df_entrenamiento_asistencia_cursos_status[['#emp', 'id_curso', 'id_estatus_vigencia', 'l.d', 'e.d', 'fecha_curso', 'asistencia', 'motivo']]
    df_hechos = df_hechos.sort_values(by=['#emp', 'id_curso']).reset_index(drop=True)

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