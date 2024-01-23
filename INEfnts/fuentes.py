import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from dateutil.relativedelta import relativedelta
import datetime

class Fuentes:
    def __init__(self, conexion) -> None:
        self.conexion = conexion

    def get_boletas(self, anio: int, mes: int) -> pd.DataFrame:
        dfs = []

        # Genera una lista de los últimos 12 meses
        start_date = datetime.date(anio, mes, 1)
        dates = [start_date - relativedelta(months=i) for i in range(12)]
        
        # Utiliza un ProcessPoolExecutor para paralelizar las llamadas a la función boletas
        with ProcessPoolExecutor() as executor:
            futures = {executor.submit(self.boletas, date.year, date.month): date for date in dates}
            
            # A medida que las llamadas a boletas se completan, guarda los resultados en dfs
            for future in as_completed(futures):
                date = futures[future]
                try:
                    df = future.result()
                    dfs.append(df)
                except Exception as exc:
                    print(f'{date} generated an exception: {exc}')
                    
        # Concatena todos los dataframes
        df_final = pd.concat(dfs, ignore_index=True)
        
        return df_final

    def boletas_ultimos_12_meses(self, anio: int, mes: int) -> pd.DataFrame:
        dfs = []

        # Genera una lista de los últimos 12 meses
        start_date = datetime.date(anio, mes, 1)
        dates = [start_date - relativedelta(months=i) for i in range(13)]
        
        # Llama a la función boletas para cada mes y guarda los resultados en dfs
        for date in dates:
            try:
                df = self.boletas(date.year, date.month)
                dfs.append(df)
            except Exception as exc:
                print(f'{date} generated an exception: {exc}')
                    
        # Concatena todos los dataframes
        df_final = pd.concat(dfs, ignore_index=True)
        
        return df_final

    def boletas(self, anio: int, mes: int) -> pd.DataFrame:
        query = f"EXEC [dbo].[sp_get_precios_recolectados_mes] {anio}, {mes}"
        df_Fnt = pd.read_sql(query, self.conexion)
        nombres_estandar = {
            'anio':                 'PerAno',
            'mes':                  'PerMes',
            'decada':               'PerSem',
            'codigo_articulo':      'ArtCod',
            'ine_poll_id':          'BolNum',
            'precio_actual':        'ArtPac',
            'precio_anterior':      'ArtPhi',
            'cantidad_actual':      'UreCan',
            'cantidad_anterior':    'UraChi',
            'cantidad_base':        'UmeCan',
            'nt_tipo':              'ArtCR',
            'nt_tipo_nombre':       'ArtSI',
            'region':               'RegCod',
            'articulo':             'ArtNOm',
            'periodicidad':         'ArtPrc',
            'tp_source_type':       'TfnCod',
            'fuente_tipo':          'TfnNom',
            'fuente_codigo':        'FntCod',
            'fuente_nombre':        'FntNom',
            'fuente_direccion':     'FntDir',
            'Codigo_Departamento':  'DepCod',
            'Codigo_Municipio':     'MunCod'
        }
        df_Fnt = df_Fnt.rename(columns=nombres_estandar)
        # IPC no necesita CBA, probalemente esta columna se usa en otro script.
        # Por si acaso, la columna se incluye.
        df_Fnt['Canasta Básica'] = 'No CBA'
        mapeo_decada = {
            'Primera década': 1,
            'Segunda década': 2,
            'Tercera década': 3
        }
        df_Fnt['PerSem'] = df_Fnt['PerSem'].replace(mapeo_decada)
        df_Fnt['ArtCR'] = df_Fnt.ArtCR.map({351762: 'S'}).fillna('N')
        df_Fnt['ArtSI'] = df_Fnt.ArtSI.map({'Sustitución Inmediata': 'S'}).fillna('N')
        # Sustituir la periodicidad por números
        mapeo_periodicidad = {
            'Mensual':      1,
            'Trimestral':   3,
            'Semestral':    6,
            'Anual':        12
        }
        df_Fnt['ArtPrc'] = df_Fnt.ArtPrc.map(mapeo_periodicidad)
        orden_columnas = [
            'Canasta Básica',
            'PerAno', 
            'PerMes',
            'PerSem',
            'ArtCod',
            'BolNum',
            'ArtPac',
            'ArtPhi',
            'UreCan',
            'UraChi',
            'UmeCan',
            'ArtCR',
            'ArtSI',
            'RegCod',
            'ArtNOm',
            'ArtPrc',
            'TfnCod',
            'TfnNom',
            'FntCod',
            'FntNom',
            'FntDir',
            'DepCod',
            'MunCod'
        ]
        df_Fnt = df_Fnt[orden_columnas]
        columnas = ('RegCod', 'MunCod', 'DepCod', 'PerAno', 'PerMes')
        df_Fnt = df_Fnt.astype(dict.fromkeys(columnas, "int64"), errors='ignore')
        return df_Fnt
