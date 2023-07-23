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
        dates = [start_date - relativedelta(months=i) for i in range(12)]
        
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
            query = f"""
    SELECT * FROM (SELECT CASE 
        WHEN (H.ArtCod = 011110101 OR H.ArtCod = 011110102 OR H.ArtCod = 011110103) THEN 'ARROZ'
        WHEN (H.ArtCod = 011120305) THEN 'AVENA'
        WHEN (H.ArtCod = 011140102 OR H.ArtCod = 011140101 OR H.ArtCod = 011140103) THEN 'PASTAS ALIMENTICIAS'
        WHEN (H.ArtCod = 011130102) THEN 'PAN DULCE CORRIENTE O DE MANTECA'
        WHEN (H.ArtCod = 011130101) THEN 'PAN TIPO FRANCES'
        WHEN (H.ArtCod = 011150101) THEN 'TORTILLA DE MAIZ'
        WHEN (H.ArtCod  =011430103) THEN 'CREMA FRESCA'
        WHEN (H.ArtCod = 011410101) THEN 'LECHE EN POLVO'
        WHEN (H.ArtCod = 011410103 OR H.ArtCod = 011410102) THEN 'LECHE LIQUIDA'
        WHEN (H.ArtCod = 011430101) THEN 'QUESO FRESCO'
        WHEN (H.ArtCod = 011220103 OR H.ArtCod = 011220101) THEN 'CERDO SIN HUESO'
        WHEN (H.ArtCod = 011230102 OR H.ArtCod = 011230103 OR H.ArtCod = 011230101) THEN 'POLLO'
        WHEN (H.ArtCod = 011210105 OR H.ArtCod = 011210107) THEN 'CARNE DE RES CON HUESO'
        WHEN (H.ArtCod = 011210102 OR H.ArtCod = 011210103 OR H.ArtCod = 011210101 OR H.ArtCod = 011210106 OR H.ArtCod = 011210104) THEN 'CARNE DE RES SIN HUESO'
        WHEN (H.ArtCod = 011240101) THEN 'SALCHICHA'
        WHEN (H.ArtCod = 011440101) THEN 'HUEVOS DE GALLINA'
        WHEN (H.ArtCod = 011710802 OR H.ArtCod = 011710801) THEN 'FRIJOL NEGRO'
        WHEN (H.ArtCod = 011810101) THEN 'AZUCAR BLANCA GRANULADA'
        WHEN (H.ArtCod = 011520101) THEN 'ACEITE VEGETAL'
        WHEN (H.ArtCod = 011610101) THEN 'AGUACATE'
        WHEN (H.ArtCod = 011610201) THEN 'BANANO'
        WHEN (H.ArtCod = 011610603) THEN 'PIÑA'
        WHEN (H.ArtCod = 011610501) THEN 'PLATANOS'
        WHEN (H.ArtCod = 011610605) THEN 'SANDIAS'
        WHEN (H.ArtCod = 011711101) THEN 'CEBOLLA'
        WHEN (H.ArtCod = 011710201) THEN 'GUISQUIL'
        WHEN (H.ArtCod = 011712002) THEN 'HIERBAS FRESCAS'
        WHEN (H.ArtCod = 011711201) THEN 'PAPA'
        WHEN (H.ArtCod = 011710102 OR H.ArtCod = 011710101) THEN 'TOMATE'
        WHEN (H.ArtCod = 012130101) THEN 'AGUAS GASEOSAS'
        WHEN (H.ArtCod = 012110101 OR H.ArtCod = 012110201) THEN 'CAFÉ EN GRANO MOLIDO'
        WHEN (H.ArtCod = 011120304) THEN 'INCAPARINA'
        WHEN (H.ArtCod = 011910501) THEN 'SAL'
        WHEN (H.ArtCod = 011930301) THEN 'SOPAS INSTANTANEAS VASO'
        WHEN (H.ArtCod = 044210101) THEN 'GAS PROPANO'
        WHEN (H.ArtCod = 072210101) THEN 'GASOLINA SUPERIOR'
        WHEN (H.ArtCod = 072210201) THEN 'GASOLINA REGULAR'
        WHEN (H.ArtCod = 072210301) THEN 'DIESEL'
        ELSE 'No CBA'
    END AS 'Canasta Básica', H.PerAno, H.PerMes, H.PerSem, H.ArtCod, H.BolNum, H.ArtPac, H.ArtPhi, H.UreCan, H.UraChi, H.UmeCan, H.ArtCR, H.ArtSI, H.RegCod, H.ArtNOm, H.ArtPrc, H.TfnCod, H.TfnNom, H.FntCod, H.FntNom, H.FntDir, H.DepCod, H.MunCod FROM 
    (SELECT a.PerAno, a.PerMes, a.PerSem, a.ArtCod, b.BolNum, a.ArtPac, a.ArtPhi, a.UreCan, a.UraChi, d.UmeCan, a.ArtCR, a.ArtSI, b.RegCod, d.ArtNOm, d.ArtPrc, e.TfnCod, e.TfnNom, c.FntCod, c.FntNom, c.FntDir, b.DepCod, b.MunCod
    FROM IPC2010_01_RN.dbo.IPC104 a 
    INNER JOIN IPC2010_01_RN.dbo.IPC103 b ON (a.BolNum = b.BolNum AND a.PerAno = b.PerAno AND a.PerMes = b.PerMes AND a.PerSem = b.PerSem AND a.RegCod = b.RegCod)
    INNER JOIN IPC2010_01_RN.dbo.IPC010 c ON (c.FntCod = b.FntCod AND c.DepCod = b.DepCod AND c.MunCod =b.MunCod)
    INNER JOIN IPC2010_01_RN.dbo.IPC007 d ON a.ArtCod = d.ArtCod
    INNER JOIN IPC2010_01_RN.dbo.IPC008 e ON c.TfnCod = e.TfnCod
    WHERE a.ArtCod IN (SELECT DISTINCT ArtCod FROM IPC2010_01_RN.dbo.IPCPH6 WHERE PerAno = 2010 AND PerMes = 12 AND ArtCod != 091110301)
    UNION ALL
    SELECT a.PerAno, a.PerMes, a.PerSem, a.ArtCod, b.BolNum, a.ArtPac, a.ArtPhi, a.UreCan, a.UraChi, d.UmeCan, a.ArtCR, a.ArtSI, b.RegCod, d.ArtNOm, d.ArtPrc, e.TfnCod, e.TfnNom, c.FntCod, c.FntNom, c.FntDir, b.DepCod, b.MunCod
    FROM IPC2010_02_RN.dbo.IPC104 a 
    INNER JOIN IPC2010_02_RN.dbo.IPC103 b ON (a.BolNum = b.BolNum AND a.PerAno = b.PerAno AND a.PerMes = b.PerMes AND a.PerSem = b.PerSem  AND a.RegCod = b.RegCod)
    INNER JOIN IPC2010_02_RN.dbo.IPC010 c ON (c.FntCod = b.FntCod AND c.DepCod = b.DepCod AND c.MunCod =b.MunCod)
    INNER JOIN IPC2010_02_RN.dbo.IPC007 d ON a.ArtCod = d.ArtCod
    INNER JOIN IPC2010_02_RN.dbo.IPC008 e ON c.TfnCod = e.TfnCod
    WHERE a.ArtCod IN (SELECT DISTINCT ArtCod FROM IPC2010_01_RN.dbo.IPCPH6 WHERE PerAno = 2010 AND PerMes = 12 AND ArtCod != 091110301)
    UNION ALL 
    SELECT a.PerAno, a.PerMes, a.PerSem, a.ArtCod, b.BolNum, a.ArtPac, a.ArtPhi, a.UreCan, a.UraChi, d.UmeCan, a.ArtCR, a.ArtSI, b.RegCod, d.ArtNOm, d.ArtPrc, e.TfnCod, e.TfnNom, c.FntCod, c.FntNom, c.FntDir, b.DepCod, b.MunCod
    FROM IPC2010_03_RN.dbo.IPC104 a 
    INNER JOIN IPC2010_03_RN.dbo.IPC103 b ON (a.BolNum = b.BolNum AND a.PerAno = b.PerAno AND a.PerMes = b.PerMes AND a.PerSem = b.PerSem  AND a.RegCod = b.RegCod)
    INNER JOIN IPC2010_03_RN.dbo.IPC010 c ON (c.FntCod = b.FntCod AND c.DepCod = b.DepCod AND c.MunCod =b.MunCod)
    INNER JOIN IPC2010_03_RN.dbo.IPC007 d ON a.ArtCod = d.ArtCod
    INNER JOIN IPC2010_03_RN.dbo.IPC008 e ON c.TfnCod = e.TfnCod
    WHERE a.ArtCod IN (SELECT DISTINCT ArtCod FROM IPC2010_01_RN.dbo.IPCPH6 WHERE PerAno = 2010 AND PerMes = 12 AND ArtCod != 091110301)
    UNION ALL 
    SELECT a.PerAno, a.PerMes, a.PerSem, a.ArtCod, b.BolNum, a.ArtPac, a.ArtPhi, a.UreCan, a.UraChi, d.UmeCan, a.ArtCR, a.ArtSI, b.RegCod, d.ArtNOm, d.ArtPrc, e.TfnCod, e.TfnNom, c.FntCod, c.FntNom, c.FntDir, b.DepCod, b.MunCod
    FROM IPC2010_04_RN.dbo.IPC104 a 
    INNER JOIN IPC2010_04_RN.dbo.IPC103 b ON (a.BolNum = b.BolNum AND a.PerAno = b.PerAno AND a.PerMes = b.PerMes AND a.PerSem = b.PerSem  AND a.RegCod = b.RegCod)
    INNER JOIN IPC2010_04_RN.dbo.IPC010 c ON (c.FntCod = b.FntCod AND c.DepCod = b.DepCod AND c.MunCod =b.MunCod)
    INNER JOIN IPC2010_04_RN.dbo.IPC007 d ON a.ArtCod = d.ArtCod
    INNER JOIN IPC2010_04_RN.dbo.IPC008 e ON c.TfnCod = e.TfnCod
    WHERE a.ArtCod IN (SELECT DISTINCT ArtCod FROM IPC2010_01_RN.dbo.IPCPH6 WHERE PerAno = 2010 AND PerMes = 12 AND ArtCod != 091110301)
    UNION ALL 
    SELECT a.PerAno, a.PerMes, a.PerSem, a.ArtCod, b.BolNum, a.ArtPac, a.ArtPhi, a.UreCan, a.UraChi, d.UmeCan, a.ArtCR, a.ArtSI, b.RegCod, d.ArtNOm, d.ArtPrc, e.TfnCod, e.TfnNom, c.FntCod, c.FntNom, c.FntDir, b.DepCod, b.MunCod
    FROM IPC2010_05_RN.dbo.IPC104 a 
    INNER JOIN IPC2010_05_RN.dbo.IPC103 b ON (a.BolNum = b.BolNum AND a.PerAno = b.PerAno AND a.PerMes = b.PerMes AND a.PerSem = b.PerSem  AND a.RegCod = b.RegCod)
    INNER JOIN IPC2010_05_RN.dbo.IPC010 c ON (c.FntCod = b.FntCod AND c.DepCod = b.DepCod AND c.MunCod =b.MunCod)
    INNER JOIN IPC2010_05_RN.dbo.IPC007 d ON a.ArtCod = d.ArtCod
    INNER JOIN IPC2010_05_RN.dbo.IPC008 e ON c.TfnCod = e.TfnCod
    WHERE a.ArtCod IN (SELECT DISTINCT ArtCod FROM IPC2010_01_RN.dbo.IPCPH6 WHERE PerAno = 2010 AND PerMes = 12 AND ArtCod != 091110301)
    UNION ALL 
    SELECT a.PerAno, a.PerMes, a.PerSem, a.ArtCod, b.BolNum, a.ArtPac, a.ArtPhi, a.UreCan, a.UraChi, d.UmeCan, a.ArtCR, a.ArtSI, b.RegCod, d.ArtNOm, d.ArtPrc, e.TfnCod, e.TfnNom, c.FntCod, c.FntNom, c.FntDir, b.DepCod, b.MunCod
    FROM IPC2010_06_RN.dbo.IPC104 a 
    INNER JOIN IPC2010_06_RN.dbo.IPC103 b ON (a.BolNum = b.BolNum AND a.PerAno = b.PerAno AND a.PerMes = b.PerMes AND a.PerSem = b.PerSem  AND a.RegCod = b.RegCod)
    INNER JOIN IPC2010_06_RN.dbo.IPC010 c ON (c.FntCod = b.FntCod AND c.DepCod = b.DepCod AND c.MunCod =b.MunCod)
    INNER JOIN IPC2010_06_RN.dbo.IPC007 d ON a.ArtCod = d.ArtCod
    INNER JOIN IPC2010_06_RN.dbo.IPC008 e ON c.TfnCod = e.TfnCod
    WHERE a.ArtCod IN (SELECT DISTINCT ArtCod FROM IPC2010_01_RN.dbo.IPCPH6 WHERE PerAno = 2010 AND PerMes = 12 AND ArtCod != 091110301)
    UNION ALL 
    SELECT a.PerAno, a.PerMes, a.PerSem, a.ArtCod, b.BolNum, a.ArtPac, a.ArtPhi, a.UreCan, a.UraChi, d.UmeCan, a.ArtCR, a.ArtSI, b.RegCod, d.ArtNOm, d.ArtPrc, e.TfnCod, e.TfnNom, c.FntCod, c.FntNom, c.FntDir, b.DepCod, b.MunCod
    FROM IPC2010_07_RN.dbo.IPC104 a 
    INNER JOIN IPC2010_07_RN.dbo.IPC103 b ON (a.BolNum = b.BolNum AND a.PerAno = b.PerAno AND a.PerMes = b.PerMes AND a.PerSem = b.PerSem  AND a.RegCod = b.RegCod)
    INNER JOIN IPC2010_07_RN.dbo.IPC010 c ON (c.FntCod = b.FntCod AND c.DepCod = b.DepCod AND c.MunCod =b.MunCod)
    INNER JOIN IPC2010_07_RN.dbo.IPC007 d ON a.ArtCod = d.ArtCod
    INNER JOIN IPC2010_07_RN.dbo.IPC008 e ON c.TfnCod = e.TfnCod
    WHERE a.ArtCod IN (SELECT DISTINCT ArtCod FROM IPC2010_01_RN.dbo.IPCPH6 WHERE PerAno = 2010 AND PerMes = 12 AND ArtCod != 091110301)
    UNION ALL 
    SELECT a.PerAno, a.PerMes, a.PerSem, a.ArtCod, b.BolNum, a.ArtPac, a.ArtPhi, a.UreCan, a.UraChi, d.UmeCan, a.ArtCR, a.ArtSI, b.RegCod, d.ArtNOm, d.ArtPrc, e.TfnCod, e.TfnNom, c.FntCod, c.FntNom, c.FntDir, b.DepCod, b.MunCod
    FROM IPC2010_08_RN.dbo.IPC104 a 
    INNER JOIN IPC2010_08_RN.dbo.IPC103 b ON (a.BolNum = b.BolNum AND a.PerAno = b.PerAno AND a.PerMes = b.PerMes AND a.PerSem = b.PerSem  AND a.RegCod = b.RegCod)
    INNER JOIN IPC2010_08_RN.dbo.IPC010 c ON (c.FntCod = b.FntCod AND c.DepCod = b.DepCod AND c.MunCod =b.MunCod)
    INNER JOIN IPC2010_08_RN.dbo.IPC007 d ON a.ArtCod = d.ArtCod
    INNER JOIN IPC2010_08_RN.dbo.IPC008 e ON c.TfnCod = e.TfnCod
    WHERE a.ArtCod IN (SELECT DISTINCT ArtCod FROM IPC2010_01_RN.dbo.IPCPH6 WHERE PerAno = 2010 AND PerMes = 12 AND ArtCod != 091110301)) H
    WHERE H.PerAno = {anio} AND H.PerMes = {mes}) J"""
            df_Fnt = pd.read_sql(query, self.conexion)
            columnas = ('RegCod', 'MunCod', 'DepCod', 'PerAno', 'PerMes')
            df_Fnt = df_Fnt.astype(dict.fromkeys(columnas, "int64"), errors='ignore')
            return df_Fnt
