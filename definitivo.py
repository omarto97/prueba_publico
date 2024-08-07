import pandas as pd
import numpy as np
import time
start_time = time.time()

# Cargar datos y convertir tipos
ruta1 = "C:\\Users\\dogor\\Desktop\\DATOS DE PRUEBA\\ops_teps.xlsx"
ruta2 = "C:\\Users\\dogor\\Desktop\\DATOS DE PRUEBA\\ops_padre.xlsx"
ruta3 = "C:\\Users\\dogor\\Desktop\\DATOS DE PRUEBA\\ops_teps_proceso_principal.xlsx"
ruta4 = "C:\\Users\\dogor\\Desktop\\DATOS DE PRUEBA\\ops_grupos.xlsx"
ruta5 = "C:\\Users\\dogor\\Desktop\\DATOS DE PRUEBA\\ops_correcciones.xlsx"
ruta6 = "C:\\Users\\dogor\\Desktop\\DATOS DE PRUEBA\\ops_movsinv.xlsx"
ruta7 = "C:\\Users\\dogor\\Desktop\\DATOS DE PRUEBA\\ops_costos_mo.xlsx"
ruta8 = "C:\\Users\\dogor\\Desktop\\DATOS DE PRUEBA\\ops_agrupacion_ccostos.xlsx"
ruta9 = "C:\\Users\\dogor\\Desktop\\DATOS DE PRUEBA\\eq_cc.xlsx"
ruta10 = "C:\\Users\\dogor\\Desktop\\DATOS DE PRUEBA\\ops_correccion_mvi.xlsx"
ruta11 = "C:\\Users\\dogor\\Desktop\\DATOS DE PRUEBA\\ops_clasificaciones_paros.xlsx"
ruta12 = "C:\\Users\\dogor\\Desktop\\DATOS DE PRUEBA\\ops_entidades_paros.xlsx"
ruta13 = "C:\\Users\\dogor\\Desktop\\DATOS DE PRUEBA\\ops_clasificaciones_fallas.xlsx"
ruta14 = "C:\\Users\\dogor\\Desktop\\DATOS DE PRUEBA\\ops_entidades_fallas.xlsx"

# Extraccion de datos
ops_teps = pd.read_excel(ruta1, dtype=str)
ops_padre = pd.read_excel(ruta2, dtype=str)
ops_procesos_especificos = pd.read_excel(ruta3, dtype=str)
ops_grupos = pd.read_excel(ruta4, dtype=str)
ops_correcciones = pd.read_excel(ruta5, dtype=str)
ops_movsinv = pd.read_excel(ruta6, dtype=str)
ops_costos_mo =  pd.read_excel(ruta7, dtype=str)
ops_agrupacion_ccostos =  pd.read_excel(ruta8, dtype=str)
equivalencias_cc =  pd.read_excel(ruta9, dtype=str)
ops_correcciones_mvi =  pd.read_excel(ruta10, dtype=str)
ops_clasificaciones_paros = pd.read_excel(ruta11, dtype=str)
ops_entidades_paros = pd.read_excel(ruta12, dtype=str)
ops_clasificaciones_fallas = pd.read_excel(ruta13, dtype=str)
ops_entidades_fallas = pd.read_excel(ruta14, dtype=str)


def fx_group_and_sum(tabla, group_cols, sum_col):
    return tabla.groupby(group_cols)[sum_col].sum().reset_index()
def fx_merge_multiple(dfs, on):
    result = dfs[0].set_index(on)
    # Iterar sobre los DataFrames restantes
    for df in dfs[1:]:
        # Realizar un outer join
        result = result.join(df.set_index(on), how='outer')
    # Restablecer el índice
    return result.reset_index()
def fx_regla_2do_nivel (row):
    if row['CENTRO DE TRABAJO'] == 'ACTIVIDADES SUPLEMENTARIAS':
        return "ACTIVIDADES SUPLEMENTARIAS"
    elif pd.isnull(row['CENTRO DE TRABAJO']):
        return None
    elif row['METODO'] != 'Estandar':
        return row['METODO']
    elif row['PROCESO PADRE'] == row['PROCESO']:
        return row['CENTRO DE TRABAJO']
    else:
        return None
def fx_regla_2do_nivel_orden (row):
    if row['CENTRO DE TRABAJO'] == 'ACTIVIDADES SUPLEMENTARIAS':
        return 1
    elif pd.isnull(row['CENTRO DE TRABAJO']):
        return 2
    elif row['METODO'] != 'Estandar':
        return 3
    elif row['PROCESO PADRE'] == row['PROCESO']:
        return 4
    else:
        return 5
def fx_clasificacion_operacion(row):
    if pd.isnull(row['CENTRO DE TRABAJO']):
        return 'OPERACION EXTERNA'
    elif row['CENTRO DE TRABAJO'] =='ACTIVIDADES SUPLEMENTARIAS':
        return 'ACTIVIDADES SUPLEMENTARIAS'
    elif row['CLASIFICACION OPERACION']=='ALISTAMIENTO':
        return 'ALISTAMIENTO'
    else:
        return 'OPERACION'
    
#-----------------------------------
                        # ----- 1. LIMPIEZA DE DATOS ------ INI
#-----------------------------------
# Convertir de tipos de datos de columnas a float y fecha
cols_to_float_teps = ['HORAS', 'HORAS ESTANDAR BASE', 'CANTIDAD COMPLETADA OP TEPS', 'CANTIDAD RECHAZADA OP TEPS', 'CANTIDAD BASE', 'CANTIDAD RETAL OP TEPS']
ops_teps[cols_to_float_teps] = ops_teps[cols_to_float_teps].astype(float)
cols_to_float_movs = ['KG SALIDA BASE', 'KG ENTRADA BASE', 'KG NETOS REAL BASE']
ops_movsinv[cols_to_float_movs] = ops_movsinv[cols_to_float_movs].astype(float)
ops_teps['FECHA CONTABILIZACION TEP'] = pd.to_datetime(ops_teps['FECHA CONTABILIZACION TEP'].str[:10])
ops_movsinv['FECHA CONTABILIZACION MVI'] = pd.to_datetime(ops_movsinv['FECHA CONTABILIZACION MVI'].str[:10])
#ops_costos_mo[['TIPO COSTO', 'CENTRO DE COSTO']] = ops_costos_mo[['TIPO COSTO', 'CENTRO DE COSTO']].astype(str)
ops_costos_mo['SALDO'] = ops_costos_mo['SALDO'].astype('float')
#ops_agrupacion_ccostos[['CENTRO DE COSTO ORIGINAL', 'CENTRO DE COSTO DISTRIBUCION']] = ops_agrupacion_ccostos[['CENTRO DE COSTO ORIGINAL', 'CENTRO DE COSTO DISTRIBUCION']].astype(str)
ops_clasificaciones_paros = ops_clasificaciones_paros.drop(['PARO'], axis=1)
ops_clasificaciones_paros['CODIGO PARO'] = ('000'+ ops_clasificaciones_paros['CODIGO PARO']).str[-3:]
ops_clasificaciones_paros['DATO 2'] = ops_clasificaciones_paros['DATO 2'].astype(str)
ops_clasificaciones_paros['DATO 2'] = ops_clasificaciones_paros['DATO 2'].apply(lambda x: x.split("-"))
ops_clasificaciones_paros['DATO 3'] = ops_clasificaciones_paros['DATO 3'].apply(lambda x: None if pd.isnull(x) else ('00'+ str(x))).str[-2:]
ops_entidades_paros['HORAS PARO'] = ops_entidades_paros['HORAS PARO'].astype(float)
ops_entidades_paros['CODIGO PARO'] = ops_entidades_paros['PARO'].apply(lambda x: ('000'+ (x.split("-")[0]))).str[-3:]
ops_entidades_paros = ops_entidades_paros.merge(ops_clasificaciones_paros, on= ['CODIGO PARO'], how='left')
ops_clasificaciones_fallas = ops_clasificaciones_fallas.drop(['FALLA'], axis=1)
ops_entidades_fallas['CANTIDAD FALLA'] = ops_entidades_fallas['CANTIDAD FALLA'].astype(float)
ops_entidades_fallas['CODIGO FALLA']= ops_entidades_fallas['FALLA'].apply(lambda x: ((x.split("-")[0])))
ops_entidades_fallas =ops_entidades_fallas[~ops_entidades_fallas['CODIGO FALLA'].isin(['MTS LINEALES'])]
ops_entidades_fallas = ops_entidades_fallas.merge(ops_clasificaciones_fallas, on=['CODIGO FALLA'], how='left')



# Calcular valor absoluto de las horas base
ops_teps['ABS HORAS BASE'] = ops_teps['HORAS ESTANDAR BASE'].abs()

# ----- 1. LIMPIEZA DE DATOS ------ FIN

#-----------------------------------
                    # ----- 2. CORRECCION DE DATOS ------ INI
#-----------------------------------

#Correccion de datos descripciones ops con archivos manuales
ops_correcciones['OP SIESA'] = ops_correcciones['OP SIESA'].str[4:]
ops_correcciones['NUMERO OPERACION ORIGINAL'] = ('0'+ops_correcciones['NUMERO OPERACION ORIGINAL']).str[-2:]
ops_correcciones['NUMERO OPERACION NUEVA'] = ('0'+ops_correcciones['NUMERO OPERACION NUEVA']).str[-2:]
ops_teps = ops_teps.merge(ops_correcciones, left_on=['DOCUMENTO OP', 'CODIGO OPERACION'], right_on=['OP SIESA', 'NUMERO OPERACION ORIGINAL'], how='left')
ops_teps['DESCRIPCION OPERACION'] = ops_teps['DESCRIPCION OPERACION NUEVA'].fillna(ops_teps['DESCRIPCION OPERACION'])
ops_teps['CODIGO OPERACION'] = ops_teps['NUMERO OPERACION NUEVA'].fillna(ops_teps['CODIGO OPERACION'])
ops_teps['CENTRO DE TRABAJO'] = ops_teps['CENTRO DE TRABAJO NUEVO'].fillna(ops_teps['CENTRO DE TRABAJO'])

#Correccion de kgs de los movimientos de inventario con archivos manuales
columnas_mvi = ops_movsinv.columns.values
print(columnas_mvi)
ops_movsinv = ops_movsinv.merge(ops_correcciones_mvi, left_on=['DOCUMENTO OP', 'DOCUMENTO INV', 'REFERENCIA MVI'], right_on=['DOCUMENTO OP', 'DOCUMENTO MOVIMIENTO DE INVENTARIO', 'REFERENCIA MOVIMIENTO DE INVENTARIO'], how='left')
ops_movsinv['KG SALIDA BASE'] = ops_movsinv.apply(lambda row: row['KG SALIDA BASE'] if pd.isnull(row['KG SALIDA NUEVOS']) else row['KG SALIDA NUEVOS'], axis=1)
ops_movsinv['KG ENTRADA BASE'] = ops_movsinv.apply(lambda row: row['KG ENTRADA BASE'] if pd.isnull(row['KG ENTRADA NUEVOS']) else row['KG ENTRADA NUEVOS'], axis=1)
ops_movsinv['KG NETOS REAL BASE'] = ops_movsinv.apply(lambda row: row['KG NETOS REAL BASE'] if pd.isnull(row['KG NETOS NUEVOS']) else row['KG NETOS NUEVOS'], axis=1)
ops_movsinv['UNIDAD INVENTARIO'] = ops_movsinv.apply(lambda row: row['UNIDAD INVENTARIO'] if pd.isnull(row['UNIDAD NUEVA']) else row['UNIDAD NUEVA'], axis=1)

# ----- 2. CORRECCION DE DATOS ------ FIN

#-----------------------------------
                        # ----- 3. COLUMNAS PRINCIPALES PP, PE, P2DO NIVEL, OPERACION PRINCIPAL, OP GRUPO, CENTRO DE COSTO ------ INI
#-----------------------------------

# Determinacion del proceso padre
ops_teps['DESCOP 5'] = ops_teps['DESCRIPCION OPERACION'].str[:5]
ops_teps = ops_teps.merge(ops_padre, on='DESCOP 5', how='left')
ops_teps.sort_values(by=['DOCUMENTO OP', 'ORDEN'], inplace=True)
ops_teps_proceso_principal = ops_teps.drop_duplicates(subset='DOCUMENTO OP', keep='first')
proceso_principal_dict = ops_teps_proceso_principal.set_index('DOCUMENTO OP')['PROCESO'].to_dict()
orden_proceso_principal_dict = ops_teps_proceso_principal.set_index('DOCUMENTO OP')['ORDEN'].to_dict()
ops_teps['PROCESO PADRE'] = ops_teps['DOCUMENTO OP'].map(proceso_principal_dict)
ops_teps['ORDEN PROCESO PADRE'] = ops_teps['DOCUMENTO OP'].map(orden_proceso_principal_dict)

#Determinacion de la operacion principal
operaciones_principales = ops_teps[ops_teps['PROCESO'] == ops_teps['PROCESO PADRE']].copy()
operaciones_principales = operaciones_principales.drop_duplicates(subset='DOCUMENTO OP', keep='first')
operacion_principal_dict = operaciones_principales.set_index('DOCUMENTO OP')['DESCRIPCION OPERACION'].to_dict()

#Determinacion del proceso de 2do nivel

ops_teps['PROCESO 2DO NIVEL'] = ops_teps.apply(fx_regla_2do_nivel, axis=1)
ops_teps['APLICACION PROCESO 2DO NIVEL'] = ops_teps.apply(fx_regla_2do_nivel_orden, axis=1)
ordenes_2donivel = ops_teps[~ops_teps['PROCESO 2DO NIVEL'].isnull()].copy()
ordenes_2donivel.sort_values(by=['DOCUMENTO OP', 'APLICACION PROCESO 2DO NIVEL'], inplace=True)
ops_teps_proceso_2donivel = ordenes_2donivel.drop_duplicates(subset='DOCUMENTO OP', keep='first')
proceso_2donivel_dict = ops_teps_proceso_2donivel.set_index('DOCUMENTO OP')['PROCESO 2DO NIVEL'].to_dict()
ops_teps['PROCESO 2DO NIVEL'] = ops_teps['DOCUMENTO OP'].map(proceso_2donivel_dict)

# Determinacion del tipo de proceso especifico y la clasificacion de operaciones
ops_teps['DESCOP 12'] = ops_teps['DESCRIPCION OPERACION'].str[:12]
ops_teps = ops_teps.merge(ops_procesos_especificos, on=['DESCOP 12'], how='left')
ops_teps['CLASIFICACION OPERACION'] = ops_teps['PROCESO ESPECIFICO'].str[:12]

ops_teps['CLASIFICACION OPERACION'] = ops_teps.apply(fx_clasificacion_operacion, axis =1)
ops_teps['PROCESO PADRE'] = ops_teps.apply(lambda row:row['CLASIFICACION OPERACION'] if pd.isnull(row['PROCESO PADRE']) else row['PROCESO PADRE'], axis=1)

# Determinacion del grupo de ops
ops_teps = ops_teps.merge(ops_grupos[['DOCUMENTO OP', 'DOCUMENTO AG - OP MONTAJE', 'DOCUMENTO OP MONTAJE']],on='DOCUMENTO OP', how='left')

#Determinacion del centro de costo equivalente
centro_trabajo_to_costo = equivalencias_cc.set_index('CODIGO CENTRO DE TRABAJO')['EQ CODIGO CENTRO DE COSTO'].to_dict()
ops_teps['CENTRO DE COSTO'] = ops_teps['CODIGO CENTRO DE TRABAJO'].map(centro_trabajo_to_costo)

# ----- 3. COLUMNAS PRINCIPALES PP, PE, P2D0, OPERACION, OP GRUPO ------ FIN

# Quitar columnas innecesarias 1
ops_teps.drop(columns=['ORDEN', 'OP SIESA', 'NUMERO OPERACION ORIGINAL', 'DESCRIPCION OPERACION NUEVA', 'NUMERO OPERACION NUEVA', 'CENTRO DE TRABAJO NUEVO', 'APLICACION PROCESO 2DO NIVEL'], inplace=True)



#-----------------------------------
                        # ----- 4. APLICACION DE CONTABILIZACION EN LOS CENTROS DE COSTO CON ESTANDAR Y CALCULO DE TARIFAS POR HORA ------ INI
#-----------------------------------

# Se obtiene el total de horas reales por periodo y centro para el cual se van a distribuir los valores mensuales de la contabilizacion.
horas_reales_costos = ops_teps.groupby(['PERIODO DE CONTABILIZACION TEP', 'CENTRO DE COSTO'])['HORAS'].sum().reset_index()
horas_reales_costos = horas_reales_costos[horas_reales_costos['HORAS']!=0]

# Se separa la ultima palabra que tiene NATURALEZA ESTANDAR O REAL (Nombres que se deben mantener en la consulta SQL)
ops_costos_mo['NATURALEZA'] = ops_costos_mo['TIPO COSTO NATURALEZA'].apply(lambda x: x.split()[-1])
# Se separa del resto del concepto la palabra que esta despues del ultimo espacio que corresponde a la naturaleza de forma que solo permanezca el tipo de costo.
ops_costos_mo['TIPO COSTO'] = ops_costos_mo['TIPO COSTO NATURALEZA'].apply(lambda x: ' '.join(x.split()[:-1]))

#Se separan los datos reales de los estandar (Ajustados a un valor positivo)
df_estandar = ops_costos_mo[ops_costos_mo['NATURALEZA'] == 'ESTANDAR'].copy()
df_estandar['SALDO'] = df_estandar['SALDO']*-1
df_real = ops_costos_mo[ops_costos_mo['NATURALEZA'] == 'REAL']

ops_costos_mo_transf=pd.DataFrame({
    'PERIODO': ['PRUEBA'],
    'TIPO COSTO': ['PRUEBA'],
    'CODIGO CENTRO DE COSTO': ['PRUEBA'],
    'DESCRIPCION CENTRO DE COSTO': ['PRUEBA'],
    'SALDO': [0]
})

# Se calculan los valores para cada periodo y tipo de costo 
for periodo in ops_costos_mo['PERIODO'].unique():
    for tipo_costo in ops_costos_mo['TIPO COSTO'].unique():
        real_df = df_real[(df_real['PERIODO'] == periodo) & (df_real['TIPO COSTO'] == tipo_costo)]
        estandar_df = df_estandar[(df_estandar['PERIODO'] == periodo) & (df_estandar['TIPO COSTO'] == tipo_costo)]
        
        # Se manteniene el valor real en centros de costo que existen tanto en real como en estandar y que no sean 301021
        centros_comunes = set(estandar_df['CODIGO CENTRO DE COSTO']).intersection(set(real_df['CODIGO CENTRO DE COSTO']))
        centros_comunes = centros_comunes - {'301021'}
        for centro in centros_comunes:
            valor_real = real_df[real_df['CODIGO CENTRO DE COSTO'] == centro]
            ops_costos_mo_transf = pd.concat([ops_costos_mo_transf, valor_real], ignore_index=True)

        # Excluir centros de costo 301021 del estándar de forma que no se sobrecargue el valor que se desconto en el paso anterior
        estandar_df = estandar_df[~estandar_df['CODIGO CENTRO DE COSTO'].isin(['301021'])]
        total_estandar = estandar_df['SALDO'].sum()
        
        for _, real_row in real_df.iterrows():
            centro_real = real_row['CODIGO CENTRO DE COSTO']
            desc_real = real_row['DESCRIPCION CENTRO DE COSTO']
            
            if centro_real not in estandar_df['CODIGO CENTRO DE COSTO'].values:
                valor_real = real_row['SALDO']
                
                # Aplicar reglas de ops_agrupacion_ccostos
                reglas = ops_agrupacion_ccostos[ops_agrupacion_ccostos['CENTRO DE COSTO ORIGINAL'] == centro_real]
                if not reglas.empty:
                    incluidas = reglas[reglas['TIPO DE INCLUSION'] == 'INCLUIDA']['CENTRO DE COSTO DISTRIBUCION']
                    excluidas = reglas[reglas['TIPO DE INCLUSION'] == 'EXCLUIDA']['CENTRO DE COSTO DISTRIBUCION']
                    
                    if not incluidas.empty:
                        estandar_df_incluidas = estandar_df[estandar_df['CODIGO CENTRO DE COSTO'].isin(incluidas)]
                        total_estandar_incluidas = estandar_df_incluidas['SALDO'].sum()
                        
                        for _, estandar_row in estandar_df_incluidas.iterrows():
                            proporcion = estandar_row['SALDO'] / total_estandar_incluidas
                            valor_distribuido = proporcion * valor_real
                            ops_costos_mo_transf = pd.concat([ops_costos_mo_transf, pd.DataFrame([{'PERIODO': periodo, 'TIPO COSTO': tipo_costo, 'CODIGO CENTRO DE COSTO': estandar_row['CODIGO CENTRO DE COSTO'], 'DESCRIPCION CENTRO DE COSTO': estandar_row['DESCRIPCION CENTRO DE COSTO'], 'SALDO': valor_distribuido, 'NATURALEZA': 'REAL', "PROPORCION": proporcion, "VALOR REAL ORIGINAL" : valor_real, "CENTRO DE COSTO ORIGINAL": centro_real, 'DESCRIPCION CENTRO DE COSTO ORIGINAL': desc_real, "ESTANDAR TOTAL": total_estandar_incluidas, "ESTANDAR BASE": estandar_row['SALDO']}])], ignore_index=True)
                    else:
                        estandar_df_excluidas = estandar_df[~estandar_df['CODIGO CENTRO DE COSTO'].isin(excluidas)]
                        total_estandar_excluidas = estandar_df_excluidas['SALDO'].sum()
                        
                        for _, estandar_row in estandar_df_excluidas.iterrows():
                            proporcion = estandar_row['SALDO'] / total_estandar_excluidas
                            valor_distribuido = proporcion * valor_real
                            ops_costos_mo_transf = pd.concat([ops_costos_mo_transf, pd.DataFrame([{'PERIODO': periodo, 'TIPO COSTO': tipo_costo, 'CODIGO CENTRO DE COSTO': estandar_row['CODIGO CENTRO DE COSTO'], 'DESCRIPCION CENTRO DE COSTO': estandar_row['DESCRIPCION CENTRO DE COSTO'],  'SALDO': valor_distribuido, 'NATURALEZA': 'REAL', "PROPORCION": proporcion, "VALOR REAL ORIGINAL" : valor_real, "CENTRO DE COSTO ORIGINAL": centro_real, 'DESCRIPCION CENTRO DE COSTO ORIGINAL': desc_real, "ESTANDAR TOTAL": total_estandar_excluidas, "ESTANDAR BASE": estandar_row['SALDO']}])], ignore_index=True)
                else:
                    for _, estandar_row in estandar_df.iterrows():
                        proporcion = estandar_row['SALDO'] / total_estandar
                        valor_distribuido = proporcion * valor_real
                        ops_costos_mo_transf = pd.concat([ops_costos_mo_transf, pd.DataFrame([{'PERIODO': periodo, 'TIPO COSTO': tipo_costo, 'CODIGO CENTRO DE COSTO': estandar_row['CODIGO CENTRO DE COSTO'], 'DESCRIPCION CENTRO DE COSTO': estandar_row['DESCRIPCION CENTRO DE COSTO'],  'SALDO': valor_distribuido, 'NATURALEZA': 'REAL', "PROPORCION": proporcion, "VALOR REAL ORIGINAL" : valor_real, "CENTRO DE COSTO ORIGINAL": centro_real, 'DESCRIPCION CENTRO DE COSTO ORIGINAL': desc_real, "ESTANDAR TOTAL": total_estandar, "ESTANDAR BASE": estandar_row['SALDO']}])], ignore_index=True)

# Unir los valores estandar originales
ops_costos_mo_transf= pd.concat([df_estandar, ops_costos_mo_transf[ops_costos_mo_transf['NATURALEZA'] == 'REAL']], ignore_index=True)

# El grupo de COSTO MOD HORAS EXTRA solo aparece en el real porque este no tiene separacion en el estandar, por lo que en este paso se reasigna el valor para que quede cargado a las horas extra, en caso contrario se hace la union de la naturaleza y el tipo de costo
ops_costos_mo_transf['TIPO COSTO NATURALEZA'] =  ops_costos_mo_transf.apply(lambda row:'COSTO MOD HORAS EXTRA REAL' if row['TIPO COSTO MOD REAL'] == 'COSTO MOD HORAS EXTRA' else row['TIPO COSTO'] + ' ' + row['NATURALEZA'] , axis=1)

# Se separan los datos en 2 grupos dado que en la posterior aplicacion en los teps se requiere para solo los datos del ESTANDAR aplicar un calculo diferente que separa el estandar en ESTANDAR y ESTANDAR APLICADO
ops_costos_mo_real = ops_costos_mo_transf[ops_costos_mo_transf['NATURALEZA'] == 'REAL']
ops_costos_mo_estandar = ops_costos_mo_transf[ops_costos_mo_transf['NATURALEZA'] == 'ESTANDAR']
agrupacion_cost_periodo_real = ops_costos_mo_real.groupby(['PERIODO', 'CODIGO CENTRO DE COSTO', 'TIPO COSTO NATURALEZA', 'TIPO COSTO', 'NATURALEZA'])['SALDO'].sum().reset_index()
agrupacion_cost_periodo_estandar = ops_costos_mo_estandar.groupby(['PERIODO', 'CODIGO CENTRO DE COSTO', 'TIPO COSTO NATURALEZA', 'TIPO COSTO', 'NATURALEZA'])['SALDO'].sum().reset_index()
union_horas_real = agrupacion_cost_periodo_real.merge(horas_reales_costos, left_on=['PERIODO', 'CODIGO CENTRO DE COSTO'], right_on= ['PERIODO DE CONTABILIZACION TEP', 'CENTRO DE COSTO'], how='inner')
union_horas_estandar = agrupacion_cost_periodo_estandar.merge(horas_reales_costos, left_on=['PERIODO', 'CODIGO CENTRO DE COSTO'], right_on= ['PERIODO DE CONTABILIZACION TEP', 'CENTRO DE COSTO'], how='inner')
# Calcular el costo por hora sobre la base de las horas reales totales del periodo para la determinacion de la tarifa de conversion
union_horas_real['COSTO REAL'] = union_horas_real['SALDO'] / union_horas_real['HORAS']
union_horas_estandar['COSTO ESTANDAR'] = union_horas_estandar['SALDO'] / union_horas_estandar['HORAS']

#                                    Tabla intermedia de validacion
backup_horas_base = union_horas_estandar.copy()
    
#Se guarda un listado de los datos unicos del tipo de costo para que en la union con los datos principales esta columna se pueda utilizar para hacer la iteracion del calculo por cada linea de los teps
columnas_reales = union_horas_real['TIPO COSTO NATURALEZA'].unique().tolist()
columnas_estandar = union_horas_estandar['TIPO COSTO NATURALEZA'].unique().tolist()
#Se segmentan las columnas que determinaran el pivoteo asi como su posterior union con el de teps y las cuales contienen sus va
union_horas_real = union_horas_real[['PERIODO DE CONTABILIZACION TEP', 'CENTRO DE COSTO', 'TIPO COSTO NATURALEZA', 'COSTO REAL']]
union_horas_real = union_horas_real.pivot(index=['PERIODO DE CONTABILIZACION TEP', 'CENTRO DE COSTO'], columns='TIPO COSTO NATURALEZA', values='COSTO REAL').reset_index()
union_horas_estandar = union_horas_estandar[['PERIODO DE CONTABILIZACION TEP', 'CENTRO DE COSTO', 'TIPO COSTO NATURALEZA', 'COSTO ESTANDAR']]
union_horas_estandar = union_horas_estandar.pivot(index=['PERIODO DE CONTABILIZACION TEP', 'CENTRO DE COSTO'], columns='TIPO COSTO NATURALEZA', values='COSTO ESTANDAR').reset_index()

# ----- 4. APLICACION DE CONTABILIZACION EN LOS CENTROS DE COSTO CON ESTANDAR Y CALCULO DE TARIFAS POR HORA ------ FIN

#-----------------------------------
                        # ----- 5. CALCULO DE KG NO CONFORME ------ INI
#-----------------------------------

#Regla: Las cantidades completadas deben salir de una combinacion entre los teps y los mvi. Termoformado y troquelado es la combinacion de no conforme y retal. Molido y descontaminacion no tienen. Los demas es solo el retal.

# Inicio Reglas de movimiento de inventario
# Segmentacion de los movimientos de inventario por reglas de negocio para la obtencion de los kg de mp consumida, los kg de retal, los kg de nc y los de torta

# Se agrega para hacer la aplicacion de las reglas de inventario segun este criterio
ops_movsinv['PROCESO PADRE'] = ops_movsinv['DOCUMENTO OP'].map(proceso_principal_dict)
# Se agrega para que la aplicacion se de en las operaciones principales que determinan el proceso padre
ops_movsinv['DESCRIPCION OPERACION'] = ops_movsinv['DOCUMENTO OP'].map(operacion_principal_dict)

ops_movsinv_kgconsumida_ext = ops_movsinv[ops_movsinv['PROCESO PADRE'].isin(['EXTRUSION LAMINA']) & ops_movsinv['CLASIFICACION DEL MOVIMIENTO'].isin(['ENTREGA REAL EXTRUSION']) & ops_movsinv['UNIDAD INVENTARIO'].isin(['KGS'])].copy()
# Se modifican los nombres de las columnas de forma que haya coincidencias entre las columnas de los tep y los mvi y se evite eliminar continuamente columnas y se puedan hacer cruces sin agregas excesivos sufijos(operacion conveniente para el calculo de nc)
ops_movsinv_kgconsumida_ext.rename(columns={'KG NETOS REAL BASE': 'KG MP EXT', 'REFERENCIA OP': 'REFERENCIA','FECHA CONTABILIZACION MVI': 'FECHA CONTABILIZACION TEP'}, inplace=True)
ops_movsinv_kgconsumida_ext['KG MP EXT'] = ops_movsinv_kgconsumida_ext['KG MP EXT'] * -1
ops_movsinv_kgconsumida_decmol = ops_movsinv[ops_movsinv['PROCESO PADRE'].isin(['DESCONTAMINACION', 'MOLIDO']) & ops_movsinv['TIPO MOVIMIENTO'].isin(['ENTREGAS'])].copy()
ops_movsinv_kgconsumida_decmol.rename(columns={'KG NETOS REAL BASE': 'KG MP DECMOL', 'REFERENCIA OP': 'REFERENCIA','FECHA CONTABILIZACION MVI': 'FECHA CONTABILIZACION TEP'}, inplace=True)
ops_movsinv_kgconsumida_otros = ops_movsinv[(~ops_movsinv['PROCESO PADRE'].isin(['DESCONTAMINACION', 'MOLIDO', 'ENTREGA REAL EXTRUSION'])) & ops_movsinv['CLASIFICACION DEL MOVIMIENTO'].isin(['ENTREGA REAL KG TERMO TROQ']) & ops_movsinv['TIPO DOC MVI'].isin(['MDC', 'MCC']) & ops_movsinv['UNIDAD INVENTARIO'].isin(['KGS'])].copy()
ops_movsinv_kgconsumida_otros.rename(columns={'KG NETOS REAL BASE': 'KG MP OTROS', 'REFERENCIA OP': 'REFERENCIA','FECHA CONTABILIZACION MVI': 'FECHA CONTABILIZACION TEP'}, inplace=True)
ops_movsinv_kgconsumida_otros['KG MP OTROS'] = ops_movsinv_kgconsumida_otros['KG MP OTROS'] * -1

ops_movsinv_retal_ncrt = ops_movsinv[ops_movsinv['PROCESO PADRE'].isin(['TERMOFORMADO', 'TROQUELADO']) & ops_movsinv['CLASIFICACION DEL MOVIMIENTO'].isin(['NO CONFORME', 'RETAL']) & ops_movsinv['TIPO DOC MVI'].isin(['MDC'])].copy()
ops_movsinv_retal_ncrt.rename(columns={'KG NETOS REAL BASE': 'RETAL NCRT', 'REFERENCIA OP': 'REFERENCIA','FECHA CONTABILIZACION MVI': 'FECHA CONTABILIZACION TEP'}, inplace=True)
ops_movsinv_retal_rt = ops_movsinv[(~ops_movsinv['PROCESO PADRE'].isin(['TERMOFORMADO', 'TROQUELADO', 'DESCONTAMINACION', 'MOLIDO'])) & ops_movsinv['CLASIFICACION DEL MOVIMIENTO'].isin(['RETAL']) & ops_movsinv['TIPO DOC MVI'].isin(['MDC'])].copy()
ops_movsinv_retal_rt.rename(columns={'KG NETOS REAL BASE': 'RETAL RT', 'REFERENCIA OP': 'REFERENCIA','FECHA CONTABILIZACION MVI': 'FECHA CONTABILIZACION TEP'}, inplace=True)

ops_movsinv_torta_ext = ops_movsinv[ops_movsinv['PROCESO PADRE'].isin(['EXTRUSION LAMINA']) & ops_movsinv['CLASIFICACION DEL MOVIMIENTO'].isin(['TORTA']) & ops_movsinv['TIPO DOC MVI'].isin(['MDC', 'MCC', 'MEP'])].copy()
ops_movsinv_torta_ext.rename(columns={'KG NETOS REAL BASE': 'KG TORTA BASE', 'REFERENCIA OP': 'REFERENCIA','FECHA CONTABILIZACION MVI': 'FECHA CONTABILIZACION TEP'}, inplace=True)

ops_movsinv_nc_termotroq = ops_movsinv[ops_movsinv['PROCESO PADRE'].isin(['TERMOFORMADO', 'TROQUELADO']) & ops_movsinv['CLASIFICACION DEL MOVIMIENTO'].isin(['NO CONFORME', 'RETAL']) & ops_movsinv['TIPO DOC MVI'].isin(['MDC'])].copy()
ops_movsinv_nc_termotroq.rename(columns={'KG NETOS REAL BASE': 'KG NC BASE TT', 'REFERENCIA OP': 'REFERENCIA','FECHA CONTABILIZACION MVI': 'FECHA CONTABILIZACION TEP'}, inplace=True)
ops_movsinv_nc_otros = ops_movsinv[(~ops_movsinv['PROCESO PADRE'].isin(['TERMOFORMADO', 'TROQUELADO', 'DESCONTAMINACION', 'MOLIDO'])) & ops_movsinv['CLASIFICACION DEL MOVIMIENTO'].isin(['NO CONFORME']) & ops_movsinv['TIPO DOC MVI'].isin(['MDC'])].copy()
ops_movsinv_nc_otros.rename(columns={'KG NETOS REAL BASE': 'KG NC BASE OTROS', 'REFERENCIA OP': 'REFERENCIA','FECHA CONTABILIZACION MVI': 'FECHA CONTABILIZACION TEP'}, inplace=True)
# Fin Reglas de movimientos de inventario

# Segmentacion de las columnas determinantes y agrupacion del valor de cada uno de los segmentos y sus formas de calculo
columnas_grupos = ['DOCUMENTO OP', 'REFERENCIA', 'FECHA CONTABILIZACION TEP', 'PROCESO PADRE', 'DESCRIPCION OPERACION']
ops_kgconsumida_ext = fx_group_and_sum(ops_movsinv_kgconsumida_ext, columnas_grupos, ['KG MP EXT'])
ops_kgconsumida_decmol = fx_group_and_sum(ops_movsinv_kgconsumida_decmol, columnas_grupos, ['KG MP DECMOL'])
ops_kgconsumida_otros = fx_group_and_sum(ops_movsinv_kgconsumida_otros, columnas_grupos, ['KG MP OTROS'])
ops_retal_ncrt = fx_group_and_sum(ops_movsinv_retal_ncrt, columnas_grupos, ['RETAL NCRT'])
ops_retal_rt = fx_group_and_sum(ops_movsinv_retal_rt, columnas_grupos, ['RETAL RT'])
ops_nc_termotroq = fx_group_and_sum(ops_movsinv_nc_termotroq, columnas_grupos, ['KG NC BASE TT'])
ops_nc_otros = fx_group_and_sum(ops_movsinv_nc_otros, columnas_grupos, ['KG NC BASE OTROS'])
# La torta no requiere agrupacion pues solo hay una regla de inventario
ops_torta_ext = fx_group_and_sum(ops_movsinv_torta_ext, columnas_grupos, ['KG TORTA BASE'])

# Consolidacion de todos los movimientos de kg consumidos sin importar la forma de calculo
ops_kgconsumida = [ops_kgconsumida_ext, ops_kgconsumida_decmol, ops_kgconsumida_otros]
ops_kgconsum = fx_merge_multiple(ops_kgconsumida, columnas_grupos)
ops_kgconsum['KG MP'] = ops_kgconsum[['KG MP EXT', 'KG MP DECMOL', 'KG MP OTROS']].sum(axis=1, min_count=1)
ops_kgconsum = ops_kgconsum.drop(['KG MP EXT', 'KG MP DECMOL', 'KG MP OTROS'], axis=1)

# Consolidacion de todos los movimientos de retal sin importar la forma de calculo
ops_kgretal = [ops_retal_ncrt, ops_retal_rt]
ops_kgret = fx_merge_multiple(ops_kgretal, columnas_grupos)
ops_kgret['KG RETAL'] = ops_kgret[['RETAL NCRT', 'RETAL RT']].sum(axis=1, min_count=1)
ops_kgret = ops_kgret.drop(['RETAL NCRT', 'RETAL RT'], axis=1)

# Consolidacion de todos los movimientos de no conforme sin importar la forma de calculo
ops_kgnoconforme = [ops_nc_termotroq, ops_nc_otros]
ops_kgnc = fx_merge_multiple(ops_kgnoconforme, columnas_grupos)
ops_kgnc['KG NC'] = ops_kgnc[['KG NC BASE TT', 'KG NC BASE OTROS']].sum(axis=1, min_count=1)
ops_kgnc = ops_kgnc.drop(['KG NC BASE TT', 'KG NC BASE OTROS'], axis=1)

#Segmentacion de movimientos de operacion (Aquellos que no son procesos especificos listados)
operacion_ops_est = ops_teps[~ops_teps['PROCESO ESPECIFICO'].isin(['ALISTAMIENTO CORTA DURACION', 'ALISTAMIENTO INTEROPERACIONAL', 'ALISTAMIENTO'])].copy()
#Segmentacion de operaciones principales que determinan la cantidad transformada
cantidad_transformada_opPrinc = operacion_ops_est[operacion_ops_est['PROCESO'] == operacion_ops_est['PROCESO PADRE']]
cantidad_transformada = cantidad_transformada_opPrinc.groupby(['DOCUMENTO OP', 'PERIODO DE CONTABILIZACION TEP'])['CANTIDAD COMPLETADA OP TEPS'].sum().reset_index()

#Agrupacion por op de datos globales
columnas_op = ['DOCUMENTO OP', 'PROCESO PADRE']
#Cantidades trasnformadas (Solo de las operaciones principales)
cantidades_transformadas_op = fx_group_and_sum(cantidad_transformada_opPrinc, columnas_op, ['CANTIDAD COMPLETADA OP TEPS'])
# Cantidades rechazadas pueden provenir de cualquier operacion
cantidades_rechazadas_op = fx_group_and_sum(ops_teps, columnas_op, ['CANTIDAD RECHAZADA OP TEPS'])
# Kg consumidos del inventario agrupado sin formas de calculo
cantidad_kgconsumida_op = fx_group_and_sum(ops_kgconsum, columnas_op, ['KG MP'])
# Kg consumidos del inventario agrupado sin formas de calculo
cantidad_kgnc_op = fx_group_and_sum(ops_kgnc, columnas_op, ['KG NC'])

# Union de todos los datos requeridos para la obtencion del no conforme y modificacion del nombre para mejor identificacion

ops_totales = [cantidades_transformadas_op, cantidades_rechazadas_op, cantidad_kgconsumida_op, cantidad_kgnc_op]
ops_tot = fx_merge_multiple(ops_totales, columnas_op)
ops_tot.rename(columns={'CANTIDAD COMPLETADA OP TEPS': 'Q TRANSF OP', 'CANTIDAD RECHAZADA OP TEPS': 'Q NC OP', 'KG MP':'KG MP OP', 'KG NC':'KG NC OP'}, inplace=True)


columnas_nc_union = ['DOCUMENTO OP', 'FECHA CONTABILIZACION TEP', 'PROCESO PADRE', 'REFERENCIA', 'DESCRIPCION OPERACION']
# Para todas los movimientos de inventario reportados con no conforme que tienen datos en el mismo dia y a la misma operacion (Operacion Principal) se les aplica un factor de conversion simple segun las horas reales reportadas en cada grupo
ops_incluidas_nc = ops_kgnc[columnas_nc_union].merge(ops_teps, on=columnas_nc_union,  how='inner')
ops_incluidas_nc['TOTAL CANTIDAD'] = ops_incluidas_nc.groupby(columnas_nc_union)['HORAS'].transform('sum')
ops_incluidas_nc['FACTOR DE APLICACION'] = ops_incluidas_nc['HORAS'] / ops_incluidas_nc['TOTAL CANTIDAD']

# No se obtiene factor de aplicacion para todos los movimientos que estan en los teps y no tienen coincidencia en los movimientos de inventarios
ops_restantes_nc =  ops_teps.merge(ops_kgnc[columnas_nc_union], on=columnas_nc_union,  how='left' ,indicator=True)
ops_restantes_nc = ops_restantes_nc[ops_restantes_nc['_merge'] == 'left_only']

# Para aquellos valores que no tienen coincidencia en los teps pero si tienen movimiento de inventario, se crean nuevas lineas de movimientos segun ciertas reglas.
ops_faltantes_nc = ops_kgnc[columnas_nc_union].merge(ops_teps[columnas_nc_union], on=columnas_nc_union,  how='left', indicator=True)
ops_faltantes_nc = ops_faltantes_nc[ops_faltantes_nc['_merge'] == 'left_only']
ops_faltantes_nc = ops_faltantes_nc.drop('_merge', axis=1)
# Se une con los datos originales pero sin el concepto de fecha de forma que se pueda obtener el movimiento al cual se le va a imputar el dato nuevo y del cual se heredaran los datos.
ops_faltantes_nc= ops_faltantes_nc.merge(ops_teps, on=['DOCUMENTO OP', 'REFERENCIA', 'PROCESO PADRE', 'DESCRIPCION OPERACION'],  how='left', suffixes=('', '_GRUPO'))
# Se obtiene con el objetivo de identificar si la op tiene reporte de cantidades en los tiempos. Luego se aplica la regla.
ops_faltantes_nc['RECHAZOS'] = ops_faltantes_nc.groupby('DOCUMENTO OP')['CANTIDAD RECHAZADA OP TEPS'].transform('sum')
# La columna TEP de fecha contiene la fecha del movimiento de inventario (Modificado en las reglas de inventario) y al unirlo con la de teps se puede identificar la diferencia de fechas respecto a los tiempos de forma que el dato con menor fecha pueda servir como base del nuevo dato. 
ops_faltantes_nc['DIFFECHA'] = (ops_faltantes_nc['FECHA CONTABILIZACION TEP_GRUPO'] - ops_faltantes_nc['FECHA CONTABILIZACION TEP']).dt.days
# REVISAR UTILIDAD Regla principalmente aplicable para los grupos de ordenes, cuando una orden tiene cantidad rechazada en 0.
ops_faltantes_nc['DIFFECHA'] = ops_faltantes_nc.apply(lambda row: row['DIFFECHA']  if row['CANTIDAD RECHAZADA OP TEPS']>=0 else row['DIFFECHA']*1000, axis=1)
ops_faltantes_nc['DIFFECHA'] = ops_faltantes_nc['DIFFECHA'].apply(lambda x: abs(x) if x < 0 else x * 100)
ops_faltantes_nc['MINDIF'] = ops_faltantes_nc.groupby(['DOCUMENTO OP', 'REFERENCIA', 'PROCESO PADRE','FECHA CONTABILIZACION TEP', 'DESCRIPCION OPERACION'])['DIFFECHA'].transform('min')
ops_faltantes_nc = ops_faltantes_nc[ops_faltantes_nc['MINDIF'] == ops_faltantes_nc['DIFFECHA']]
ops_faltantes_nc['ID INTERNO MOV SIESA'] = ops_faltantes_nc.apply(lambda row: row['ID INTERNO MOV SIESA'] + '_1_' + row['FECHA CONTABILIZACION TEP'].strftime('%Y%m%d'), axis=1)
ops_faltantes_nc['TOTAL CANTIDAD'] = ops_faltantes_nc.groupby(['DOCUMENTO OP', 'FECHA CONTABILIZACION TEP','PROCESO PADRE', 'DESCRIPCION OPERACION'])['HORAS'].transform('sum')
ops_faltantes_nc['FACTOR DE APLICACION'] = ops_faltantes_nc['HORAS'] / ops_faltantes_nc['TOTAL CANTIDAD']

ops_distribuciones_nc = pd.concat([ops_faltantes_nc, ops_incluidas_nc, ops_restantes_nc], ignore_index=True)
ops_distribuciones_nc['Q NC OPS INCLUIDAS'] = ops_distribuciones_nc.groupby(['DOCUMENTO OP', 'FECHA CONTABILIZACION TEP'])['CANTIDAD RECHAZADA OP TEPS'].transform('sum')
ops_distribuciones_nc = ops_distribuciones_nc.merge(ops_kgnc[['DOCUMENTO OP', 'FECHA CONTABILIZACION TEP', 'PROCESO PADRE', 'REFERENCIA', 'KG NC']], on = ['DOCUMENTO OP', 'FECHA CONTABILIZACION TEP', 'PROCESO PADRE', 'REFERENCIA'], how='outer')
ops_distribuciones_nc = ops_distribuciones_nc.merge(ops_tot, on = ['DOCUMENTO OP', 'PROCESO PADRE'], how='left')
ops_distribuciones_nc[['Q TRANSF OP', 'Q NC OP', 'KG MP OP', 'KG NC OP', 'CANTIDAD RECHAZADA OP TEPS', 'KG NC']] = ops_distribuciones_nc[['Q TRANSF OP', 'Q NC OP', 'KG MP OP', 'KG NC OP', 'CANTIDAD RECHAZADA OP TEPS', 'KG NC']].astype(float).fillna(0)

def calcular_kg_nc_fecha(row):
    if row['PROCESO PADRE'] in ['TERMOFORMADO', 'TROQUELADO']:
        if row['Q TRANSF OP'] > 0:
            return ((row['KG MP OP'] - row['KG NC OP']) / row['Q TRANSF OP']) * row['CANTIDAD RECHAZADA OP TEPS']
        else:
            return 0
    else:
        if row['KG NC']>0 and row['Q NC OPS INCLUIDAS']==0:
            return row['KG NC'] * row['FACTOR DE APLICACION']
        elif row['KG NC']>0 and row['Q NC OPS INCLUIDAS']>0:
            return (row['CANTIDAD RECHAZADA OP TEPS'] / row['Q NC OPS INCLUIDAS']) * row['KG NC']
def fx_factor_aplicado(row):
    if row['PROCESO PADRE'] in ['TERMOFORMADO', 'TROQUELADO']:
        if row['Q TRANSF OP'] > 0 and row['Q NC OP']>0:
            return ((row['KG MP OP'] - row['KG NC OP']) / row['Q TRANSF OP']) * (row['CANTIDAD RECHAZADA OP TEPS'] / row['Q NC OP'])
        else:
            return 0
    else:
        if row['KG NC']>0 and row['Q NC OPS INCLUIDAS']==0:
            return 1
        elif row['KG NC']>0 and row['Q NC OPS INCLUIDAS']>0:
            return (row['CANTIDAD RECHAZADA OP TEPS'] / row['Q NC OPS INCLUIDAS'])

ops_distribuciones_nc['KG NC DISTRIBUCION'] = ops_distribuciones_nc.apply(calcular_kg_nc_fecha, axis=1) 
#ops_distribuciones_nc['FACTOR DE CONVERSION FALLAS'] = ops_distribuciones_nc.apply(fx_factor_aplicado, axis=1)
#ops_fallas = ops_distribuciones_nc.merge(ops_entidades_fallas, on=['DOCUMENTO OP'], how='left')


union_tablas = [ops_retal_ncrt, ops_retal_rt, ops_kgconsumida_ext, ops_kgconsumida_decmol, ops_kgconsumida_otros, ops_torta_ext]
ops_cantidades = union_tablas[0]
for df in union_tablas[1:]:
    ops_cantidades = pd.merge(ops_cantidades, df, on = ['DOCUMENTO OP', 'REFERENCIA', 'FECHA CONTABILIZACION TEP', 'PROCESO PADRE', 'DESCRIPCION OPERACION'], how='outer')

ops_faltantes = ops_cantidades.merge(ops_teps[['DOCUMENTO OP', 'REFERENCIA', 'FECHA CONTABILIZACION TEP', 'PROCESO PADRE', 'DESCRIPCION OPERACION']], on=['DOCUMENTO OP', 'REFERENCIA', 'FECHA CONTABILIZACION TEP', 'PROCESO PADRE', 'DESCRIPCION OPERACION'],  how='left', indicator=True)
ops_faltantes = ops_faltantes[ops_faltantes['_merge'] == 'left_only']
ops_faltantes = ops_faltantes.drop('_merge', axis=1)
ops_faltantes= ops_faltantes.merge(ops_teps, on=['DOCUMENTO OP', 'REFERENCIA', 'PROCESO PADRE', 'DESCRIPCION OPERACION'],  how='left', suffixes=('', '_GRUPO'))
ops_faltantes['DIFFECHA'] = (ops_faltantes['FECHA CONTABILIZACION TEP_GRUPO'] - ops_faltantes['FECHA CONTABILIZACION TEP']).dt.days
ops_faltantes['DIFFECHA'] = ops_faltantes['DIFFECHA'].apply(lambda x: abs(x) if x < 0 else x * 100)
ops_faltantes['MINDIF'] = ops_faltantes.groupby(['DOCUMENTO OP', 'REFERENCIA','FECHA CONTABILIZACION TEP', 'PROCESO PADRE', 'DESCRIPCION OPERACION'])['DIFFECHA'].transform('min')
ops_distribuciones_prueba = ops_faltantes.copy()
ops_faltantes = ops_faltantes[ops_faltantes['MINDIF'] == ops_faltantes['DIFFECHA']]
ops_faltantes['ID INTERNO MOV SIESA'] = ops_faltantes.apply(lambda row: row['ID INTERNO MOV SIESA'] + '_0_' + row['FECHA CONTABILIZACION TEP'].strftime('%Y%m%d'), axis=1)

ops_incluidas = ops_cantidades.merge(ops_teps, on=['DOCUMENTO OP', 'REFERENCIA', 'FECHA CONTABILIZACION TEP', 'PROCESO PADRE', 'DESCRIPCION OPERACION'],  how='inner')

ops_distribuciones = pd.concat([ops_faltantes, ops_incluidas], ignore_index=True)
ops_distribuciones['TOTAL HORAS'] = ops_distribuciones.groupby(['DOCUMENTO OP', 'FECHA CONTABILIZACION TEP'])['HORAS'].transform('sum')
ops_distribuciones['KG DISTRIBUCION RETAL RT'] = (ops_distribuciones['HORAS'] / ops_distribuciones['TOTAL HORAS']) * ops_distribuciones['RETAL RT']
ops_distribuciones['KG DISTRIBUCION RETAL NCRT'] = (ops_distribuciones['HORAS'] / ops_distribuciones['TOTAL HORAS']) * ops_distribuciones['RETAL NCRT']
ops_distribuciones['KG DISTRIBUCION CONS EXT'] = (ops_distribuciones['HORAS'] / ops_distribuciones['TOTAL HORAS']) * ops_distribuciones['KG MP EXT']
ops_distribuciones['KG DISTRIBUCION CONS DECMOL'] = (ops_distribuciones['HORAS'] / ops_distribuciones['TOTAL HORAS']) * ops_distribuciones['KG MP DECMOL']
ops_distribuciones['KG DISTRIBUCION CONS OTROS'] = (ops_distribuciones['HORAS'] / ops_distribuciones['TOTAL HORAS']) * ops_distribuciones['KG MP OTROS']
ops_distribuciones['KG TORTA'] = (ops_distribuciones['HORAS'] / ops_distribuciones['TOTAL HORAS']) * ops_distribuciones['KG TORTA BASE']

#Limpieza de columnas distribuciones de nc
columnas_excluir_nc = ['CANTIDAD COMPLETADA OP TEPS' ,'CANTIDAD RETAL OP TEPS' ,'CANTIDAD RECHAZADA OP TEPS' ,'FECHA CONTABILIZACION TEP_GRUPO' ,'HORAS' ,'HORAS ESTANDAR BASE' ,'CANTIDAD COMPLETADA' ,'CANTIDAD BASE' ,'ABS HORAS BASE' ,'DIFFECHA' ,'MINDIF' ,'TOTAL HORAS', 'RECHAZOS', 'TOTAL CANTIDAD', 'FACTOR DE APLICACION', 'Q NC OPS INCLUIDAS', '_merge', 'KG NC', 'Q TRANSF OP', 'Q NC OP', 'KG MP OP', 'KG NC OP']
columnas_totales_nc = list(ops_teps.columns.values)
columnas_incluidas_nc = list(set(ops_teps) - set(columnas_excluir_nc))
columnas_nc = list(ops_distribuciones_nc.columns.values)
columnas_inc_nc = list(set(ops_distribuciones_nc) - set(columnas_excluir_nc))
columnas_exc_nc = list(set(columnas_nc) - set(columnas_inc_nc))

#Limpieza de columnas ditribuciones otras cantidades
columnas_excluir = ['CANTIDAD COMPLETADA OP TEPS' ,'CANTIDAD RETAL OP TEPS' ,'CANTIDAD RECHAZADA OP TEPS' ,'FECHA CONTABILIZACION TEP_GRUPO'  ,'HORAS' ,'HORAS ESTANDAR BASE' ,'CANTIDAD COMPLETADA' ,'CANTIDAD BASE' ,'ABS HORAS BASE' ,'DIFFECHA' ,'MINDIF' ,'TOTAL HORAS', 'KG NC DISTRIBUCION']
columnas_totales = list(ops_teps.columns.values)
columnas_incluidas = list(set(columnas_totales) - set(columnas_excluir))
columnas = list(ops_distribuciones.columns.values)
columnas_inc = list(set(ops_distribuciones) - set(columnas_excluir))
columnas_exc = list(set(columnas) - set(columnas_inc))

ops_distribuciones = ops_distribuciones.drop(columnas_exc, axis=1)
ops_distribuciones_nc = ops_distribuciones_nc.drop(columnas_exc_nc, axis=1)
ops_teps = ops_teps.merge(ops_distribuciones_nc, on = columnas_incluidas_nc, how='outer')
ops_teps = ops_teps.merge(ops_distribuciones, on = columnas_incluidas, how='outer')




# Filtrar datos por proceso específico
alistamiento_ops_cd = ops_teps[ops_teps['PROCESO ESPECIFICO'].isin(['ALISTAMIENTO CORTA DURACION'])].copy()
alistamiento_ops_interop = ops_teps[ops_teps['PROCESO ESPECIFICO'].isin(['ALISTAMIENTO INTEROPERACIONAL'])].copy()
estandar_unitario = operacion_ops_est.groupby(['DOCUMENTO OP', 'PERIODO DE CONTABILIZACION TEP', 'DESCRIPCION OPERACION'])[['HORAS ESTANDAR BASE', 'CANTIDAD BASE']].mean().reset_index()
estandar_unitario['TIEMPO UNITARIO'] = estandar_unitario['HORAS ESTANDAR BASE'] / estandar_unitario['CANTIDAD BASE']

operacion_rechazada = ops_teps.groupby(['DOCUMENTO OP', 'PERIODO DE CONTABILIZACION TEP'])['CANTIDAD RECHAZADA OP TEPS'].sum().reset_index()
operacion_rechazada.rename(columns={'CANTIDAD RECHAZADA OP TEPS': 'RECHAZADA'}, inplace=True)
operacion_ops_est = operacion_ops_est.merge(operacion_rechazada, on =['DOCUMENTO OP', 'PERIODO DE CONTABILIZACION TEP'], how='left')
ops_teps_extrusion = ops_teps[ops_teps['PROCESO PADRE'] == 'EXTRUSION LAMINA'].copy() 
operacion_retal = ops_teps_extrusion.groupby(['DOCUMENTO OP', 'PERIODO DE CONTABILIZACION TEP'])['CANTIDAD RETAL OP TEPS'].sum().reset_index()
operacion_retal.rename(columns={'CANTIDAD RETAL OP TEPS': 'RETAL'}, inplace=True)
operacion_ops_est = operacion_ops_est.merge(operacion_retal, on =['DOCUMENTO OP', 'PERIODO DE CONTABILIZACION TEP'], how='left')
operacion_ops_est['RETAL'] = operacion_ops_est['RETAL'].fillna(0)

operacion_ops_est = operacion_ops_est.merge(cantidad_transformada, on=['DOCUMENTO OP', 'PERIODO DE CONTABILIZACION TEP'], how = 'left', suffixes=('','_RESUMEN'))
operacion_ops_est = operacion_ops_est.merge(estandar_unitario, on = ['DOCUMENTO OP', 'PERIODO DE CONTABILIZACION TEP', 'DESCRIPCION OPERACION'], how = 'left', suffixes=('','_RESUMEN'))
operacion_ops_est['CANTIDAD COMPLETADA TEPS'] = operacion_ops_est['CANTIDAD COMPLETADA OP TEPS_RESUMEN'] - operacion_ops_est['RECHAZADA'] - operacion_ops_est['RETAL']
operacion_ops_est['HORAS ESTANDAR TOTALES']  = operacion_ops_est['TIEMPO UNITARIO'] * operacion_ops_est['CANTIDAD COMPLETADA TEPS']
operacion_ops_est['HORAS REALES GRUPO'] = operacion_ops_est.groupby(['DOCUMENTO OP', 'PERIODO DE CONTABILIZACION TEP', 'DESCRIPCION OPERACION'])['HORAS'].transform('sum')
operacion_ops_est['H_EST_OPER'] = (operacion_ops_est['HORAS'] / operacion_ops_est['HORAS REALES GRUPO']) * operacion_ops_est['HORAS ESTANDAR TOTALES']
operacion_ops_est['HORAS ESTANDAR'] = operacion_ops_est['H_EST_OPER']
cantidad_completada_resumen = operacion_ops_est.groupby(['DOCUMENTO OP', 'PERIODO DE CONTABILIZACION TEP'])['CANTIDAD COMPLETADA TEPS'].mean().reset_index()

# Procesar alistamiento montaje
alistamiento_montaje = ops_teps[ops_teps['DESCOP 12'].isin(['MONTAJE Y DE'])].copy()
alistamiento_montaje['MINFECHA'] = alistamiento_montaje.groupby('DOCUMENTO AG - OP MONTAJE')['FECHA CONTABILIZACION TEP'].transform('min')
alistamiento_montaje['HORAS REALES'] = alistamiento_montaje.groupby(['DOCUMENTO AG - OP MONTAJE', 'DOCUMENTO OP', 'DOCUMENTO OP MONTAJE', 'FECHA CONTABILIZACION TEP', 'DESCOP 12'])['HORAS'].transform('sum')
alistamiento_montaje['HORAS REALES GRUPO'] = alistamiento_montaje.groupby(['DOCUMENTO AG - OP MONTAJE', 'DESCOP 12'])['HORAS'].transform('sum')

alistamiento_montaje_grupo = alistamiento_montaje.groupby(['DOCUMENTO AG - OP MONTAJE', 'DOCUMENTO OP', 'DOCUMENTO OP MONTAJE', 'FECHA CONTABILIZACION TEP', 'DESCOP 12'])[['HORAS REALES', 'MINFECHA', 'ABS HORAS BASE']].mean().reset_index()
alistamiento_montaje_grupo['DIFFECHA'] = (alistamiento_montaje_grupo['FECHA CONTABILIZACION TEP'] - alistamiento_montaje_grupo['MINFECHA']).dt.days

alistamiento_montaje_grupo['MINORDEN'] = alistamiento_montaje_grupo[alistamiento_montaje_grupo['DIFFECHA']==0].groupby(['DOCUMENTO AG - OP MONTAJE','FECHA CONTABILIZACION TEP'])['DOCUMENTO OP'].transform('min')

alistamiento_montaje_grupo['HORAS BASE'] = np.where(
    (alistamiento_montaje_grupo['DOCUMENTO OP'] == alistamiento_montaje_grupo['MINORDEN']) & (alistamiento_montaje_grupo['DIFFECHA'] == 0),
    alistamiento_montaje_grupo['ABS HORAS BASE'],
    np.where((alistamiento_montaje_grupo['DIFFECHA'] > 2) & (alistamiento_montaje_grupo['HORAS REALES'] > alistamiento_montaje_grupo['ABS HORAS BASE'] * 0.9),
             alistamiento_montaje_grupo['ABS HORAS BASE'],
             None)
)

alistamiento_montaje_grupo = alistamiento_montaje_grupo[alistamiento_montaje_grupo['HORAS BASE'] > 0]
alistamiento_montaje_res = alistamiento_montaje_grupo.groupby(['DOCUMENTO AG - OP MONTAJE', 'DESCOP 12']).agg(H_EST_ALIST_MON_P=('HORAS BASE', 'sum'), cantidad=('DOCUMENTO OP', 'count'))

alistamiento_montaje = alistamiento_montaje.merge(alistamiento_montaje_res, on='DOCUMENTO AG - OP MONTAJE', how='left')
alistamiento_montaje['H_EST_ALIST_MON'] = alistamiento_montaje['H_EST_ALIST_MON_P'] * (alistamiento_montaje['HORAS'] / alistamiento_montaje['HORAS REALES GRUPO'])
alistamiento_montaje['HORAS ESTANDAR'] = alistamiento_montaje['H_EST_ALIST_MON']

# Procesar alistamiento puesta a punto
alistamiento_puestapunto = ops_teps[ops_teps['DESCOP 12'].isin(['PUESTA A PUN'])].copy()
alistamiento_puestapunto['HORAS REALES GRUPO'] = alistamiento_puestapunto.groupby(['DOCUMENTO AG - OP MONTAJE', 'DESCOP 12'])['HORAS'].transform('sum')
alistamiento_puestapunto_grupo = alistamiento_puestapunto.groupby(['DOCUMENTO AG - OP MONTAJE', 'DESCOP 12'])['ABS HORAS BASE'].mean().reset_index()
alistamiento_puestapunto_consolidado = alistamiento_puestapunto_grupo.merge(alistamiento_montaje_res, on='DOCUMENTO AG - OP MONTAJE', how='left')
alistamiento_puestapunto_consolidado['H_EST_ALIST_PP_P'] = alistamiento_puestapunto_consolidado['ABS HORAS BASE'] * alistamiento_puestapunto_consolidado['cantidad']
alistamiento_puestapunto = alistamiento_puestapunto.merge(alistamiento_puestapunto_consolidado, on='DOCUMENTO AG - OP MONTAJE', how='left')
alistamiento_puestapunto['H_EST_ALIST_PP'] = alistamiento_puestapunto['H_EST_ALIST_PP_P'] * (alistamiento_puestapunto['HORAS'] / alistamiento_puestapunto['HORAS REALES GRUPO'])
alistamiento_puestapunto['HORAS ESTANDAR'] = alistamiento_puestapunto['H_EST_ALIST_PP']

# Promedio por grupo
mean_op_per_group_cd = alistamiento_ops_cd.groupby(['DOCUMENTO AG - OP MONTAJE', 'DESCRIPCION OPERACION', 'PROCESO ESPECIFICO'])['ABS HORAS BASE'].mean().reset_index()
mean_op_per_group_io = alistamiento_ops_interop.groupby(['DOCUMENTO AG - OP MONTAJE', 'DESCRIPCION OPERACION', 'PROCESO ESPECIFICO'])[['ABS HORAS BASE', 'CANTIDAD BASE']].mean().reset_index()

alistamiento_ops_cd = alistamiento_ops_cd.merge(mean_op_per_group_cd, on=['DOCUMENTO AG - OP MONTAJE', 'DESCRIPCION OPERACION', 'PROCESO ESPECIFICO'], how='left', suffixes=('', '_GROUP'))
alistamiento_ops_interop = alistamiento_ops_interop.merge(mean_op_per_group_io, on=['DOCUMENTO AG - OP MONTAJE', 'DESCRIPCION OPERACION', 'PROCESO ESPECIFICO'], how='left', suffixes=('', '_GROUP'))

alistamiento_ops_cd['TOTAL HORAS'] = alistamiento_ops_cd.groupby(['DOCUMENTO AG - OP MONTAJE', 'DESCRIPCION OPERACION'])['HORAS'].transform('sum')
alistamiento_ops_interop['TOTAL HORAS'] = alistamiento_ops_interop.groupby(['DOCUMENTO OP','PERIODO DE CONTABILIZACION TEP', 'DESCRIPCION OPERACION'])['HORAS'].transform('sum')
alistamiento_ops_interop = alistamiento_ops_interop.merge(cantidad_completada_resumen, on=['DOCUMENTO OP', 'PERIODO DE CONTABILIZACION TEP'], how='left')
alistamiento_ops_cd = alistamiento_ops_cd[alistamiento_ops_cd['TOTAL HORAS'] >= alistamiento_ops_cd['ABS HORAS BASE_GROUP'] * 0.5]

alistamiento_ops_cd['H_EST_ALIST_CD'] = alistamiento_ops_cd['ABS HORAS BASE_GROUP'] * (alistamiento_ops_cd['HORAS'] / alistamiento_ops_cd['TOTAL HORAS'])
alistamiento_ops_cd['HORAS ESTANDAR'] = alistamiento_ops_cd['H_EST_ALIST_CD']
alistamiento_ops_interop['H_EST_ALIST_IO'] = ((alistamiento_ops_interop['ABS HORAS BASE_GROUP'] * alistamiento_ops_interop['CANTIDAD COMPLETADA TEPS']) / alistamiento_ops_interop['CANTIDAD BASE_GROUP']) * (alistamiento_ops_interop['HORAS'] / alistamiento_ops_interop['TOTAL HORAS'])
alistamiento_ops_interop['HORAS ESTANDAR'] = alistamiento_ops_interop['H_EST_ALIST_IO'] 

# Fusionar resultados finales
columnas_horas_estandar = ['ID INTERNO MOV SIESA', 'HORAS ESTANDAR']
calculos_horas_estandar = [alistamiento_montaje, alistamiento_puestapunto, alistamiento_ops_cd, alistamiento_ops_interop, operacion_ops_est]
calculos_horas_estandar = [calculo[columnas_horas_estandar] for calculo in calculos_horas_estandar]
calculos_horas_estandar = fx_merge_multiple(calculos_horas_estandar, columnas_horas_estandar)

ops_teps = ops_teps.merge(calculos_horas_estandar, on='ID INTERNO MOV SIESA', how='left')

ops_teps['KG RETAL'] = ops_teps[['KG DISTRIBUCION RETAL RT', 'KG DISTRIBUCION RETAL NCRT']].sum(axis=1, min_count=1)
#ops_teps['HORAS ESTANDAR'] = ops_teps[['H_EST_ALIST_MON', 'H_EST_ALIST_PP', 'H_EST_ALIST_CD', 'H_EST_ALIST_IO', 'H_EST_OPER']].sum(axis=1, min_count=1)
ops_teps['KG MP CONSUMIDA'] = ops_teps[['KG DISTRIBUCION CONS EXT', 'KG DISTRIBUCION CONS DECMOL', 'KG DISTRIBUCION CONS OTROS']].sum(axis=1, min_count=1)
ops_teps['PERIODO DE CONTABILIZACION TEP'] = ops_teps.apply(lambda row: row['FECHA CONTABILIZACION TEP'].strftime('%Y%m'), axis=1)

ops_teps = ops_teps.merge(union_horas_real, on =['PERIODO DE CONTABILIZACION TEP', 'CENTRO DE COSTO'], how='left') 
ops_teps = ops_teps.merge(union_horas_estandar, on =['PERIODO DE CONTABILIZACION TEP', 'CENTRO DE COSTO'], how='left')


# ------ CALCULO DE CONCEPTOS DE PARO
ops_resultado_tiempos = ops_teps.copy()
ops_resultado_tiempos['CODIGO CENTRO DE TRABAJO'] = ops_resultado_tiempos['CODIGO CENTRO DE TRABAJO'].str[:2]
ops_resultado_tiempos = ops_resultado_tiempos[ops_resultado_tiempos['HORAS ESTANDAR BASE'].notnull()]
ops_resultado_tiempos['HORAS ESTANDAR'] =ops_resultado_tiempos.apply(lambda row: 0 if pd.isnull(row['HORAS ESTANDAR']) and pd.notnull(row['HORAS']) else row['HORAS ESTANDAR'], axis=1)
ops_resultado_tiempos_entidades = ops_resultado_tiempos.merge(ops_entidades_paros, on = ['DOCUMENTO OP'], how='inner')

def filtrado_datos(row):
    if row['TIPO DE INCLUSION'] == 'INCLUIDOS':
        if row[row['COLUMNA 1 REFERENCIA']] == row['DATO 1'] and row[row['COLUMNA 2 REFERENCIA']] in (row['DATO 2']):
            row['VALIDACION'] = "IN"
        else:
            # None
            if pd.notnull(row['COLUMNA 3 REFERENCIA']) and row[row['COLUMNA 3 REFERENCIA']] in (row['DATO 3']) :
                row['VALIDACION'] = "CO"
    elif row['TIPO DE INCLUSION'] == 'EXCLUIDOS':
        if row[row['COLUMNA 1 REFERENCIA']] == row['DATO 1'] and row[row['COLUMNA 2 REFERENCIA']] not in (row['DATO 2']):
            row['VALIDACION'] = "EX"
        else:
            None
            if pd.notnull(row['COLUMNA 3 REFERENCIA']) and row[row['COLUMNA 3 REFERENCIA']] in (row['DATO 3']) :
                row['VALIDACION'] = "CO"
    elif row['TIPO DE INCLUSION'] == 'TODOS':
        if row[row['COLUMNA 1 REFERENCIA']] == row['DATO 1']:
            row['VALIDACION'] = "TO"
    return row
ops_resultado_tiempos_entidades = ops_resultado_tiempos_entidades.apply(filtrado_datos, axis=1)
ops_resultado_tiempos_entidades = ops_resultado_tiempos_entidades[ops_resultado_tiempos_entidades['VALIDACION'].notnull()]

ops_resultado_tiempos_entidades['TOTAL HORAS'] = ops_resultado_tiempos_entidades.groupby(['DOCUMENTO OP', 'CODIGO PARO'])['HORAS'].transform('sum')
ops_resultado_tiempos_entidades['HORAS PARO DISTRIBUCION'] = (ops_resultado_tiempos_entidades['HORAS'] / ops_resultado_tiempos_entidades['TOTAL HORAS']) * ops_resultado_tiempos_entidades['HORAS PARO']

ops_movimientos_paros = ops_resultado_tiempos_entidades['ID INTERNO MOV SIESA'].unique()
ops_resultado_tiempos_noparo = ops_resultado_tiempos[~ops_resultado_tiempos['ID INTERNO MOV SIESA'].isin(ops_movimientos_paros)]

ops_resultado_tiempos_totales = pd.concat([ops_resultado_tiempos_entidades, ops_resultado_tiempos_noparo], ignore_index=True)

# Crear una lista para almacenar los nuevos registros
conceptos_paros_detalle = []

# Agrupar por ID INTERNO MOV SIESA
grouped = ops_resultado_tiempos_totales.groupby('ID INTERNO MOV SIESA')

for name, group in grouped:
    # print(group)
    costos_reales = [group[costo].mean() for costo in columnas_reales]
    horas = group['HORAS'].iloc[0]
    horas_estandar = group['HORAS ESTANDAR'].iloc[0]
    horas_distribucion_paro = group['HORAS PARO DISTRIBUCION'].sum()
    # print(horas ,  horas_estandar , horas_distribucion_paro)

    # 000-HORAS ESTANDAR
    conceptos_paros_detalle.append([name, "ESTANDAR", '000-HORAS ESTANDAR', horas_estandar]+costos_reales)

    # Conceptos de PARO
    for _, row in group.iterrows():
        if not pd.isna(row['PARO']):
            conceptos_paros_detalle.append([name, group['GRUPO DE PAROS'].iloc[0], row['PARO'], row['HORAS PARO DISTRIBUCION']]+costos_reales)
    
    if horas < horas_estandar:
        # 998-INCREMENTO DE EFICIENCIA O ESTANDAR INCORRECTO
        conceptos_paros_detalle.append([name, "INCREMENTO DE EFICIENCIA",'998-INCREMENTO DE EFICIENCIA O ESTANDAR INCORRECTO', horas - horas_estandar]+costos_reales)
        # 999-AJUSTE REGISTRO DE PAROS SUP A LA DIFERENCIA EST VS REAL
        conceptos_paros_detalle.append([name, "AJUSTES",'997-AJUSTE REGISTRO DE PAROS SUP A LA DIFERENCIA EST VS REAL', -horas_distribucion_paro]+costos_reales)
    else:
        diferencia = horas - horas_estandar
        if diferencia < horas_distribucion_paro:
            # 999-AJUSTE REGISTRO DE PAROS SUP A LA DIFERENCIA EST VS REAL
            conceptos_paros_detalle.append([name, "AJUSTES",'997-AJUSTE REGISTRO DE PAROS SUP A LA DIFERENCIA EST VS REAL', - horas_distribucion_paro]+costos_reales)
            conceptos_paros_detalle.append([name, "INCREMENTO DE EFICIENCIA", '998-INCREMENTO DE EFICIENCIA O ESTANDAR INCORRECTO', diferencia]+costos_reales)
             
        else:
            # 997-INEFICIENCIA NO JUSTIFICADA
            conceptos_paros_detalle.append([name, "INEFICIENCIA NO JUSTIFICADA",'999-INEFICIENCIA NO JUSTIFICADA', diferencia - horas_distribucion_paro]+costos_reales)

# Convertir la lista a un DataFrame
conceptos_paros_detalle_resultado = pd.DataFrame(conceptos_paros_detalle, columns=['ID INTERNO MOV SIESA', 'GRUPO DE PAROS','CONCEPTO', 'HORAS CONCEPTO']+columnas_reales)

for columna in columnas_reales:
    ops_teps[columna] = ops_teps[columna] * ops_teps['HORAS']
    conceptos_paros_detalle_resultado[columna] = conceptos_paros_detalle_resultado[columna] * conceptos_paros_detalle_resultado['HORAS CONCEPTO']
for columna in columnas_estandar:
    columnan = columna + ' APLICADO'
    ops_teps[columnan] = ops_teps[columna] * ops_teps['HORAS']
    ops_teps[columna] = ops_teps[columna] * ops_teps['HORAS ESTANDAR']


#----------CALCULO DE CANTIDADES TRANSFORMADAS Y ENTREGADAS

ops_teps['CANTIDAD TRANSFORMADA'] = ops_teps.apply(lambda row: row['CANTIDAD COMPLETADA OP TEPS'] if row['PROCESO'] == row['PROCESO PADRE'] else None, axis=1)

ops_teps_copia = ops_teps.copy()
ops_teps_copia = ops_teps_copia.sort_values(by=['DOCUMENTO OP', 'FECHA CONTABILIZACION TEP'], ascending=False)
ops_teps_copia['CANTIDAD RECHAZADA'] = 0.0
ops_teps_copia['CANTIDAD RETAL'] = 0.0

ops_teps_copia['ultima_fecha'] = pd.NaT

for op in ops_teps_copia['DOCUMENTO OP'].unique():
    ops_teps_op = ops_teps_copia[ops_teps_copia['DOCUMENTO OP']==op]
    suma_acum =0.0
    suma_acum2=0.0
    fecha_anterior=None
    ultima_fecha = None
    for i, row in ops_teps_op.iterrows():
        if row['PROCESO'] == row['PROCESO PADRE'] and row['CANTIDAD COMPLETADA OP TEPS']>0:
            if row['FECHA CONTABILIZACION TEP'] != fecha_anterior:
                if fecha_anterior == None:
                    suma_acum = ops_teps_op.loc[ops_teps_op['FECHA CONTABILIZACION TEP'] >= row['FECHA CONTABILIZACION TEP'], 'CANTIDAD RECHAZADA OP TEPS'].sum()
                    suma_acum2 = ops_teps_op.loc[ops_teps_op['FECHA CONTABILIZACION TEP'] >= row['FECHA CONTABILIZACION TEP'], 'CANTIDAD RETAL OP TEPS'].sum()
                else:
                    suma_acum = ops_teps_op.loc[ops_teps_op['FECHA CONTABILIZACION TEP'] >= row['FECHA CONTABILIZACION TEP'], 'CANTIDAD RECHAZADA OP TEPS'].sum()-ops_teps_op.loc[ops_teps_op['FECHA CONTABILIZACION TEP'] >= fecha_anterior, 'CANTIDAD RECHAZADA OP TEPS'].sum()
                    suma_acum2 = ops_teps_op.loc[ops_teps_op['FECHA CONTABILIZACION TEP'] >= row['FECHA CONTABILIZACION TEP'], 'CANTIDAD RETAL OP TEPS'].sum()-ops_teps_op.loc[ops_teps_op['FECHA CONTABILIZACION TEP'] >= fecha_anterior, 'CANTIDAD RETAL OP TEPS'].sum()
                fecha_anterior = row['FECHA CONTABILIZACION TEP']
                ops_teps_copia.at[i,'CANTIDAD RECHAZADA'] = suma_acum
                ops_teps_copia.at[i,'CANTIDAD RETAL'] = suma_acum2
                ultima_fecha = row['FECHA CONTABILIZACION TEP']
            else:
                ops_teps_copia.at[i, 'CANTIDAD RECHAZADA'] = suma_acum
                ops_teps_copia.at[i, 'CANTIDAD RETAL'] = suma_acum2
        ops_teps_copia.at[i,'ultima_fecha'] = ultima_fecha

suma_acum =0

for op in ops_teps_copia['DOCUMENTO OP'].unique():
    ops_teps_op = ops_teps_copia[ops_teps_copia['DOCUMENTO OP']==op]
    fecha_min = ops_teps_op['ultima_fecha'].min()
    ops_teps_op = ops_teps_op[ops_teps_op['FECHA CONTABILIZACION TEP']<=fecha_min]
    for i, row in ops_teps_op.iterrows():
        if row['PROCESO'] == row['PROCESO PADRE'] and row['FECHA CONTABILIZACION TEP'] == fecha_min and row['CANTIDAD COMPLETADA OP TEPS']>0:
            suma_acum = ops_teps_op.loc[ops_teps_op['FECHA CONTABILIZACION TEP'] < row['FECHA CONTABILIZACION TEP'], 'CANTIDAD RECHAZADA OP TEPS'].sum() + row['CANTIDAD RECHAZADA']
            suma_acum2 = ops_teps_op.loc[ops_teps_op['FECHA CONTABILIZACION TEP'] < row['FECHA CONTABILIZACION TEP'], 'CANTIDAD RETAL OP TEPS'].sum() + row['CANTIDAD RETAL']
            ops_teps_copia.at[i, 'CANTIDAD RECHAZADA'] = suma_acum
            ops_teps_copia.at[i, 'CANTIDAD RETAL'] = suma_acum2

ops_teps_copia['TOTAL TRANSFORMADA'] = ops_teps_copia.groupby(['DOCUMENTO OP','FECHA CONTABILIZACION TEP'])['CANTIDAD TRANSFORMADA'].transform('sum')







end_time = time.time()
elapsed_time = end_time - start_time


print(f"Tiempo de ejecución: {elapsed_time:.2f} segundos")


final = ops_teps
# resultado = ops_resultado_tiempos
# resultado2 = alistamiento_ops_interop
resultado3 = ops_teps_copia
# resultado4 = prueba2
resultado5 = conceptos_paros_detalle_resultado
final.to_excel("C:\\Users\\dogor\\Desktop\\Distrib_Tiempos.xlsx")
# resultado.to_excel("C:\\Users\\dogor\\Desktop\\resultado.xlsx")
# resultado2.to_excel("C:\\Users\\dogor\\Desktop\\resultado2.xlsx")
resultado3.to_excel("C:\\Users\\dogor\\Desktop\\resultado3.xlsx")
# resultado4.to_excel("C:\\Users\\dogor\\Desktop\\resultado4.xlsx")
resultado5.to_excel("C:\\Users\\dogor\\Desktop\\resultado5.xlsx")