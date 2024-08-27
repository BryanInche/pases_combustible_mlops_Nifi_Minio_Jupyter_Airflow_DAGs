# Librerias para trabajar con Airflow Dags y todos los trabajos
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from io import BytesIO
from minio import Minio
import urllib3
from pyspark.sql import SparkSession
from delta import *
import numpy as np

#PASO 1 :  Define una funcion de Python Operatior si es Necesario para tu DAG

# 1.1 Función para leer datos desde MinIO
def read_data_from_minio(ti):

    client = Minio(
        "192.168.25.223", #Dirección del servidor MinIO
        #"minio.minio-user.svc.cluster.local:80",  # Dirección del servidor MinIO
        access_key="LjPODFkcEC5NwpsNCGWX",         # Clave de acceso
        secret_key="PIGKHRP0HoQF4yrkcumtXJamnRTxtlwpHwTvKtn2",  # Clave secreta
        secure=False,  # Cambia a True si usas HTTPS
        http_client=urllib3.PoolManager(cert_reqs='CERT_NONE')  # Ignora la verificación del certificado
    )

    bucket_name = 'hudbayraw2'
    object_name = 'bd_hubbay_pases_all.parquet'

    # Obtener el archivo Parquet del bucket
    response = client.get_object(bucket_name, object_name)
    data = response.read()

    # Convertir los datos leídos en un DataFrame de Pandas
    parquet_buffer = BytesIO(data)
    df = pd.read_parquet(parquet_buffer)

    # Utilizar ti.xcom_push para pasar los datos al siguiente task
    ti.xcom_push(key='minio_data', value=df)

    # (Opcional) Procesar o almacenar el DataFrame como necesites
    #return df.head()


# 1.2 Función para preprocesar datos y guardar como tabla Delta en MinIO
def preprocess_and_save_delta(ti):
    # Recuperar los datos desde XComs
    datos = ti.xcom_pull(key='minio_data', task_ids='read_data_minio')  #'read_data_minio'  es el task_id definido mas abajo que pertenece a la funcion read_data_from_minio

    # 1. Tratamiento de valores nulos
    valores_nulos = datos.isnull().sum()
    porcentaje_nulos = (valores_nulos / len(datos)) * 100
    columnas_a_eliminar = porcentaje_nulos[porcentaje_nulos > 80].index
    datos = datos.drop(columnas_a_eliminar, axis=1)

    #4. Tratamiento de variables 
    #4.1 Eliminando columnas especificas que no aportan informacion ( # errors='ignore':ignore cualquier error si alguna de las columnas especificadas no se encuentra en el DataFrame.)
    datos = datos.drop(['tipoubicacionsupervisor_camion','tipoubicacionsupervisor_pala', 'id_cargadescarga_pases','dumpreal','loadreal', 'rownum'], axis=1, errors='ignore') # 'turno'

    # 3.2 Calcula la moda de 'has_block_pases' y Completa los valores nulos con la moda en la columna 'has_block_pases'
    moda_has_block_pases = datos['has_block_pases'].mode()[0]
    datos['has_block_pases'].fillna(moda_has_block_pases, inplace=True)

    # 3.3 Calcula la moda de 'tipodescargaidentifier' y completa los valores nulos con la moda en la columna 'tipodescargaidentifier'
    moda_tipodescargaidentifier = datos['tipodescargaidentifier'].mode()[0]
    datos['tipodescargaidentifier'].fillna(moda_tipodescargaidentifier, inplace=True)

    #4. Transformacion de Datos Tiempo a formato Datetime
    #4.1 Convertir las columnas a DateTime si aun no la estan
    # Lista de columnas que contienen fechas
    columnas_fecha = ['tiem_llegada_global', 'tiem_esperando', 'tiem_cuadra', 
                    'tiem_cuadrado', 'tiem_carga', 'tiem_acarreo', 'tiem_cola', 
                    'tiem_retro', 'tiem_listo', 'tiem_descarga', 'tiem_viajando', 
                    'tiempo_inicio_carga_carguio', 'tiempo_esperando_carguio', 
                    'previous_esperando_pala', 'tiempo_inicio_cambio_estado_camion', 
                    'tiempo_inicio_cambio_estado_pala']

    # 4.2 Convertir todas las fechas al mismo formato y eliminar valores Nulos de los DATETIME
    for columna in columnas_fecha:
        datos[columna] = pd.to_datetime(datos[columna], errors='coerce') #Si hubiese una fecha con  Error, lo reemplaza con NAT
    # 4.3 Reemplazar los valores nulos con la fecha de la fila anterior más 3 segundos adicionales
    for columna in columnas_fecha:
        mask_nat = datos[columna].isna()
        datos[columna].loc[mask_nat] = datos[columna].fillna(method='ffill') + pd.to_timedelta(3, unit='s')
    # 4.4 Formatear las fechas en el formato deseado
    formato_deseado = "%Y-%m-%d %H:%M:%S.%f%z"  # Formato deseado
    for columna in columnas_fecha:
        datos[columna] = datos[columna].dt.strftime(formato_deseado)
        datos[columna] = pd.to_datetime(datos[columna])
        datos[columna] = datos[columna] + pd.to_timedelta(999, unit='ms')


    # 3.4 Rellenar los valores nulos con ceros en todo el DataFrame
    datos = datos.fillna(0)

    #5. Eliminamos los filas duplicadas
    # 5.1. Identificar las columnas no de tipo 'object'
    columnas_no_object = datos.select_dtypes(exclude=['object']).columns

    # 5.2. Eliminar duplicados basados solo en las columnas no 'object'
    datos = datos.drop_duplicates(subset=columnas_no_object)

    # 6. Transformacion a Tipo de datos adecuado formato para las variables
    # 6.1 Diccionario para especificar los tipos de datos deseados para cada columna
    tipos_de_datos = {
        'id_ciclo_acarreo': 'int64','id_cargadescarga': 'int64','id_palas': 'int64','id_equipo_camion': 'int64','id_ciclo_carguio': 'float64',
        'id_equipo_carguio': 'float64','id_trabajador_pala': 'float64','id_guardia_realiza_carga_al_camion': 'float64','id_locacion': 'float64',
        'id_poligono_se_obtiene_material': 'float64','tiempo_ready_cargando_pala': 'float64','tiempo_ready_esperando_pala': 'float64',
        'cantidad_equipos_espera_al_termino_carga_pala': 'float64','id_estados_camion': 'int64','id_equipo_table_estados_camion': 'int64',
        'id_detal_estado_camion': 'int64','tiempo_estimado_duracion_estado_camion': 'int64','en_campo_o_taller_mantenimiento_camion': 'int64',
        'id_tipo_estad_camion': 'int64','id_estados_pala': 'float64','id_equipo_table_estados_pala': 'float64','id_detal_estado_pala': 'float64',
        'tiempo_estimado_duracion_estado_pala': 'float64','en_campo_o_taller_mantenimiento_pala': 'float64','id_tipo_estad_pala': 'float64',
        'id_descarga': 'int64','id_factor': 'int64','id_poligono': 'float64', 
        #'tiempo_ready_llegada_esperando': 'float64','tiempo_ready_esperando_cuadra': 'float64','tiempo_ready_cuadra_cuadrado': 'float64', #'tiempo_ready_cuadrado_cargado': 'float64','tiempo_ready_carga_acarreo': 'float64','tiempo_ready_acarreo_cola': 'float64',#'tiempo_ready_cola_retro': 'float64','tiempo_ready_retro_listo': 'float64','tiempo_ready_listo_descarga': 'float64',#'tiempo_ready_descarga_viajandovacio': 'float64',
        'id_trabajador_camion': 'int64','id_palanext': 'int64','tonelajevims': 'float64','yn_estado': 'bool',
        'id_guardia_hizocarga': 'int64','id_guardia_hizodescarga': 'int64','id_zona_aplicafactor': 'int64','id_zona_pertenece_poligono': 'float64',
        'factor': 'int64','toneladas_secas': 'float64',
        #'productividad_operativa_acarreo_tn_h': 'float64',
        'productividad_operativa_carguio_tn_h': 'float64','efhcargado': 'float64','efhvacio': 'float64','distrealcargado': 'float64','distrealvacio': 'float64','coorxdesc': 'float64',
        'coorydesc': 'float64','coorzdesc': 'float64','tipodescargaidentifier': 'float64','tonelajevvanterior': 'int64','tonelajevvposterior': 'float64','velocidadvimscargado': 'float64','velocidadvimsvacio': 'float64','velocidadgpscargado': 'float64','velocidadgpsvacio': 'float64','tonelajevimsretain': 'float64', 'nivelcombuscargado': 'float64','nivelcombusdescargado':'float64',
        'volumen': 'float64','aplicafactor_vol': 'bool','coorzniveldescarga': 'float64','efh_factor_loaded': 'float64','efh_factor_empty':'float64',
        'id_secundario': 'int64','id_principal': 'int64','capacidad_vol_equipo': 'float64','capacidad_pes_equipo': 'float64',
        'capacidadtanque_equipo': 'int64','peso_bruto_equipo': 'float64','ishp_equipo': 'bool','ancho_equipo': 'int64','largo_equipo': 'int64',
        'numeroejes_equipo': 'int64','id_turnos_turnocarga': 'int64','horaini_turnocarga': 'int64','horafin_turnocarga': 'int64',
        'id_turnos_turnodescarga': 'int64','horaini_turnodescarga': 'int64','horafin_turnodescarga': 'int64',
        'id_zona_encuentra_descarga': 'int64','id_nodo_carga': 'float64','id_nodo_descarga': 'int64',
        'elevacion_descarga': 'int64','nivel_elevacion_locacion_mts': 'float64','radio_locacion': 'float64','id_material': 'float64',
        'elevacion_poligono_mts': 'float64','densidad_poligono': 'float64','tonelaje_inicial_poligono': 'float64','id_pases': 'float64',
        'id_palas_pases': 'float64','angulo_giro_promedio_pases': 'float64','has_block_pases': 'bool','capacidad_pes_equipo_carguio': 'float64', 'capacidad_vol_equipo_carguio': 'float64', 'tonelaje': 'int64',
        'radiohexagonocuchara_equipo_carguio': 'int64' }

    # 6.2 Convertir las columnas al tipo de dato correspondiente
    for columna, tipo in tipos_de_datos.items():
        datos[columna] = datos[columna].astype(tipo)

    # 8. Transformacion de datos, convercion de Tipo de datos a Booleano (True/False)
    # VARIABLE 1 
    # 8.1 Reemplazar '0' por la moda
    datos['termino_carga_equipo_en_espera_cuadrado_cuadrandose_carguio'] = datos['termino_carga_equipo_en_espera_cuadrado_cuadrandose_carguio'].replace(0,  datos['termino_carga_equipo_en_espera_cuadrado_cuadrandose_carguio'].mode().iloc[0])
    # 8.2 Reemplazar 'True' por True y 'False' por False , Paso crucial para antes de Convertir a DATOS BOOLEANO
    datos['termino_carga_equipo_en_espera_cuadrado_cuadrandose_carguio'] = datos['termino_carga_equipo_en_espera_cuadrado_cuadrandose_carguio'].replace({'True': True, 'False': False})
    # 8.3 Convertir la columna a tipo de datos booleano
    datos['termino_carga_equipo_en_espera_cuadrado_cuadrandose_carguio'] = datos['termino_carga_equipo_en_espera_cuadrado_cuadrandose_carguio'].astype(bool)

    # VARIABLE 2
    # 8.4 Reemplazar '0' por la moda
    datos['cambio_estado_operatividad_carguio'] =datos['cambio_estado_operatividad_carguio'].replace(0, datos['cambio_estado_operatividad_carguio'].mode().iloc[0])

    # 8.5 Reemplazar 'True' por True y 'False' por False , Paso crucial para antes de Convertir a DATOS BOOLEANO
    datos['cambio_estado_operatividad_carguio'] = datos['cambio_estado_operatividad_carguio'].replace({'True': True, 'False': False})

    # 8.6 Convertir la columna a tipo de datos booleano
    datos['cambio_estado_operatividad_carguio'] = datos['cambio_estado_operatividad_carguio'].astype(bool)


    # #pasar tonelajevims a toneladas
    datos['tonelajevims'] = datos['tonelajevims'] / 10


    # 9. Renombrar variables para mayor entendimiento del negocio
    # Define un diccionario con los nuevos nombres de las columnas solo para algunas columnas
    nuevos_nombres = {'id_cargadescarga' : 'id_cargadescarga_ciclo',
        'termino_carga_equipo_en_espera_cuadrado_cuadrandose_carguio' : 'al_termino_cargar_en_espera_cuadrado_cuadrandose',
        'id_descarga' : 'id_zona_hace_descarga',  
        'tiem_llegada_global': 'tiempo_llegada_camion', 'tiem_esperando': 'tiempo_esperando_camion_en_locacion', 
        'tiem_cuadra': 'tiempo_cuadra_camion','tiem_cuadrado': 'tiempo_cuadrado_camion' , 'tiem_carga' : 'tiempo_cargar_al_camion', 
        'tiem_acarreo' : 'tiempo_acarreo_camion', 'tiem_cola': 'tiempo_cola_camion_en_zonadescarga','tiem_retro': 'tiempo_retroceso_para_descargar',
        'tiem_listo' : 'tiempo_listo_para_descargar',  'tiem_descarga': 'tiempo_descarga_camion', 'tiem_viajando': 'tiempo_viajando_vacio_locacion', 
        'tonelaje':'tonelaje_nominal', 'tonelajevims':'tonelaje_segun_computadora', 'yn_estado': 'cambios_estado_en_ciclo',
        'distrealcargado': 'distancia_recorrida_camioncargado_km_gps_mts', 'distrealvacio': 'distancia_recorrida_camionvacio_km_gps_mts' ,
        'coorxdesc': 'coordenada_x_descarga_km', 'coorydesc': 'coordenada_y_descarga_km' , 'coorzdesc': 'coordenada_z_descarga_km',
        'tipodescargaidentifier': 'tipo_descarga_efectuado', 
        'tonelajevvanterior': 'tonelaje_camion_viajevacio_cicloanterior_vims', 'tonelajevvposterior': 'tonelaje_camion_viajevacio_cicloactual_vims',
        'velocidadvimscargado': 'promedio_velocidad_camioncargado_km/hr_compu', 
        'velocidadvimsvacio': 'promedio_velocidad_camionvacio_km/hr_compu',
        'velocidadgpscargado':'promedio_velocidad_camioncargado_km/hr_gps', 'velocidadgpsvacio': 'promedio_velocidad_camionvacio_km/hr_gps', 
        'tonelajevimsretain': 'tonelaje_camion_antes_cargaestabilizada', 'nivelcombuscargado': 'porcentaje_combustible_camioncargando', 
        'nivelcombusdescargado':'porcentaje_combustible_camiondescargando', 'volumen': 'volumen_nominal', 'aplicafactor_vol': 'aplica_factor_volumen_o_tonelaje',
        'coorzniveldescarga': 'nivel_descarga_metros', 'nombre_equipo':'nombre_equipo_acarreo', 
        'id_secundario':'id_flota_secundaria', 'flota_secundaria':'nombre_flota_secundaria', 'id_principal': 'id_flota_principal', 'flota_principal':'nombre_flota_principal',
        'capacidad_vol_equipo': 'capacidad_en_volumen_equipo_acarreo_m3', 'capacidad_pes_equipo':'capacidad_en_peso_equipo_acarreo', 'capacidadtanque_equipo': 'capacidadtanque_equipoacarreo_galones',
        'peso_bruto_equipo':'peso_bruto_equipo_acarreo', 'ishp_equipo':'si_no_equipo_altaprecision', 'ancho_equipo':'ancho_equipo_metros', 'largo_equipo':'largo_equipo_metros',
        'elevacion_descarga':'nivel_elevacion_descarga_metros', 'nombre_descarga':'nombre_zona_descarga', 
        'nombre_carga_locacion':'nombre_locacion_carga', 'nivel_elevacion_locacion_mts':'nivel_elevacion_locacion_carga_metros', 'radio_locacion':'radio_locacion_metros',
        'ids_poligonos_en_locacion':'ids_poligonos_en_locacion_carga', 'id_material': 'id_material_dominante_en_poligono', 
        'elevacion_poligono_mts':'elevacion_poligono_metros', 'ley_in':'lista_leyes', 'densidad_poligono':'densidad_inicial_poligono_creado_tn/m3',
        'capacidad_vol_equipo_carguio' : 'capacidad_en_volumen_equipo_carguio_m3', 'capacidad_pes_equipo_carguio':'capacidad_en_peso_equipo_carguio', 'capacidadtanque_equipo_carguio': 'capacidadtanque_equipocarguio_galones',
        'radiohexagonocuchara_equipo_carguio' : 'radiohexagonocuchara_equipocarguio', 'id_tablegen' : 'id_guardia_acarreocarga', 'nombre_tablegen' : 'nombre_guardia_acarreocarga', 
        'id_guardiadescarga': 'id_guardia_acarreodescarga', 'nombre_guardiadescarga':'nombre_guardia_acarreodescarga',
        'id':'id_guardia_carguio', 'nombre': 'nombre_guardia_carguio', 'id_locacion' : 'id_locacion_hace_carga','tonelaje_inicial_poligono': 'tonelaje_inicial_poligono_creado',  'efhvacio':'efhvacio_mts', 'efhcargado':'efhcargado_mts'
    }
    # 9.1 Renombra las columnas del DataFrame
    datos = datos.rename(columns=nuevos_nombres)

    #10. Agregamos Nuevas variables calculadas
    #Agregamos la variable 'porcentaje_eficiencia_toneladas_movidas_acarreo'
    datos['porcentaje_eficiencia_toneladas_movidas_acarreo'] = (datos['tonelaje_segun_computadora'] / datos['tonelaje_nominal']) * 100

    #Agregamos la variable 'altura_elevacion'
    datos['altura_elevacion'] = abs(datos['nivel_elevacion_descarga_metros'] - datos['nivel_elevacion_locacion_carga_metros'] )

    # Agregamos la variable 'factor_perfil_rutavacio_mts'
    datos['factor_perfil_rutavacio'] = np.where(datos['distancia_recorrida_camionvacio_km_gps_mts'] != 0,
                                                datos['efhvacio_mts'] / datos['distancia_recorrida_camionvacio_km_gps_mts'],
                                                0)

    # Agregamos la variable 'factor_perfil_rutacargado_mts'
    datos['factor_perfil_rutacargado'] = np.where(datos['distancia_recorrida_camioncargado_km_gps_mts'] != 0,
                                                datos['efhcargado_mts'] / datos['distancia_recorrida_camioncargado_km_gps_mts'],
                                                0)

    # Agregamos la variable calculada "numero_pases_carguio" basado en la columna 'coord_x_pases'
    # datos['numero_pases_carguio'] = datos['coord_x_pases'].apply(lambda x: len(eval(x)) if isinstance(x, str) and '[' in x else x if isinstance(x, int) else 0)
    # Calcular el número de elementos en cada array y manejar arrays vacíos
    datos['numero_pases_carguio'] = datos['coord_x_pases'].apply(lambda x: len(x) if isinstance(x, np.ndarray) else 0)

    #Agregamos la variable Galones_diponible_camioncargando
    datos['Galones_disponibles_camioncargando'] = (datos['porcentaje_combustible_camioncargando']/100) * datos['capacidadtanque_equipoacarreo_galones']
    #datos['demanda_galones_camioncargando'] = (datos['porcentaje_combustible_camioncargando']/100) * datos['capacidadtanque_equipoacarreo_galones']

    #Agregar la variable Galones_diponible_camiondescargando 
    datos['Galones_disponibles_camiondescargando'] = (datos['porcentaje_combustible_camiondescargando']/100) * datos['capacidadtanque_equipoacarreo_galones']

    #Agregar la variable Galones_consumidos_entre_cargando_descargando 
    datos['Galones_consumidos_entre_cargando_descargando_acarreo'] = datos['Galones_disponibles_camioncargando'] - datos['Galones_disponibles_camiondescargando']


    # 11. Agregar mas filtros 
    #11.1 Filtramos la fecha
    fecha_minima = '2023-01-01 00:00:00.999'
    datos=datos.sort_values(by='tiempo_inicio_carga_carguio') 
    datos=datos[datos['tiempo_inicio_carga_carguio'] > fecha_minima]

    # 12. Calculo tonelaje por pase
    # Paso 1: Asegúrate de que todos los datos en 'tonelaje_pases' son cadenas de texto
    datos['tonelaje_pases'] = datos['tonelaje_pases'].astype(str)

    # Paso 2: Reemplazar espacios por comas en las cadenas de texto
    datos['tonelaje_pases'] = datos['tonelaje_pases'].apply(lambda x: x.replace(' ', ','))

    # Paso 3: Reemplazar cualquier carácter no numérico (excepto comas) por vacío
    datos['tonelaje_pases'] = datos['tonelaje_pases'].str.replace(r'[^\d,]', '', regex=True)

    # Paso 4: Dividir las cadenas en columnas usando comas como delimitador
    tabla_datos2 = datos['tonelaje_pases'].str.split(',', expand=True)

    # Paso 5: Reemplazar valores vacíos por NaN y luego convertir a float
    tabla_datos2 = tabla_datos2.replace('', np.nan).astype(float)

    # Paso 6: Calcular la suma de los tonelajes por fila y dividir por 10
    datos['tonelaje_pases'] = tabla_datos2.sum(axis=1) / 10

    # Filtro 
    # 13. Tratamiento de Valores Outliers (Numero de Pases)
    mask = datos['numero_pases_carguio'] >= 4
    datos = datos[mask]

    # 13. Reeemplzar Capacidad carguio != 0 (Numero de Pases)
    mask = datos['capacidad_en_peso_equipo_carguio']  != 0 
    datos = datos[mask]
    
    # Filtramos los tonelajes Vims (70% * capacidad de tolva) , para tener ciclos con pases 
    mask = (datos['tonelaje_segun_computadora'] > 150)
    datos = datos[mask]
    
    # Filtramos los tonelajes por pases, que sean distintos de 0, en cada pase que se dio 
    mask = datos['tonelaje_pases'] > 0
    datos = datos[mask]
    
    # Filtrar las tonaledas por pase. que los pases dados, sean superioes a (70%capacidad tolva) y menores al (125%capacidad tolva)
    mask = (datos['tonelaje_pases'] >= 180) & (datos['tonelaje_pases'] <= 400)
    datos = datos[mask]
    
    # # Filtramos la Procedencia de Material diferente de DES 
    # mask = datos['Procedencia'] != 'DES'
    # datos = datos[mask]
    
    # Filtramos solo pases por encima de 4, y los configuramos con el tonelaje Vims 
    mask = datos['numero_pases_carguio'] >= 4
    datos.loc[mask, 'numero_pases_carguio2'] = (datos[mask]['tonelaje_segun_computadora'] / datos[mask]['capacidad_en_peso_equipo_carguio']).round(0)


    # Paso 1: Cambiar 'DELAY' a 'READY' donde el estado no es 'DELAY' y 'numero_pases_carguio' es diferente de 0
    datos.loc[(datos['estado_primario_pala'] != 'DELAY'), 'estado_primario_pala'] = 'READY'

    # Paso 2: Cambiar 'DELAY' a 'READY' donde 'numero_pases_carguio' es diferente de 0
    datos.loc[(datos['estado_primario_pala'] == 'DELAY') & (datos['numero_pases_carguio'] > 0), 'estado_primario_pala'] = 'READY'

    # Paso 3: Filtrar solo los estados 'READY' (donde se efectuaron pases)
    datos = datos[(datos['estado_primario_pala'] == 'READY') & (datos['numero_pases_carguio'] > 0)]

    # Agregamos la variable 'tiempo de carga', cuanto demora el operador de carguio en realizar el carguio al camion de acarreo
    datos['tiempo_carga'] = (datos['tiempo_esperando_carguio'] - datos['tiempo_inicio_carga_carguio']).dt.total_seconds() #Pasas a segundos


    #4.1 Eliminando columnas especificas que no aportan informacion
    datos = datos.drop(['previous_esperando_pala','ids_poligonos_en_locacion_carga', 'estado_detalle_camion', 'estado_secundario_camion', 'estado_primario_camion', 'estado_detalle_pala',
        'estado_secundario_pala', 'estado_primario_pala','nombre_equipo_acarreo', 'nombre_flota_secundaria','nombre_flota_principal', 'nombre_turnocarga',
        'nombre_turnodescarga', 'nombre_zona_descarga', 'nombre_locacion_carga', 'nombre_poligono', 'lista_leyes', 'coord_x_pases', 'coord_y_pases', 'coord_z_pases', 
            'angulo_giro_pases', 'duracion_excavacion_pases'], axis=1, errors='ignore') # errors='ignore':ignore cualquier error si alguna de las columnas especificadas no se encuentra en el DataFrame.

    # Remover la zona horaria de las columnas datetime
    for col in ['tiempo_inicio_carga_carguio', 'tiempo_esperando_carguio', 'tiempo_inicio_cambio_estado_camion', 
                'tiempo_inicio_cambio_estado_pala', 'tiempo_llegada_camion', 'tiempo_esperando_camion_en_locacion',
                'tiempo_cuadra_camion', 'tiempo_cuadrado_camion', 'tiempo_cargar_al_camion', 'tiempo_acarreo_camion',
                'tiempo_cola_camion_en_zonadescarga', 'tiempo_retroceso_para_descargar', 'tiempo_listo_para_descargar',
                'tiempo_descarga_camion', 'tiempo_viajando_vacio_locacion']:
        if pd.api.types.is_datetime64tz_dtype(datos[col]):
            datos[col] = datos[col].dt.tz_localize(None)


    # 2. Configuración de Spark y Delta
    S3_ACCESS_KEY = "nIfbWDx9lhnt5Y2I58bh"
    S3_BUCKET = "hudbayprocessed2"
    S3_SECRET_KEY = "OjbcZm24KUk8jlEPqedS7FzmvHilnwkN5mK77E7e"
    S3_ENDPOINT = "192.168.25.223"


    # Configuración de la sesión de Spark para guardar como tabla Delta en MinIO
    spark = SparkSession \
        .builder \
        .appName("AppDeltaMinio9") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.memory", "4g") \
        .config("spark.driver.cores", "2") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.2.0,com.amazonaws:aws-java-sdk-pom:1.12.720") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
        .getOrCreate()

    spark_df = spark.createDataFrame(datos)
    spark_df.write.format("delta").mode("overwrite").save(f"s3a://hudbayprocessed2/tabladelta_procesing_hudbay_bryan")

# PASO 2 : Definir el DAG (argumentos basicos)
# Definir el DAG
with DAG(
    'dag_modelo_pasesvf',
    description='DAG-ETL-Modelo-Pases',
    schedule_interval='@daily',  # Configura el intervalo según tus necesidades
    start_date=datetime(2024, 8, 25),  # Fecha y hora de inicio yyyy/mm/dd
    catchup=False,
) as dag:
    

# PASO 3 : Definir los Task del DAG

    # Crear la tarea de lectura
    read_task = PythonOperator(
        task_id='read_data_minio',
        python_callable=read_data_from_minio,
    )

    preprocess_task = PythonOperator(
        task_id='preprocess_save_delta',
        python_callable=preprocess_and_save_delta,
    )

# PASO 4 : Secuenciar las tareas del DAG(basados en los task)
    read_task >> preprocess_task
