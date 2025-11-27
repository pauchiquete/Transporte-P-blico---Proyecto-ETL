"""
ETL Pipeline para Análisis de Transporte Público
Genera datos sintéticos, los transforma y los carga en Postgres
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import logging
import os
from sqlalchemy import text

# Configuración por defecto del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Definir el DAG
dag = DAG(
    'transporte_publico_etl',
    default_args=default_args,
    description='Pipeline ETL para análisis de transporte público',
    schedule_interval='@daily',  # Se ejecuta diariamente
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['transport', 'etl', 'social'],
)

# Ruta para almacenar datos
DATA_DIR = '/opt/airflow/data'
RAW_FILE = f'{DATA_DIR}/raw_transport_data.csv'
CLEAN_FILE = f'{DATA_DIR}/clean_transport_data.csv'
PROCESSED_FILE = f'{DATA_DIR}/processed_transport_data.parquet'


def extract_data(**context):
    """
    EXTRACT: Genera datos sintéticos de transporte público
    Simula 30 días de operación con múltiples rutas y horarios
    """
    try:
        logging.info("Iniciando extracción de datos sintéticos...")
        
        # Crear directorio si no existe
        os.makedirs(DATA_DIR, exist_ok=True)
        
        # Configuración de datos sintéticos
        np.random.seed(42)
        n_records = 5000  # Registros por ejecución
        
        # Definir rutas de autobús
        routes = ['Ruta 1 - Centro', 'Ruta 2 - Norte', 'Ruta 3 - Sur', 
                  'Ruta 4 - Este', 'Ruta 5 - Oeste', 'Ruta 6 - Express']
        
        # Generar datos
        data = {
            'fecha': pd.date_range(end=datetime.now(), periods=n_records, freq='5T'),
            'ruta': np.random.choice(routes, n_records),
            'numero_bus': np.random.randint(100, 200, n_records),
            'pasajeros_subieron': np.random.poisson(8, n_records),
            'pasajeros_bajaron': np.random.poisson(6, n_records),
            'capacidad_maxima': np.random.choice([40, 50, 60], n_records),
            'hora_programada': None,
            'hora_real': None,
            'temperatura_exterior': np.random.normal(25, 5, n_records),
            'dia_semana': None,
        }
        
        df = pd.DataFrame(data)
        
        # Calcular horas programadas y reales (con retrasos aleatorios)
        df['hora_programada'] = df['fecha'].dt.floor('15T')
        df['hora_real'] = df['hora_programada'] + pd.to_timedelta(
            np.random.exponential(2, n_records), unit='m'
        )
        
        # Día de la semana
        df['dia_semana'] = df['fecha'].dt.day_name()
        
        # Simular algunos valores faltantes y duplicados (para la limpieza)
        df.loc[np.random.choice(df.index, 50), 'pasajeros_subieron'] = np.nan
        df.loc[np.random.choice(df.index, 30), 'temperatura_exterior'] = np.nan
        df = pd.concat([df, df.sample(20)], ignore_index=True)  # Duplicados
        
        # Guardar datos crudos
        df.to_csv(RAW_FILE, index=False)
        
        logging.info(f"✓ Datos extraídos exitosamente: {len(df)} registros")
        logging.info(f"✓ Archivo guardado en: {RAW_FILE}")
        
        # Push metadata al XCom
        context['task_instance'].xcom_push(key='raw_records', value=len(df))
        
        return RAW_FILE
        
    except Exception as e:
        logging.error(f"Error en extracción: {str(e)}")
        raise


def transform_data(**context):
    """
    TRANSFORM: Limpia, procesa y agrega features
    - Elimina duplicados
    - Maneja valores faltantes
    - Crea nuevas características
    - Calcula métricas agregadas
    """
    try:
        logging.info("Iniciando transformación de datos...")
        
        # Leer datos crudos
        df = pd.read_csv(RAW_FILE)
        
        # Convertir columnas de fecha de forma más robusta
        try:
            df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
            df['hora_programada'] = pd.to_datetime(df['hora_programada'], errors='coerce')
            df['hora_real'] = pd.to_datetime(df['hora_real'], errors='coerce')
        except Exception as e:
            logging.error(f"Error al convertir fechas: {str(e)}")
            raise
        
        initial_count = len(df)
        
        # LIMPIEZA
        # 1. Eliminar duplicados
        df = df.drop_duplicates().reset_index(drop=True)
        logging.info(f"✓ Duplicados eliminados: {initial_count - len(df)}")
        
        # 2. Manejar valores faltantes
        df['pasajeros_subieron'] = pd.to_numeric(df['pasajeros_subieron'], errors='coerce')
        df['pasajeros_bajaron'] = pd.to_numeric(df['pasajeros_bajaron'], errors='coerce')
        df['temperatura_exterior'] = pd.to_numeric(df['temperatura_exterior'], errors='coerce')
        
        df['pasajeros_subieron'].fillna(df['pasajeros_subieron'].median(), inplace=True)
        df['pasajeros_bajaron'].fillna(df['pasajeros_bajaron'].median(), inplace=True)
        df['temperatura_exterior'].fillna(df['temperatura_exterior'].mean(), inplace=True)
        
        # 3. Validar tipos de datos
        df['numero_bus'] = pd.to_numeric(df['numero_bus'], errors='coerce').astype(int)
        df['capacidad_maxima'] = pd.to_numeric(df['capacidad_maxima'], errors='coerce').astype(int)
        
        # FEATURE ENGINEERING
        # 1. Ocupación actual del bus
        df['flujo_neto'] = df['pasajeros_subieron'] - df['pasajeros_bajaron']
        # Agrupar por bus y ordenar por fecha antes de cumsum
        df = df.sort_values(['numero_bus', 'fecha']).reset_index(drop=True)
        df['ocupacion_actual'] = df.groupby('numero_bus')['flujo_neto'].cumsum()
        df['ocupacion_actual'] = df['ocupacion_actual'].clip(lower=0)
        
        # 2. Porcentaje de ocupación
        df['porcentaje_ocupacion'] = (df['ocupacion_actual'] / df['capacidad_maxima'] * 100).round(2)
        df['porcentaje_ocupacion'] = df['porcentaje_ocupacion'].clip(upper=100)  # No puede superar 100%
        
        # 3. Retraso en minutos
        retraso_timedelta = df['hora_real'] - df['hora_programada']
        df['retraso_minutos'] = retraso_timedelta.dt.total_seconds() / 60
        df['retraso_minutos'] = df['retraso_minutos'].round(2)
        
        # 4. Clasificación de puntualidad
        df['puntualidad'] = pd.cut(
            df['retraso_minutos'],
            bins=[-np.inf, 2, 5, np.inf],
            labels=['A Tiempo', 'Retraso Leve', 'Retraso Grave']
        )
        # Convertir a string para evitar problemas con NaN en parquet
        df['puntualidad'] = df['puntualidad'].astype(str)
        
        # 5. Periodo del día
        hora = df['fecha'].dt.hour
        df['periodo_dia'] = pd.cut(
            hora,
            bins=[0, 6, 12, 18, 24],
            labels=['Madrugada', 'Mañana', 'Tarde', 'Noche'],
            include_lowest=True
        )
        # Convertir a string para evitar problemas con NaN en parquet
        df['periodo_dia'] = df['periodo_dia'].astype(str)
        
        # 6. Indicador de hora pico
        df['es_hora_pico'] = hora.isin([7, 8, 9, 17, 18, 19]).astype(int)
        
        # 7. Convertir columnas booleanas y categóricas a tipos que Parquet maneja mejor
        df['es_hora_pico'] = df['es_hora_pico'].astype(bool)
        
        # CRUCIAL: Convertir todas las columnas de timestamp a string para evitar conflicto ns/us con PyArrow
        # PyArrow 11.0.0 usa microsegundos pero pandas usa nanosegundos
        logging.info("Convirtiendo timestamps a formato compatible...")
        df['fecha'] = df['fecha'].astype(str)
        df['hora_programada'] = df['hora_programada'].astype(str)
        df['hora_real'] = df['hora_real'].astype(str)
        
        # AGREGACIONES (para el dashboard)
        # Guardar datos limpios en CSV (sin tipos especiales)
        df_csv = df.copy()
        df_csv['puntualidad'] = df_csv['puntualidad'].fillna('Desconocido')
        df_csv['periodo_dia'] = df_csv['periodo_dia'].fillna('Desconocido')
        df_csv.to_csv(CLEAN_FILE, index=False)
        logging.info(f"✓ Datos limpios guardados: {CLEAN_FILE}")
        
        # Rellenar NaN en categorías con valor por defecto antes de guardar en Parquet
        df['puntualidad'] = df['puntualidad'].fillna('Desconocido')
        df['periodo_dia'] = df['periodo_dia'].fillna('Desconocido')
        
        # Guardar en formato Parquet (más eficiente)
        try:
            df.to_parquet(PROCESSED_FILE, index=False, compression='snappy')
            logging.info(f"✓ Datos en formato Parquet: {PROCESSED_FILE}")
        except Exception as parquet_error:
            logging.error(f"Error al guardar Parquet con snappy: {str(parquet_error)}")
            try:
                df.to_parquet(PROCESSED_FILE, index=False, compression=None)
                logging.info(f"✓ Datos en formato Parquet (sin compresión): {PROCESSED_FILE}")
            except Exception as parquet_error2:
                logging.error(f"Error al guardar Parquet sin compresión: {str(parquet_error2)}", exc_info=True)
                raise
        
        # Push metadata
        context['task_instance'].xcom_push(key='clean_records', value=len(df))
        context['task_instance'].xcom_push(key='records_removed', value=initial_count - len(df))
        
        logging.info(f"✓ Transformación completada exitosamente")
        return PROCESSED_FILE
        
    except Exception as e:
        logging.error(f"Error en transformación: {str(e)}", exc_info=True)
        raise


def load_to_postgres(**context):
    """
    LOAD: Carga datos limpios a PostgreSQL
    Crea la tabla si no existe y carga los datos por chunks
    """
    engine = None
    try:
        logging.info("Iniciando carga a PostgreSQL...")
        
        # Leer datos procesados
        df = pd.read_parquet(PROCESSED_FILE)
        
        # Obtener conexión a Postgres
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = postgres_hook.get_sqlalchemy_engine()
        
        # Cargar datos en chunks (escalabilidad)
        chunk_size = 1000
        total_chunks = (len(df) // chunk_size) + (1 if len(df) % chunk_size > 0 else 0)
        
        for i, chunk in enumerate(np.array_split(df, total_chunks)):
            try:
                chunk.to_sql(
                    'transport_data',
                    engine,
                    if_exists='append' if i > 0 else 'replace',
                    index=False,
                    method='multi'
                )
                logging.info(f"✓ Chunk {i+1}/{total_chunks} cargado ({len(chunk)} registros)")
            except Exception as chunk_error:
                logging.error(f"Error al cargar chunk {i+1}: {str(chunk_error)}", exc_info=True)
                raise
        
        logging.info(f"✓ {len(df)} registros cargados exitosamente a PostgreSQL")
        
        # Verificar carga
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM transport_data"))
                count = result.fetchone()[0]
                logging.info(f"✓ Verificación: {count} registros en la tabla")
                conn.close()
        except Exception as verify_error:
            logging.error(f"Advertencia: No se pudo verificar la carga: {str(verify_error)}")
        
        logging.info("✓ Carga a PostgreSQL completada")
        return True
        
    except Exception as e:
        logging.error(f"Error en carga a Postgres: {str(e)}", exc_info=True)
        raise
    finally:
        # Asegurar cierre de conexión
        if engine:
            try:
                engine.dispose()
                logging.info("Conexión a PostgreSQL cerrada")
            except:
                pass


def generate_summary_stats(**context):
    """
    Genera estadísticas resumen para validación y monitoreo
    """
    try:
        logging.info("Generando estadísticas resumen...")
        
        df = pd.read_parquet(PROCESSED_FILE)
        
        # Convertir columnas numéricas a float para cálculos
        df['porcentaje_ocupacion'] = pd.to_numeric(df['porcentaje_ocupacion'], errors='coerce')
        df['retraso_minutos'] = pd.to_numeric(df['retraso_minutos'], errors='coerce')
        
        # Las fechas ahora son strings, convertir para min/max
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
        
        stats = {
            'total_registros': int(len(df)),
            'rutas_unicas': int(df['ruta'].nunique()),
            'buses_unicos': int(df['numero_bus'].nunique()),
            'ocupacion_promedio': float(df['porcentaje_ocupacion'].mean().round(2)),
            'retraso_promedio': float(df['retraso_minutos'].mean().round(2)),
            'fecha_inicio': df['fecha'].min().strftime('%Y-%m-%d'),
            'fecha_fin': df['fecha'].max().strftime('%Y-%m-%d'),
        }
        
        logging.info("=" * 50)
        logging.info("RESUMEN DE PROCESAMIENTO")
        logging.info("=" * 50)
        for key, value in stats.items():
            logging.info(f"{key}: {value}")
        logging.info("=" * 50)
        
        # Push al XCom
        context['task_instance'].xcom_push(key='summary_stats', value=stats)
        
        logging.info("✓ Estadísticas generadas exitosamente")
        return stats
        
    except Exception as e:
        logging.error(f"Error generando estadísticas: {str(e)}", exc_info=True)
        raise


# Definir tareas
task_extract = PythonOperator(
    task_id='extract_transport_data',
    python_callable=extract_data,
    dag=dag,
)

task_transform = PythonOperator(
    task_id='transform_transport_data',
    python_callable=transform_data,
    dag=dag,
)

task_load = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

task_summary = PythonOperator(
    task_id='generate_summary_stats',
    python_callable=generate_summary_stats,
    dag=dag,
)

# Definir dependencias (flujo del pipeline)
task_extract >> task_transform >> [task_load, task_summary]
