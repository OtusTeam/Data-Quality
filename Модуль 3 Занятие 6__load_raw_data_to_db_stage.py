from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
import logging

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# Конфигурация
EXCEL_FILE_PATH = '/home/paveltashkinov/MSFO_PL_DATA_QUALITY.xlsx'
PG_CONN_ID = 'postgres_default'  # Подключение к PostgreSQL
TARGET_DB = 'DB_Stage'  # Целевая база данных

# Маппинг листов -> целевые таблицы
SHEET_TO_TABLE = {
    'MSFO_PL__RU01': 'f_msfo_pl',
    'MSFO_MARGINALITY_BASE__RU01': 'f_bm',
    'MSFO_PL_ITEMS_MAPPING': 'b_pl_bm'  # мостиковая таблица
}

def switch_to_db_stage():
    """Переключается на базу DB_Stage"""
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    
    # Проверяем, существует ли база DB_Stage
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'DB_Stage'")
    exists = cursor.fetchone()
    
    if not exists:
        # Создаем базу DB_Stage
        cursor.execute("CREATE DATABASE DB_Stage;")
        print(" База данных DB_Stage создана")
    else:
        print(" База данных DB_Stage уже существует")
    
    cursor.close()
    conn.close()
    
    # Возвращаем новое подключение уже к DB_Stage
    return PG_CONN_ID

def validate_excel_file(**context):
    """Проверяет наличие файла и всех необходимых листов"""
    if not os.path.exists(EXCEL_FILE_PATH):
        raise Exception(f" Файл не найден: {EXCEL_FILE_PATH}")
    
    print(f" Файл найден: {EXCEL_FILE_PATH}")
    
    try:
        xls = pd.ExcelFile(EXCEL_FILE_PATH)
        available_sheets = set(xls.sheet_names)
        required_sheets = set(SHEET_TO_TABLE.keys())
        
        print(f"📄 Доступные листы: {available_sheets}")
        print(f"📋 Требуемые листы: {required_sheets}")
        
        missing_sheets = required_sheets - available_sheets
        if missing_sheets:
            raise Exception(f"Отсутствуют листы: {missing_sheets}")
        
        print(" Все необходимые листы присутствуют")
        
        # Сохраняем информацию о листах в XCom
        context['task_instance'].xcom_push(key='available_sheets', value=list(available_sheets))
        
    except Exception as e:
        raise Exception(f" Ошибка при чтении Excel: {e}")

def load_sheet_to_db_stage(sheet_name, target_table, **context):
    """Загружает конкретный лист в таблицу в DB_Stage"""
    
    print(f"\n Загрузка листа {sheet_name} -> DB_Stage.{target_table}")
    
    try:
        # Читаем лист
        df = pd.read_excel(EXCEL_FILE_PATH, sheet_name=sheet_name)
        print(f" Прочитано {len(df)} строк, {len(df.columns)} колонок")
        
        if len(df) == 0:
            print(f" Лист {sheet_name} пустой, пропускаем")
            return
        
        # Очистка имен колонок для PostgreSQL
        original_columns = df.columns.tolist()
        df.columns = [col.strip().replace(' ', '_').replace('-', '_').replace('.', '_')
                      .replace('(', '').replace(')', '').replace('/', '_').replace('\\', '_')
                      .lower() for col in df.columns]
        
        print(f" Колонки: {df.columns.tolist()}")
        
        # Добавляем технические поля
        df['_load_dt'] = datetime.now()
        df['_source_file'] = os.path.basename(EXCEL_FILE_PATH)
        df['_source_sheet'] = sheet_name
        
        # Подключаемся к DB_Stage
        pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        
        # Создаем подключение с указанием базы DB_Stage
        conn = pg_hook.get_conn()
        conn.autocommit = False
        
        # Переключаемся на DB_Stage
        cursor = conn.cursor()
        cursor.execute("SET search_path TO public;")
        
        # Создаем таблицу, если не существует
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            {', '.join([f'"{col}" TEXT' for col in df.columns])}
        );
        """
        cursor.execute(create_table_sql)
        
        # Очищаем таблицу перед загрузкой (опционально)
        # cursor.execute(f"TRUNCATE TABLE {target_table};")
        
        # Загружаем данные через COPY
        from io import StringIO
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, sep='|')
        buffer.seek(0)
        
        cursor.copy_expert(f"""
            COPY {target_table} ({', '.join([f'"{col}"' for col in df.columns])})
            FROM STDIN WITH CSV DELIMITER '|';
        """, buffer)
        
        conn.commit()
        
        # Проверяем результат
        cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
        total_rows = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f"Лист {sheet_name} загружен. Всего записей в {target_table}: {total_rows}")
        
        # Сохраняем результат в XCom
        context['task_instance'].xcom_push(
            key=f'result_{sheet_name}', 
            value={'rows': len(df), 'table': target_table}
        )
        
    except Exception as e:
        print(f"Ошибка при загрузке {sheet_name}: {str(e)}")
        raise

def verify_load(**context):
    """Проверяет результаты загрузки"""
    
    print("ПРОВЕРКА ЗАГРУЗКИ В DB_Stage")
    print("="*60)
    
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Переключаемся на DB_Stage
    cursor.execute("SET search_path TO public;")
    
    for sheet, table in SHEET_TO_TABLE.items():
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f" {table} (из {sheet}): {count} записей")
        except Exception as e:
            print(f" {table} (из {sheet}): таблица не найдена или ошибка - {str(e)}")
    
    # Показываем список всех таблиц в DB_Stage
    cursor.execute("""
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public'
        ORDER BY tablename;
    """)
    tables = cursor.fetchall()
    
    print("\n Все таблицы в DB_Stage:")
    for table in tables:
        print(f"   - {table[0]}")
    
    cursor.close()
    conn.close()
    
    print("="*60)

with DAG(
    'load_raw_data_to_db_stage',
    default_args=default_args,
    description='Загрузка данных из Excel в базу DB_Stage',
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'excel', 'db_stage'],
) as dag:

    # Валидация файла
    validate_task = PythonOperator(
        task_id='validate_excel',
        python_callable=validate_excel_file,
        provide_context=True
    )
    
    # Создаем задачи загрузки для каждого листа
    load_tasks = []
    for sheet, table in SHEET_TO_TABLE.items():
        load_task = PythonOperator(
            task_id=f'load_{sheet.lower()}',
            python_callable=load_sheet_to_db_stage,
            op_kwargs={
                'sheet_name': sheet,
                'target_table': table
            },
            provide_context=True
        )
        load_tasks.append(load_task)
    
    # Проверка загрузки
    verify_task = PythonOperator(
        task_id='verify_load',
        python_callable=verify_load,
        provide_context=True
    )
    
    validate_task >> load_tasks >> verify_task
