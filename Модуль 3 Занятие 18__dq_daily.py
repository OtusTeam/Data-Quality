"""
Простой DAG для ежедневного запуска всех dq-вьюшек и записи в dq.dq_history
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

default_args = {
    'owner': 'data_quality',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'dq_daily',
    default_args=default_args,
    description='Автоматические ежедневные проверки качества данных',
    schedule_interval='0 8 * * *',
    catchup=False,
    tags=['dq'],
)

def run_all_dq_views():
    """Запускает все вьюшки dq.v_dq_* и инсертит результаты в dq.dq_history"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Получаем список всех вьюшек в схеме dq
    get_views_sql = """
    SELECT table_name 
    FROM information_schema.views 
    WHERE table_schema = 'dq' 
      AND table_name LIKE 'v_dq_%'
    ORDER BY table_name;
    """
    
    views = hook.get_records(get_views_sql)
    
    for view in views:
        view_name = view[0]
        logging.info(f"Обрабатываем вьюшку: {view_name}")
        
        # Инсертим результат в историю
        insert_sql = f"""
        INSERT INTO dq.dq_history (dq_check_id, dq_dimension, value, target_value, metric_weight, dq_check_dttm)
        SELECT dq_check_id, dq_dimension, value::NUMERIC, target_value::NUMERIC, metric_weight::NUMERIC, dq_check_dttm
        FROM dq.{view_name}
        WHERE dq_check_id IS NOT NULL;
        """
        
        try:
            hook.run(insert_sql)
            logging.info(f"✓ {view_name} успешно загружена")
        except Exception as e:
            logging.error(f"✗ Ошибка в {view_name}: {str(e)}")

# Создаем одну задачу, которая запускает все вьюшки
run_views = PythonOperator(
    task_id='run_all_dq_views',
    python_callable=run_all_dq_views,
    dag=dag,
)
