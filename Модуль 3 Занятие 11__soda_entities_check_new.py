from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Константы
SODA_CONFIG = "/opt/airflow/soda/configuration.yml"
SODA_CHECKS = "/opt/airflow/soda/checks/checks.yml"
SODA_RESULTS = "/opt/airflow/soda/results"
SODA_DATASOURCE = "postgres_dev"
TABLE_NAME = "entities"

DB_CONN = {
    "host": "postgres_dev",
    "port": 5432,
    "user": "demo",
    "password": "demo",
    "dbname": "test"
}

# --- Main
def run_soda_and_log():
    import psycopg2
    from soda.scan import Scan
    from datetime import datetime
    import re, json

    # 1. Запустить SODA scan
    scan = Scan()
    scan.set_data_source_name(SODA_DATASOURCE)
    scan.add_configuration_yaml_file(SODA_CONFIG)
    scan.add_sodacl_yaml_file(SODA_CHECKS)
    scan.execute()

    # 2. Проверить на ошибки самого скана
    if scan.has_error_logs():
        raise Exception(f"SODA scan error:\n{scan.get_error_logs_text()}")

    # 3. Записать логи
    scan_results = scan.get_scan_results()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"{SODA_RESULTS}/scan_results_{timestamp}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(scan_results, f, indent=2, ensure_ascii=False)

    print(f"Результаты сохранены: {output_path}")

    # 4. Распарсить результаты
    rows = []
    now = datetime.now()
    pattern = re.compile(
        r"\]\s+(PASS|FAIL|WARN)\.\s+"  # статус
        r"(.+?)"  # название проверки
        r"\s+\[(.+?)\]"  # значение в скобках
    )
    check = scan._checks[0]
    print(vars(check))
    print(vars(check.check_cfg))
    for i,check in enumerate(scan._checks):
        print (f'Processing check {i}\n{check}')
        print(check.check_cfg.source_header.replace("checks for",""))
        if 'table' in check.dict.values():
            TABLE_NAME = check.dict['table']
        else:
            print(check.dict.values())
            TABLE_NAME = check.check_cfg.source_header.replace("checks for","")
        outcome = str(check.outcome)
        check_name = check.check_cfg.source_line
        try:
            value = check.dict['diagnostics']['value']
        except:
            value = 0

        metrics = getattr(check, 'metrics', None)

        metrics_list = []
        if metrics:
            for k, v in metrics.items():
                metrics_list.append([k,getattr(v, 'value', v)])
        print('Metrics: ', metrics_list)
        result_type = metrics_list[0][0]
        if outcome.strip() == 'FAIL':
            print(f"Table: {TABLE_NAME} | Check: {check_name} | Failed records: {value}")
            if 'freshness' in check_name:
                value = metrics_list[0][1]
                if value is not None:
                    value = value.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    value = 'NULL values'
        value = str(value)
        result_value = value
        print('All attributes: ')
        print('dataset: ',TABLE_NAME)
        print('Outcome: ',outcome)
        print('Check name: ',check_name)
        print('Result type: ',result_type)
        print('Errors: ',result_value)
        # Все атрибуты объекта
        print(f"\n{'=' * 60}")
        print(f"CHECK #{i} : {check.check_cfg.source_line}")
        print(f"outcome  : {check.outcome}")
        print(f"{'=' * 60}")

        # Ищем значение через metric
        print("\n--- check.metric ---")
        metric = getattr(check, 'metric', None)
        print(f"check.metric           : {metric}")
        print(f"check.metric.value     : {getattr(metric, 'value', 'N/A')}")



        check_status = 1 if outcome =='PASS' else 0
        #result_type = get_result_type(check_name)

        rows.append((TABLE_NAME,check_name,check_status,result_type,result_value,now))

    #import pandas as pd
    #df = pd.DataFrame(rows, columns=['table_name', 'check_name', 'check_status','result_type', 'result_value', 'create_timestamp'])
    #print("The output is:")
    #print(df)
    #return
    # 5. Записать в dq_checks_log
    conn = psycopg2.connect(**DB_CONN)
    cursor = conn.cursor()

    cursor.executemany("""
INSERT INTO dq_checks_log
(table_name, check_name, check_status,
result_type, result_value, create_timestamp)
VALUES (%s, %s, %s, %s, %s, %s)
""",rows
)

    conn.commit()
    cursor.close()
    conn.close()

    # 6. Вывод общей статистики
    passed = sum(r[2] for r in rows)
    failed = len(rows) - passed

    print(f"\n{'='*60}")
    print(f"Total checks : {len(rows)}")
    print(f"Passed : {passed}")
    print(f"Failed : {failed}")
    print(f"Logged to : dq_checks_log")
    print(f"{'='*60}\n")


# ------- DAG

default_args = {"owner": "airflow",
"retries": 1,
"retry_delay": timedelta(minutes=5),
"email_on_failure": False,
}

with DAG(
    dag_id="run_soda_data_quality_checks",
    description="Data Quality checks for entities with logging",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["data_quality", "soda", "entities"],
) as dag:

    soda_check = PythonOperator(
        task_id="run_soda_checks_and_log",
        python_callable=run_soda_and_log,
    )
