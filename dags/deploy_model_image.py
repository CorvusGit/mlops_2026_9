"""
DAG: data_pipeline
Description: DAG for processing data with Dataproc and PySpark.
"""

import uuid
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.settings import Session
from airflow.models import Connection, Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.operators.bash import BashOperator

# Общие переменные для вашего облака
YC_ZONE = Variable.get("YC_ZONE")
YC_FOLDER_ID = Variable.get("YC_FOLDER_ID")
YC_SUBNET_ID = Variable.get("YC_SUBNET_ID")
YC_SSH_PUBLIC_KEY = Variable.get("YC_SSH_PUBLIC_KEY")

# Переменные для подключения к Object Storage
S3_ENDPOINT_URL = Variable.get("S3_ENDPOINT_URL")
S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
S3_SECRET_KEY = Variable.get("S3_SECRET_KEY")
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
#S3_INPUT_BUCKET_NAME = Variable.get("S3_INPUT_BUCKET_NAME")
S3_INPUT_DATA_BUCKET = S3_BUCKET_NAME + "/airflow/"     # YC S3 bucket for input data
S3_SRC_BUCKET = S3_BUCKET_NAME[:]                       # YC S3 bucket for pyspark source files
S3_DP_LOGS_BUCKET = S3_BUCKET_NAME + "/airflow_logs/"   # YC S3 bucket for Data Proc logs

# Переменные необходимые для создания Dataproc кластера
DP_SA_AUTH_KEY_PUBLIC_KEY = Variable.get("DP_SA_AUTH_KEY_PUBLIC_KEY")
DP_SA_JSON = Variable.get("DP_SA_JSON")
DP_SA_ID = Variable.get("DP_SA_ID")
DP_SECURITY_GROUP_ID = Variable.get("DP_SECURITY_GROUP_ID")

# для сохранения и чтения точек отсчёта следующей партии
PERSIST_BUCKET_NAME = Variable.get("PERSIST_BUCKET_NAME")
PERSIST_BUCKET_KEY = Variable.get("PERSIST_BUCKET_KEY")
PERSIST_BUCKET_SECRET = Variable.get("PERSIST_BUCKET_SECRET")

# Создание подключения для Object Storage
YC_S3_CONNECTION = Connection(
    conn_id="yc-s3",
    conn_type="s3",
    host=S3_ENDPOINT_URL,
    extra={
        "aws_access_key_id": S3_ACCESS_KEY,
        "aws_secret_access_key": S3_SECRET_KEY,
        "host": S3_ENDPOINT_URL,
    },
)

# Создание подключения для Dataproc
YC_SA_CONNECTION = Connection(
    conn_id="yc-sa",
    conn_type="yandexcloud",
    extra={
        "extra__yandexcloud__public_ssh_key": DP_SA_AUTH_KEY_PUBLIC_KEY,
        "extra__yandexcloud__service_account_json": DP_SA_JSON,
    },
)

# Проверка наличия подключений в Airflow
# Если подключения отсутствуют, то они добавляются
# и сохраняются в базе данных Airflow
# Подключения используются для доступа к Object Storage и Dataproc
def setup_airflow_connections(*connections: Connection) -> None:
    """
    Check and add missing connections to Airflow.

    Parameters
    ----------
    *connections : Connection
        Variable number of Airflow Connection objects to verify and add

    Returns
    -------
    None
    """
    session = Session()
    try:
        for conn in connections:
            print("Checking connection:", conn.conn_id)
            if not session.query(Connection).filter(Connection.conn_id == conn.conn_id).first():
                session.add(conn)
                print("Added connection:", conn.conn_id)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


# Функция для выполнения setup_airflow_connections в рамках оператора
def run_setup_connections(**kwargs): # pylint: disable=unused-argument
    """Создает подключения внутри оператора"""
    setup_airflow_connections(YC_S3_CONNECTION, YC_SA_CONNECTION)
    return True


# Настройки DAG
with DAG(
    dag_id="data_pipeline",
    start_date=datetime(year=2026, month=1, day=25),
    schedule_interval=timedelta(minutes=60*24),
    catchup=False,
    max_active_runs=1
) as dag:
    # Задача для создания подключений
    setup_connections = PythonOperator(
        task_id="setup_connections",
        python_callable=run_setup_connections,
    )
# 1. Скачиваем манифест из S3
    sync_manifest = BashOperator(
        task_id='sync_manifest_from_s3',
        bash_command=f"""
                    aws s3 cp s3a://{S3_BUCKET_NAME}/k8s/deployment.yaml /tmp/deployment.yaml && \
                    aws s3 cp s3a://{S3_BUCKET_NAME}/k8s/service.yaml /tmp/service.yaml
                        """
    )

    # 2. Применяем его к кластеру K8s
    # Важно: на воркере Airflow должен быть настроен kubectl (yc managed-kubernetes cluster get-credentials --internal)
    apply_k8s = BashOperator(
    task_id='apply_to_kubernetes',
    bash_command="""
        kubectl apply -f /tmp/deployment.yaml && \
        kubectl apply -f /tmp/service.yaml
    """
)

    sync_manifest >> apply_k8s



