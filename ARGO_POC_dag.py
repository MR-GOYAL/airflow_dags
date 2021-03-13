import time
from datetime import timedelta,datetime
from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'airflow',
}

with DAG(
        dag_id='test_dag_1',
        default_args=args,
        schedule_interval='0 0 * * *',
        start_date=datetime(year=2021,month=3,day=12),
        dagrun_timeout=timedelta(minutes=60)
) as dag:
    end = DummyOperator(
        task_id='end',
    )
    start = DummyOperator(
        task_id='start',
    )
    run_load = BashOperator(
        task_id="run_connect_mysql",
        bash_command="python /usr/local/airflow/code/connect_mysql.py"
    )
    sleep = BashOperator(
        task_id="sleep",
        bash_command="sleep 100"
    )


start >> sleep >> run_load  >> end

if __name__ == "__main__":
    dag.cli()