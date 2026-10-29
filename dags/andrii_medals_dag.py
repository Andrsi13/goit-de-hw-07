import random
import time
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

CONN_ID = "goit_mysql_db"
RESULTS_TABLE = "andrii.hw_dag_results"
SOURCE_TABLE = "olympic_dataset.athlete_event_results"
MEDALS = ["Bronze", "Silver", "Gold"]


# задачі

def pick_medal():
    medal = random.choice(MEDALS)
    print(f"Обрано медаль: {medal}")
    return medal


def choose_branch(ti):
    medal = ti.xcom_pull(task_ids="pick_medal")
    return f"calc_{medal}"


def generate_delay(params):
    delay = params["delay"]
    print(f"Затримка {delay} секунд...")
    time.sleep(delay)


# DAG

with DAG(
    dag_id="andrii_medal_counter",
    start_date=datetime(2024, 1, 1),
    schedule=None,             
    catchup=False,
    params={"delay": Param(5, type="integer", minimum=0,
                           description="Затримка перед сенсором")},
    tags=["andrii", "hw7"],
) as dag:

    # створення таблиці
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(10),
                count INT,
                created_at DATETIME
            );
        """,
    )

    # випадковий вибір медалі
    pick_medal_op = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_medal,
    )

    # розгалуження
    pick_medal_task = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=choose_branch,
    )

    # задачі підрахунку
    calc_tasks = []
    for medal in MEDALS:
        calc = SQLExecuteQueryOperator(
            task_id=f"calc_{medal}",
            conn_id=CONN_ID,
            sql=f"""
                INSERT INTO {RESULTS_TABLE} (medal_type, count, created_at)
                SELECT '{medal}', COUNT(*), NOW()
                FROM {SOURCE_TABLE}
                WHERE medal = '{medal}';
            """,
        )
        calc_tasks.append(calc)

    #  затримка - запускається, якщо хоча б одна з calc_* успішна
    delay_task = PythonOperator(
        task_id="generate_delay",
        python_callable=generate_delay,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # сенсор - чи найновіший запис не старший за 30 секунд
    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=CONN_ID,
        sql=f"""
            SELECT TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= 30
            FROM {RESULTS_TABLE};
        """,
        mode="poke",
        poke_interval=5,
        timeout=15,
    )

    # Порядок виконання
    create_table >> pick_medal_op >> pick_medal_task >> calc_tasks
    calc_tasks >> delay_task >> check_for_correctness