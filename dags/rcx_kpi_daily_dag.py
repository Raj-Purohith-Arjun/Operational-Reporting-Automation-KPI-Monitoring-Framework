from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator


default_args = {
    "owner": "rcx-ops-analytics",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
}


def choose_publish_path():
    with open("data/processed/publish_decision.json", "r", encoding="utf-8") as f:
        decision = json.load(f)
    return "publish_outputs" if decision.get("publish_allowed") else "skip_publish"


with DAG(
    dag_id="rcx_kpi_daily_monitoring",
    default_args=default_args,
    description="Daily RCX KPI monitoring pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="30 3 * * *",  # daily 03:30 UTC after source lands
    catchup=False,
    max_active_runs=1,
    tags=["rcx", "kpi", "ops"],
) as dag:
    ingest_raw = BashOperator(
        task_id="generate_raw_data",
        bash_command="python src/generate_data.py",
    )

    run_transform = BashOperator(
        task_id="run_pipeline",
        bash_command="python src/pipeline.py",
    )

    branch_publish = BranchPythonOperator(
        task_id="branch_publish_decision",
        python_callable=choose_publish_path,
    )

    publish_outputs = EmptyOperator(task_id="publish_outputs")

    skip_publish = EmptyOperator(task_id="skip_publish")

    # if we skip publish, this task still succeeds but dashboard refresh is intentionally skipped
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    ingest_raw >> run_transform >> branch_publish
    branch_publish >> publish_outputs >> end
    branch_publish >> skip_publish >> end
