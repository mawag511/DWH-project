from datetime import datetime
from airflow.decorators import dag
from airflow.operators.dagrun_operator import TriggerDagRunOperator

DAG_ID = 'project_metadag_[HIDDEN_username]'

@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 3, 22),
    schedule_interval=None, 
    catchup=False,
    is_paused_upon_creation=False,
    tags = ['[HIDDEN_username] - Project']
)

def init_metadag():
    trigger_deploy_dag = TriggerDagRunOperator(
        task_id="trigger_deploy_dag",
        trigger_dag_id="deploy_project_[HIDDEN_username]"
    )
    
    trigger_stg_dag = TriggerDagRunOperator(
        task_id="trigger_stg_dag",
        trigger_dag_id="upload_project_stg_[HIDDEN_username]"
    )

    trigger_ods_dag = TriggerDagRunOperator(
        task_id="trigger_ods_dag",
        trigger_dag_id="upload_project_ods_[HIDDEN_username]"
    )

    trigger_dds_and_dm_dag = TriggerDagRunOperator(
        task_id="trigger_dds_and_dm_dag",
        trigger_dag_id="upload_project_dds_and_dm_[HIDDEN_username]"
    )

   
    trigger_deploy_dag >> trigger_stg_dag >> trigger_ods_dag >> trigger_dds_and_dm_dag

init_metadag()