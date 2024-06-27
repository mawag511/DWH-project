from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


DAG_ID = "upload_project_stg_[HIDDEN_username]"

@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 3, 22),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags = ['[HIDDEN_username] - Project']
)
def load_to_stg_layer():
  def execute_function(
    proc_name:str,
    dwh_conn_id: str = "[HIDDEN_username]_db"
    ):
      pg_hook = PostgresHook(postgres_conn_id=dwh_conn_id)
      pg_hook.run(sql=f"call {proc_name}();")
  
  @task
  def insert_stg_client():
    execute_function(proc_name="project_stg.client_load")
    return True

  @task
  def insert_stg_game():
    execute_function(proc_name="project_stg.game_load")
    return True
  
  @task
  def insert_stg_sales():
    execute_function(proc_name="project_stg.sales_load")
    return True
  
  @task
  def insert_stg_wishlist():
    execute_function(proc_name="project_stg.wishlist_load")
    return True

  insert_stg_client()
  insert_stg_game()
  insert_stg_sales()
  insert_stg_wishlist()

init_dag = load_to_stg_layer()