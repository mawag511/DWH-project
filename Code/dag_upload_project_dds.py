from datetime import datetime

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook


DAG_ID = "upload_project_dds_and_dm_[HIDDEN_username]"


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 3, 22),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags = ['[HIDDEN_username] - Project']
)
def load_to_layers():
  def execute_function(
    func_name:str,
    dwh_conn_id: str = "[HIDDEN_username]_db"
    ):
      pg_hook = PostgresHook(postgres_conn_id=dwh_conn_id)
      pg_hook.run(sql=f"select * from {func_name}();")
  
  @task_group(group_id="insert_subdimension_tables")
  def insert_subDT():    
    @task
    def insert_dds_credentials():
      execute_function(func_name="project_dds.insert_credentials")
      return True
    
    @task
    def insert_dds_region():
      execute_function(func_name="project_dds.insert_region")
      return True
    
    @task
    def insert_dds_genre():
          execute_function(func_name="project_dds.insert_genre")
          return True
        
    @task
    def insert_dds_publisher():
          execute_function(func_name="project_dds.insert_publisher")
          return True
    
    @task
    def insert_dds_developer():
          execute_function(func_name="project_dds.insert_developer")
          return True
    
    @task
    def insert_dds_reviews():
          execute_function(func_name="project_dds.insert_reviews")
          return True
    
    @task
    def insert_dds_payment_method():
          execute_function(func_name="project_dds.insert_payment_method")
          return True
    
    insert_dds_credentials()
    insert_dds_region()
    insert_dds_payment_method()
    insert_dds_reviews()
    insert_dds_developer()
    insert_dds_publisher()
    insert_dds_genre()
    
  @task_group(group_id = 'insert_dimension_tables')
  def insert_DT():
    @task
    def insert_dds_client():
         execute_function(func_name="project_dds.insert_client")
         return True
    
    @task
    def insert_dds_game():
         execute_function(func_name="project_dds.insert_game")
         return True
    
    insert_dds_client()
    insert_dds_game()

  @task_group(group_id = 'insert_fact_tables')
  def insert_FT():
    @task
    def insert_dds_wishlist():
         execute_function(func_name="project_dds.insert_wishlist")
         return True
  
    @task
    def insert_dds_sales():
         execute_function(func_name="project_dds.insert_sales")
         return True
    
    @task
    def insert_dds_refunds():
         execute_function(func_name="project_dds.insert_refunds")
         return True
    
    @task
    def insert_dds_user_x_game():
         execute_function(func_name="project_dds.insert_user_x_game")
         return True
    
    insert_dds_sales()
    insert_dds_refunds()
    insert_dds_user_x_game()
    insert_dds_wishlist()

  @task_group(group_id = 'insert_data_marts')
  def insert_DM():
    @task
    def insert_dm_gen_data():
         execute_function(func_name="project_dm.insert_user_g_data")
         return True
    
    @task
    def insert_dm_game_owned_data():
         execute_function(func_name="project_dm.insert_g_owned_data")
         return True
    
    insert_dm_gen_data()
    insert_dm_game_owned_data()
   
    
  insert_subDT() >> insert_DT() >> insert_FT() >> insert_DM()

init_dag = load_to_layers()