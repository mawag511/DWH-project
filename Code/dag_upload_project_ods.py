from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "upload_project_ods_[HIDDEN_username]"

@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 3, 22),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags = ['[HIDDEN_username] - Project']
)
def load_to_ods_layer():

    def upload_table(
        source_conn_id: str,
        source_db: str,
        source_table: str,
        target_table: str,
        source_schema: str,
        dwh_schema: str,
        fields: list,
        dwh_conn_id: str = "[HIDDEN_username]_db",
        dwh_db: str = "[HIDDEN_username]_db"
    ):
        source_db_pg_hook = PostgresHook(
            postgres_conn_id=source_conn_id, schema=source_db
        )
        dwh_pg_hook = PostgresHook(postgres_conn_id=dwh_conn_id, schema=dwh_db)
        query = f"SELECT {', '.join(fields)} FROM {source_schema}.{source_table};"
        curr_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        records = [
            record + (curr_dt,) for record in source_db_pg_hook.get_records(query)
        ]
        fields.append("upload_to_ods_date")
        dwh_pg_hook.insert_rows(
            table=f'"{dwh_schema}"."{target_table}"',
            rows=records,
            target_fields=fields
        )

    @task
    def upload_client():
      upload_table(
        source_conn_id="[HIDDEN_username]_db",
        source_db="[HIDDEN_username]_db",
        source_table="client",
        target_table="client",
        fields=[
                "user_id",
                "full_name",
                "nickname",
                "gender",
                "region",
                "birthday",
                "age",
                "email",
                "password",
                "phone_number",
                "games_owned",
                "last_update",
                "deleted_flg"
            ],
        source_schema="project_stg",
        dwh_schema="project_ods"
      )
      return True
        
    @task
    def upload_game():
      upload_table(
        source_conn_id="[HIDDEN_username]_db",
        source_db="[HIDDEN_username]_db",
        source_table="game",
        target_table="game",
        fields=[
                "game_id",
                "title",
                "game_description",
                "genre_name",
                "release_date",
                "price",
                "publisher",
                "developer",
                "reviews",
                "last_update",
                "deleted_flg"
            ],
        source_schema="project_stg",
        dwh_schema="project_ods"
      )
      return True
      
    @task
    def upload_sales():
      upload_table(
        source_conn_id="[HIDDEN_username]_db",
        source_db="[HIDDEN_username]_db",
        source_table="sales",
        target_table="sales",
        fields=[
                "user_id",
                "game_id",
                "date_bought",
                "payment_method",
                "started_once",
                "played_hours",
                "refunded",
                "date_refunded"
            ],
        source_schema="project_stg",
        dwh_schema="project_ods"
      )
      return True
      
    @task
    def upload_wishlist():
      upload_table(
        source_conn_id="[HIDDEN_username]_db",
        source_db="[HIDDEN_username]_db",
        source_table="wishlist",
        target_table="wishlist",
        fields=[
                "user_id",
                "game_id",
                "date_added",
                "deleted_flg"
            ],
        source_schema="project_stg",
        dwh_schema="project_ods"
      )
      return True
    
    upload_client()
    upload_game()
    upload_sales()
    upload_wishlist()
    
init_dag = load_to_ods_layer()