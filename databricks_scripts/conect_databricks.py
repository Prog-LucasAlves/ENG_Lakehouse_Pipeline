import os
from databricks.sql import connect
from dotenv import load_dotenv

load_dotenv()


def connect_to_databricks():
    """Conecta ao Databricks"""
    try:
        connection = connect(
            server_hostname=os.getenv("DATABRICKS_HOST"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            access_token=os.getenv("DATABRICKS_TOKEN"),
            catalog=os.getenv("DATABRICKS_CATALOG"),
            schema=os.getenv("DATABRICKS_SCHEMA"),
        )
        print("Conexão bem-sucedida ao Databricks!")
        return connection
    except Exception as e:
        print(f"Erro ao conectar ao Databricks: {e}")
        raise


if __name__ == "__main__":
    conn = connect_to_databricks()
