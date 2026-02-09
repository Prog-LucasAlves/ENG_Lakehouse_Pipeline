from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv
import os

load_dotenv()

# Configurações de conexão com o banco de dados
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

Base = declarative_base()


def get_engine():
    """Cria e retorna uma engine de conexão com o banco de dados."""
    return create_engine(DB_URL, pool_pre_ping=True, pool_recycle=300)


if __name__ == "__main__":
    engine = get_engine()
