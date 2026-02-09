from conect_supabase_db import get_engine, Base


# Criar engine e sessão
engine = get_engine()

# Criar todas as tabelas (se não existirem)
Base.metadata.create_all(engine)


def connect_db():
    try:
        with engine.connect():
            print("Conexão com o banco de dados estabelecida com sucesso!")
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")


def create_tables():
    try:
        Base.metadata.create_all(engine)
        print("Tabelas criadas com sucesso!")
    except Exception as e:
        print(f"Erro ao criar tabelas: {e}")


if __name__ == "__main__":
    connect_db()
    create_tables()
