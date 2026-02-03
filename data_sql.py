import psycopg2
from faker import Faker
import random
from tqdm import tqdm
import logging
from dotenv import load_dotenv
import os
import re

load_dotenv()  # Carregar variáveis de ambiente do arquivo .env


# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurações
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

# Quantidades
NUM_CLIENTES = 30
NUM_PRODUTOS = 5
NUM_PEDIDOS = 80

# Inicializar Faker com localização brasileira
fake = Faker("pt_BR")

# Marcas e categorias para produtos
MARCAS = [
    "Nike",
    "Adidas",
    "Puma",
    "Mizuno",
    "Asics",
    "Vans",
    "Converse",
    "New Balance",
    "Reebok",
    "Olympikus",
]
CATEGORIAS = ["Casual", "Esporte", "Corrida", "Basquete", "Skate", "Social"]
GENEROS = ["Masculino", "Feminino", "Unissex"]
CORES = [
    "Preto/Branco",
    "Azul/Branco",
    "Vermelho/Preto",
    "Preto/Vermelho",
    "Xadrez",
    "Rosa/Branco",
    "Branco/Azul",
    "Marrom",
    "Verde/Preto",
    "Roxo/Branco",
]
METODOS_PAGAMENTO = ["Cartão Crédito", "Cartão Débito", "PIX", "Boleto"]
STATUS_PEDIDO = ["Pendente", "Processando", "Enviado", "Entregue", "Cancelado"]
BAIRROS_BR = [
    "Centro",
    "Jardins",
    "Copacabana",
    "Ipanema",
    "Moema",
    "Barra",
    "Savassi",
    "Batel",
    "Boa Viagem",
    "Jardim Paulista",
    "Vila Madalena",
    "Pinheiros",
    "Perdizes",
    "Lapa",
    "Tijuca",
    "Botafogo",
    "Flamengo",
    "Leblon",
    "Barra da Tijuca",
    "Recreio",
    "Alphaville",
    "Morumbi",
    "Itaim Bibi",
    "Brooklin",
    "Santo Amaro",
    "Consolação",
    "Higienópolis",
    "Vila Mariana",
    "Vila Olímpia",
    "Campo Belo",
    "Grajaú",
    "Casa Verde",
    "São Miguel",
    "Anchieta",
    "Cidade Nova",
    "Parque São Jorge",
    "Vila Prudente",
    "Tatuapé",
    "Penha",
    "Mooca",
    "Brás",
    "Cambuci",
    "Liberdade",
    "Sé",
    "Bom Retiro",
    "Santa Cecília",
    "Higienópolis",
    "Jabaquara",
    "Jardim Ângela",
    "Capão Redondo",
    "Cidade Ademar",
    "Vila Formosa",
    "Itaquera",
    "Ermelino Matarazzo",
    "São Mateus",
    "Guaianases",
    "Iguatemi",
    "Mauá",
    "Ribeirão Pires",
    "Santo André",
    "São Bernardo do Campo",
    "Diadema",
    "Osasco",
    "Carapicuíba",
    "Barueri",
    "Santana de Parnaíba",
]


def conectar_banco():
    """Estabelece conexão com o banco de dados"""
    try:
        conn = psycopg2.connect(
            dbname=DBNAME,
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT,
        )
        return conn
    except Exception as e:
        logger.error(f"Erro ao conectar ao banco de dados: {e}")
        raise


def drop_tables(conn):
    """Remove tabelas se existirem"""
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS itens_pedido;")
        cur.execute("DROP TABLE IF EXISTS pedidos;")
        cur.execute("DROP TABLE IF EXISTS produtos;")
        cur.execute("DROP TABLE IF EXISTS clientes;")
        conn.commit()


def create_and_insert_clientes(conn):
    """Cria tabela clientes e insere dados"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clientes (
                id SERIAL PRIMARY KEY,
                nome VARCHAR(100),
                sobrenome VARCHAR(100),
                email VARCHAR(100) UNIQUE,
                telefone VARCHAR(20),
                endereco VARCHAR(200),
                cidade VARCHAR(100),
                estado VARCHAR(50),
                cep VARCHAR(10),
                data_cadastro TIMESTAMP
            );
        """)
        conn.commit()

        for _ in tqdm(range(NUM_CLIENTES), desc="Inserindo clientes"):
            nome = fake.name()
            sobrenome = fake.last_name()
            # E-mail único para cada cliente nome + sobrenome + número aleatório
            email = (
                re.sub(r"[^a-zA-Z0-9]", "", nome.lower())
                + re.sub(r"[^a-zA-Z0-9]", "", sobrenome.lower())
                + str(random.randint(1000, 9999))
                + "@"
                + fake.domain_name()
            )
            telefone = fake.phone_number()
            endereco = fake.street_address()
            cidade = fake.city()
            estado = fake.state()
            cep = fake.postcode()
            data_cadastro = fake.date_time_between(start_date="-2y", end_date="now")

            cur.execute(
                """
                INSERT INTO clientes (nome, sobrenome, email, telefone, endereco, cidade, estado, cep, data_cadastro)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
                (
                    nome,
                    sobrenome,
                    email,
                    telefone,
                    endereco,
                    cidade,
                    estado,
                    cep,
                    data_cadastro,
                ),
            )
        conn.commit()


def create_and_insert_produtos(conn):
    """Cria tabela produtos e insere dados"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS produtos (
                id SERIAL PRIMARY KEY,
                nome VARCHAR(100),
                descricao TEXT,
                preco DECIMAL(10, 2),
                estoque INT,
                categoria VARCHAR(50),
                marca VARCHAR(50),
                genero VARCHAR(20),
                cor VARCHAR(50)
            );
        """)
        conn.commit()

        for _ in tqdm(range(NUM_PRODUTOS), desc="Inserindo produtos"):
            nome = f"{random.choice(MARCAS)} {fake.word().capitalize()}"
            descricao = fake.text(max_nb_chars=200)
            preco = round(random.uniform(97, 355), 2)
            estoque = random.randint(0, 100)
            categoria = random.choice(CATEGORIAS)
            # Se nome contém marca, atribua a mesma
            if any(marca in nome for marca in MARCAS):
                marca = next(marca for marca in MARCAS if marca in nome)
            else:
                marca = random.choice(MARCAS)
            genero = random.choice(GENEROS)
            cor = random.choice(CORES)

            cur.execute(
                """
                INSERT INTO produtos (nome, descricao, preco, estoque, categoria, marca, genero, cor)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """,
                (nome, descricao, preco, estoque, categoria, marca, genero, cor),
            )
        conn.commit()


def create_and_insert_pedidos(conn):
    """Cria tabela pedidos e insere dados"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pedidos (
                id SERIAL PRIMARY KEY,
                cliente_id INT REFERENCES clientes(id),
                produto_id INT REFERENCES produtos(id),
                quantidade INT,
                preco_total DECIMAL(10, 2),
                data_pedido TIMESTAMP,
                status VARCHAR(50),
                metodo_pagamento VARCHAR(50),
                endereco_entrega VARCHAR(200),
                bairro_entrega VARCHAR(100),
                cidade_entrega VARCHAR(100),
                estado_entrega VARCHAR(50),
                cep_entrega VARCHAR(10)
            );
        """)
        conn.commit()

        for _ in tqdm(range(NUM_PEDIDOS), desc="Inserindo pedidos"):
            cliente_id = random.randint(1, NUM_CLIENTES)
            produto_id = random.randint(1, NUM_PRODUTOS)
            quantidade = random.randint(1, 5)
            preco_total = round(random.uniform(50, 500) * quantidade, 2)
            data_pedido = fake.date_time_between(start_date="-1y", end_date="now")
            status = random.choice(STATUS_PEDIDO)
            metodo_pagamento = random.choice(METODOS_PAGAMENTO)
            endereco_entrega = fake.street_address()
            bairro_entrega = random.choice(BAIRROS_BR)
            cidade_entrega = fake.city()
            estado_entrega = fake.state()
            cep_entrega = fake.postcode()

            cur.execute(
                """
                INSERT INTO pedidos (cliente_id, produto_id, quantidade, preco_total, data_pedido, status, metodo_pagamento,
                endereco_entrega, bairro_entrega, cidade_entrega, estado_entrega, cep_entrega)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
                (
                    cliente_id,
                    produto_id,
                    quantidade,
                    preco_total,
                    data_pedido,
                    status,
                    metodo_pagamento,
                    endereco_entrega,
                    bairro_entrega,
                    cidade_entrega,
                    estado_entrega,
                    cep_entrega,
                ),
            )
        conn.commit()


def create_and_insert_itens_pedido(conn):
    """Cria tabela itens_pedido e insere dados"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS itens_pedido (
                id SERIAL PRIMARY KEY,
                pedido_id INT REFERENCES pedidos(id),
                produto_id INT REFERENCES produtos(id),
                quantidade INT,
                preco_unitario DECIMAL(10, 2)
            );
        """)
        conn.commit()

        for _ in tqdm(range(NUM_PEDIDOS), desc="Inserindo itens de pedidos"):
            pedido_id = random.randint(1, NUM_PEDIDOS)
            produto_id = random.randint(1, NUM_PRODUTOS)
            quantidade = random.randint(1, 2)
            preco_unitario = round(random.uniform(50, 500), 2)

            cur.execute(
                """
                INSERT INTO itens_pedido (pedido_id, produto_id, quantidade, preco_unitario)
                VALUES (%s, %s, %s, %s);
            """,
                (pedido_id, produto_id, quantidade, preco_unitario),
            )
        conn.commit()


# Atulizar pedidos baseado em itens_pedido (tabela pedidos id e tabela itens_pedido pedido_id) as quantidades tem que ser iguais e o preco_total tem que ser a soma do preco_unitario * quantidade de itens_pedido tem que somar o mesmo pedido_id
def update_pedidos(conn):
    """Atualiza tabela pedidos baseado em itens_pedido"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT pedido_id, SUM(quantidade) AS total_quantidade, SUM(preco_unitario * quantidade) AS total_preco
            FROM itens_pedido
            GROUP BY pedido_id;
        """)
        resultados = cur.fetchall()

        for pedido_id, total_quantidade, total_preco in resultados:
            cur.execute(
                """
                UPDATE pedidos
                SET quantidade = %s, preco_total = %s
                WHERE id = %s;
            """,
                (total_quantidade, total_preco, pedido_id),
            )
        conn.commit()


def update_pedidos_adrress(conn):
    """Atualiza endereços de entrega dos pedidos para garantir consistência"""
    with conn.cursor() as cur:
        cur.execute("""
             SELECT p.id, c.endereco, c.cidade, c.estado, c.cep
             FROM pedidos p
             JOIN clientes c ON p.cliente_id = c.id;
         """)
        resultados = cur.fetchall()

        for pedido_id, endereco, cidade, estado, cep in resultados:
            cur.execute(
                """
                 UPDATE pedidos
                 SET endereco_entrega = %s,
                     cidade_entrega = %s,
                     estado_entrega = %s,
                     cep_entrega = %s
                 WHERE id = %s;
             """,
                (endereco, cidade, estado, cep, pedido_id),
            )
        conn.commit()


if __name__ == "__main__":
    conn = conectar_banco()
    drop_tables(conn)
    create_and_insert_clientes(conn)
    create_and_insert_produtos(conn)
    create_and_insert_pedidos(conn)
    create_and_insert_itens_pedido(conn)
    update_pedidos(conn)
    update_pedidos_adrress(conn)
    conn.close()
