# Script que realiza a conexão com o banco de dados, verifica se as tabelas existem(Caso não ele cria), e insere os dados.

# Bibliotecas utilizadas
import supabase.conect_supabase_db
from src import list_auxiliar
from supabase.model_supabase_db import (
    clientes,
    produtos,
    pedidos,
    itenspedido,
    status,
    formapagamento,
    canalvenda,
    categorias,
    generocliente,
    generoproduto,
    marcas,
    estadocivil,
    emailmarketing,
    entregue,
)
from supabase.model_supabase_db import Base
from tqdm import tqdm
from faker import Faker
import re
import random
from sqlalchemy import text
import sqlalchemy as sa

# Quantidades
NUM_CLIENTES = random.randint(5, 20)
NUM_PRODUTOS = 200
NUM_PEDIDOS = random.randint(200, 400)

# Dados auxiliares
DDD_BR = list_auxiliar.DDD_BR
MARCAS = list_auxiliar.MARCAS
CATEGORIAS = list_auxiliar.CATEGORIAS
CORES = list_auxiliar.CORES
FORMA_PAGAMENTO = list_auxiliar.FORMA_PAGAMENTO
STATUS_PEDIDO = list_auxiliar.STATUS_PEDIDO
GENEROS_PESSOAS = list_auxiliar.GENEROS_PESSOAS
GENEROS_PRODUTOS = list_auxiliar.GENEROS_PRODUTOS
CANAL_VENDA = list_auxiliar.CANAIS_VENDA
CBE = list_auxiliar.CBE
ESTADO_CIVIL = list_auxiliar.ESTADO_CIVIL
MODELOS = list_auxiliar.MODELOS
EMAIL_MARKETING = list_auxiliar.EMAIL_MARKETING
ENTREGUE = list_auxiliar.ENTREGUE

# Inicializar Faker com localização brasileira
fake = Faker("pt_BR")

# Criar engine e sessão com o banco de dados
get_engine = supabase.conect_supabase_db.get_engine
engine = get_engine()


def drop_tables():
    """Exclui todas as tabelas do banco de dados."""
    with engine.begin() as connection:
        connection.execute(text("DROP TABLE IF EXISTS itenspedido CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS pedidos CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS produtos CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS clientes CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS status CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS formapagamento CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS canalvenda CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS categorias CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS marcas CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS generocliente CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS generoproduto CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS estadocivil CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS emailmarketing CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS entregue CASCADE"))

    print("Tabelas excluídas com sucesso!")

    # Criar todas as tabelas (se não existirem)
    Base.metadata.create_all(engine)


def table_empty(table):
    """Verifica se uma tabela está vazia."""
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
        count = result.scalar()
        return count == 0


def insert_data_assistant():
    """Insere dados auxiliares nas tabelas correspondentes."""

    if not table_empty("status"):
        print("Tabelas auxiliates já populadas")
        return
    else:
        with engine.begin() as connection:
            connection.execute(
                status.__table__.insert(),
                [{"nome": nome} for nome in STATUS_PEDIDO],
            )
            connection.execute(
                formapagamento.__table__.insert(),
                [{"nome": nome} for nome in FORMA_PAGAMENTO],
            )
            connection.execute(
                canalvenda.__table__.insert(),
                [{"nome": nome} for nome in CANAL_VENDA],
            )
            connection.execute(
                generocliente.__table__.insert(),
                [{"nome": nome} for nome in GENEROS_PESSOAS],
            )
            connection.execute(
                categorias.__table__.insert(),
                [{"nome": nome} for nome in CATEGORIAS],
            )
            connection.execute(
                generoproduto.__table__.insert(),
                [{"nome": nome} for nome in GENEROS_PRODUTOS],
            )
            connection.execute(
                marcas.__table__.insert(),
                [{"nome": nome} for nome in MARCAS],
            )
            connection.execute(
                estadocivil.__table__.insert(),
                [{"nome": nome} for nome in ESTADO_CIVIL],
            )
            connection.execute(
                emailmarketing.__table__.insert(),
                [{"nome": nome} for nome in EMAIL_MARKETING],
            )
            connection.execute(
                entregue.__table__.insert(),
                [{"nome": nome} for nome in ENTREGUE],
            )


def insert_data_clientes():
    """Insere dados de clientes no banco de dados."""

    batch = []

    for i in tqdm(range(NUM_CLIENTES), desc="Inserindo clientes"):
        id_genero = random.choices([1, 2, 3], weights=[0.60, 0.36, 0.04], k=1)[0]
        if id_genero == 1:
            nome = fake.first_name_male()
            sobrenome = fake.last_name_male()
        elif id_genero == 2:
            nome = fake.first_name_female()
            sobrenome = fake.last_name_female()
        else:
            nome = fake.first_name_nonbinary()
            sobrenome = fake.last_name_nonbinary()
        id_estadocivil = random.randint(1, len(ESTADO_CIVIL))
        email = (
            re.sub(r"[^a-zA-Z0-9]", "", nome.lower())
            + re.sub(r"[^a-zA-Z0-9]", "", sobrenome.lower())
            + str(random.randint(1000, 9999))
            + "@"
            + fake.free_email_domain()
        )
        cpf = fake.cpf()
        telefone = f"55 ({random.choice(DDD_BR)}) 9 " + fake.numerify(text="####-####")
        endereco = fake.street_address()
        estado = random.choice(list(CBE.keys()))
        cidade = random.choice(list(CBE[estado].keys()))
        bairro = random.choice(CBE[estado][cidade])
        cep = fake.bothify(text="#####-###")
        data_cadastro = fake.date_between(start_date="-2y", end_date="today")
        id_emailmarketing = random.choices([1, 2], weights=[0.70, 0.30], k=1)[0]

        # Batch insert
        batch.append(
            {
                "nome": nome,
                "sobrenome": sobrenome,
                "id_genero": id_genero,
                "id_estadocivil": id_estadocivil,
                "email": email,
                "cpf": cpf,
                "telefone": telefone,
                "endereco": endereco,
                "estado": estado,
                "cidade": cidade,
                "bairro": bairro,
                "cep": cep,
                "data_cadastro": data_cadastro,
                "id_emailmarketing": id_emailmarketing,
            },
        )

        if len(batch) >= 10:
            with engine.begin() as connection:
                connection.execute(clientes.__table__.insert(), batch)
            batch = []

    # 🔥 INSERE O RESTO
    if batch:
        with engine.begin() as connection:
            connection.execute(clientes.__table__.insert(), batch)

    print("Clientes inseridos com sucesso!")


def insert_data_produtos():
    """Insere dados de produtos no banco de dados."""

    if not table_empty("produtos"):
        print("Produtos já existem - pulando carga")
        return
    else:
        print("Populando produtos...")

        batch = []

        for _ in tqdm(range(NUM_PRODUTOS), desc="Inserindo produtos"):
            sku = fake.isbn13(separator="")
            marca = random.choice(MARCAS)
            id_marca = MARCAS.index(marca) + 1
            modelos_marca = MODELOS.get(marca)
            modelo = random.choice(modelos_marca)
            nome = f"{marca} {modelo}"
            descricao = fake.text(max_nb_chars=200)
            preco = round(random.uniform(97, 345), 2)
            margem = random.uniform(0.40, 0.60)
            preco_custo = round(preco * (1 - margem), 2)
            estoque = random.randint(0, 100)
            id_categoria = random.randint(1, len(CATEGORIAS))
            id_genero = random.choices([1, 2, 3], weights=[0.60, 0.36, 0.04], k=1)[0]
            if id_genero == 1:
                tamanho = random.choices(
                    ["37", "38", "39", "40", "41", "42", "43", "44"],
                    weights=[5, 8, 15, 18, 18, 15, 8, 5],
                    k=1,
                )[0]
            elif id_genero == 2:
                tamanho = random.choices(
                    ["34", "35", "36", "37", "38", "39", "40"],
                    weights=[5, 8, 18, 20, 18, 10, 5],
                    k=1,
                )[0]
            else:
                tamanho = random.choices(
                    ["34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44"],
                    weights=[3, 5, 8, 12, 15, 15, 15, 12, 8, 5, 3],
                    k=1,
                )[0]
            cor = random.choice(CORES)

            # Batch insert
            batch.append(
                {
                    "sku": sku,
                    "nome": nome,
                    "descricao": descricao,
                    "preco": preco,
                    "preco_custo": preco_custo,
                    "margem": margem,
                    "tamanho": tamanho,
                    "estoque": estoque,
                    "id_categoria": id_categoria,
                    "id_marca": id_marca,
                    "modelo": modelo,
                    "id_genero": id_genero,
                    "cor": cor,
                },
            )

            if len(batch) >= 10:
                with engine.begin() as connection:
                    connection.execute(produtos.__table__.insert(), batch)
                batch = []

        # 🔥 INSERE O RESTO
        if batch:
            with engine.begin() as connection:
                connection.execute(produtos.__table__.insert(), batch)

        print("Produtos inseridos com sucesso!")


def insert_data_pedidos():
    """Insere dados de pedidos no banco de dados."""

    with engine.begin() as connection:
        result_clientes = connection.execute(clientes.__table__.select()).fetchall()
        result_produtos = connection.execute(produtos.__table__.select()).fetchall()

    ids_clientes = [cliente.id for cliente in result_clientes]
    ids_produtos = [produto.id for produto in result_produtos]

    batch = []

    for _ in tqdm(range(NUM_PEDIDOS), desc="Inserindo pedidos"):
        id_cliente = random.choice(ids_clientes)
        id_produto = random.choice(ids_produtos)
        quantidade = random.randint(1, 2)
        subtotal = round(random.uniform(97, 345), 2)
        data_pedido = fake.date_between(start_date="-1y", end_date="today")
        id_canalvenda = random.randint(1, len(CANAL_VENDA))

        # Se estado do cliente for SP - RJ e grátis outros 12,50
        if result_clientes[ids_clientes.index(id_cliente)].estado in ["SP", "RJ"]:
            frete = 0
        else:
            frete = 12.50

        # Se valor do pedido for maior que 200, desconto de 10%,
        if subtotal > 200:
            valor_desconto = round(subtotal * 0.10, 2)
        else:
            valor_desconto = 0
        total = subtotal + frete - valor_desconto
        id_forma_pagamento = random.randint(1, len(FORMA_PAGAMENTO))
        id_status = random.randint(1, len(STATUS_PEDIDO))

        # Endereço de entrega baseado no cliente do pedido
        endereco_entrega = (
            result_clientes[ids_clientes.index(id_cliente)].endereco
            + ", "
            + result_clientes[ids_clientes.index(id_cliente)].bairro
            + " - "
            + result_clientes[ids_clientes.index(id_cliente)].cidade
            + " - "
            + result_clientes[ids_clientes.index(id_cliente)].estado
        )

        # Se status for entregue, data_entrega é 3 a 7 dias após data_pedido. Status for
        if id_status == 4:  # Entregue
            data_envio = fake.date_between(start_date=data_pedido, end_date="+2d")
            data_entrega = fake.date_between(start_date=data_envio, end_date="+5d")
        elif id_status == 3:  # Enviado (AINDA NÃO ENTREGUE)
            data_envio = fake.date_between(start_date=data_pedido, end_date="+2d")
            data_entrega = None
        elif id_status == 2:  # Processando
            data_envio = None
            data_entrega = None
        elif id_status == 1:  # Pendente
            data_envio = None
            data_entrega = None
        elif id_status == 5:  # Cancelado
            data_envio = None
            data_entrega = None

        # Verifica se o pedido foi entregue
        if id_status == 4:  # Entregue
            id_entregue = 1  # Sim
        else:
            id_entregue = 2  # Não

        batch.append(
            {
                "id_cliente": id_cliente,
                "id_produto": id_produto,
                "quantidade": quantidade,
                "subtotal": subtotal,
                "data_pedido": data_pedido,
                "id_canalvenda": id_canalvenda,
                "frete": frete,
                "valor_desconto": valor_desconto,
                "total": total,
                "id_forma_pagamento": id_forma_pagamento,
                "id_status": id_status,
                "endereco_entrega": endereco_entrega,
                "data_envio": data_envio,
                "data_entrega": data_entrega,
                "id_entregue": id_entregue,
            },
        )

        if len(batch) >= 10:
            with engine.begin() as connection:
                connection.execute(pedidos.__table__.insert(), batch)
            batch = []

    if batch:
        with engine.begin() as connection:
            connection.execute(pedidos.__table__.insert(), batch)

    print("Pedidos inseridos com sucesso!")


def insert_data_itens_pedidos():
    """Insere dados de itens pedidos no banco de dados."""

    with engine.begin() as connection:
        result_pedidos = connection.execute(pedidos.__table__.select()).fetchall()
        result_produtos = connection.execute(produtos.__table__.select()).fetchall()

    ids_pedidos = [pedido.id for pedido in result_pedidos]
    ids_produtos = [produto.id for produto in result_produtos]

    batch = []

    for _ in tqdm(range(NUM_PEDIDOS), desc="Inserindo itens pedidos"):
        id_pedido = random.choice(ids_pedidos)
        id_produto = random.choice(ids_produtos)
        quantidade = random.randint(1, 2)
        preco_unitario = round(random.uniform(97, 345), 2)
        subtotal = round(quantidade * preco_unitario, 2)

        batch.append(
            {
                "id_pedido": id_pedido,
                "id_produto": id_produto,
                "quantidade": quantidade,
                "preco_unitario": preco_unitario,
                "subtotal": subtotal,
            },
        )

        if len(batch) >= 10:
            with engine.begin() as connection:
                connection.execute(itenspedido.__table__.insert(), batch)
            batch = []

    if batch:
        with engine.begin() as connection:
            connection.execute(itenspedido.__table__.insert(), batch)

    print("Itens pedidos inseridos com sucesso!")


def update_pedidos():
    """Atualiza os pedidos com base nos itens dos pedidos."""

    with engine.begin() as connection:
        result_pedidos = connection.execute(pedidos.__table__.select()).fetchall()
        result_itenspedidos = connection.execute(
            itenspedido.__table__.select(),
        ).fetchall()
        result_clientes = connection.execute(clientes.__table__.select()).fetchall()

    pedidos_map = {p.id: p for p in result_pedidos}
    clientes_map = {c.id: c for c in result_clientes}

    itens_por_pedido = {}
    for item in result_itenspedidos:
        itens_por_pedido.setdefault(item.id_pedido, []).append(item)

    stmt = (
        pedidos.__table__.update()
        .where(pedidos.__table__.c.id == sa.bindparam("b_id"))
        .values(
            quantidade=sa.bindparam("quantidade"),
            subtotal=sa.bindparam("subtotal"),
            frete=sa.bindparam("frete"),
            valor_desconto=sa.bindparam("valor_desconto"),
            total=sa.bindparam("total"),
        )
    )

    batch = []

    for id_pedido, pedido in tqdm(pedidos_map.items(), desc="Atualizando pedidos"):
        itens = itens_por_pedido.get(id_pedido)
        if not itens:
            continue

        quantidade_total = sum(i.quantidade for i in itens)
        subtotal_total = sum(i.subtotal for i in itens)

        cliente = clientes_map[pedido.id_cliente]
        frete = 0 if cliente.estado in ["SP", "RJ"] else 12.50

        valor_desconto = round(subtotal_total * 0.10, 2) if subtotal_total > 200 else 0
        total = subtotal_total + frete - valor_desconto

        batch.append(
            {
                "b_id": id_pedido,
                "quantidade": quantidade_total,
                "subtotal": subtotal_total,
                "frete": frete,
                "valor_desconto": valor_desconto,
                "total": total,
            },
        )

        if len(batch) >= 500:
            with engine.begin() as connection:
                connection.execute(stmt, batch)
            batch = []

    if batch:
        with engine.begin() as connection:
            connection.execute(stmt, batch)


if __name__ == "__main__":
    # drop_tables()
    Base.metadata.create_all(engine)
    insert_data_assistant()
    insert_data_clientes()
    insert_data_produtos()
    insert_data_pedidos()
    insert_data_itens_pedidos()
    update_pedidos()
