from sqlalchemy import Column, Date, Float, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from .conect_supabase_db import Base


class clientes(Base):
    __tablename__ = "clientes"

    id = Column(Integer, primary_key=True)
    nome = Column(String(255), nullable=False)
    sobrenome = Column(String(255), nullable=False)
    id_genero = Column(Integer, ForeignKey("generocliente.id"), nullable=False)
    id_estadocivil = Column(Integer, ForeignKey("estadocivil.id"), nullable=False)
    email = Column(String(255), nullable=False, unique=True)
    cpf = Column(String(14), nullable=False, unique=True)
    telefone = Column(String(25))
    endereco = Column(String(255))
    bairro = Column(String(255))
    cidade = Column(String(255))
    estado = Column(String(50))
    cep = Column(String(10))
    data_cadastro = Column(Date, nullable=False)
    id_emailmarketing = Column(Integer, ForeignKey("emailmarketing.id"), nullable=False)

    pedidos = relationship("pedidos", back_populates="cliente")
    generocliente = relationship("generocliente", back_populates="cliente")
    estadocivil = relationship("estadocivil", back_populates="cliente")
    emailmarketing = relationship("emailmarketing", back_populates="cliente")


class produtos(Base):
    __tablename__ = "produtos"

    id = Column(Integer, primary_key=True)
    sku = Column(String(50), nullable=False, unique=True)
    nome = Column(String(255), nullable=False)
    descricao = Column(String(255))
    preco = Column(Float, nullable=False)
    preco_custo = Column(Float, nullable=False)
    margem = Column(Float, nullable=False)
    tamanho = Column(String(20))
    estoque = Column(Integer, nullable=False)
    id_categoria = Column(Integer, ForeignKey("categorias.id"), nullable=False)
    id_marca = Column(Integer, ForeignKey("marcas.id"), nullable=False)
    modelo = Column(String(50))
    id_genero = Column(Integer, ForeignKey("generoproduto.id"), nullable=False)
    cor = Column(String(20))

    pedidos = relationship("pedidos", back_populates="produto")
    itens = relationship("itens_pedido", back_populates="produto")
    categoria = relationship("categorias", back_populates="produto")
    generoproduto = relationship("generoproduto", back_populates="produto")
    marca = relationship("marcas", back_populates="produto")


class pedidos(Base):
    __tablename__ = "pedidos"

    id = Column(Integer, primary_key=True)
    id_cliente = Column(Integer, ForeignKey("clientes.id"), nullable=False)
    id_produto = Column(Integer, ForeignKey("produtos.id"), nullable=False)
    quantidade = Column(Integer, nullable=False)
    subtotal = Column(Float, nullable=False)
    data_pedido = Column(Date, nullable=False)
    id_canalvenda = Column(Integer, ForeignKey("canalvenda.id"), nullable=False)
    frete = Column(Float, nullable=False)
    valor_desconto = Column(Float, nullable=False)
    total = Column(Float, nullable=False)
    id_forma_pagamento = Column(
        Integer,
        ForeignKey("formapagamento.id"),
        nullable=False,
    )
    id_status = Column(Integer, ForeignKey("status.id"), nullable=False)
    endereco_entrega = Column(String(500), nullable=False)
    data_envio = Column(Date)
    data_entrega = Column(Date)
    id_entregue = Column(Integer, ForeignKey("entregue.id"), nullable=False)

    cliente = relationship("clientes", back_populates="pedidos")
    produto = relationship("produtos", back_populates="pedidos")
    itens = relationship("itens_pedido", back_populates="pedido")
    status = relationship("status", back_populates="pedido")
    formapagamento = relationship("formapagamento", back_populates="pedido")
    canalvenda = relationship("canalvenda", back_populates="pedido")
    entregue = relationship("entregue", back_populates="pedido")


class status(Base):
    __tablename__ = "status"

    id = Column(Integer, primary_key=True)
    nome = Column(String(50), nullable=False, unique=True)

    pedido = relationship("pedidos", back_populates="status")


class formapagamento(Base):
    __tablename__ = "formapagamento"

    id = Column(Integer, primary_key=True)
    nome = Column(String(50), nullable=False, unique=True)

    pedido = relationship("pedidos", back_populates="formapagamento")


class canalvenda(Base):
    __tablename__ = "canalvenda"

    id = Column(Integer, primary_key=True)
    nome = Column(String(50), nullable=False, unique=True)

    pedido = relationship("pedidos", back_populates="canalvenda")


class generocliente(Base):
    __tablename__ = "generocliente"

    id = Column(Integer, primary_key=True)
    nome = Column(String(20), nullable=False, unique=True)

    cliente = relationship("clientes", back_populates="generocliente")


class generoproduto(Base):
    __tablename__ = "generoproduto"

    id = Column(Integer, primary_key=True)
    nome = Column(String(20), nullable=False, unique=True)

    produto = relationship("produtos", back_populates="generoproduto")


class categorias(Base):
    __tablename__ = "categorias"

    id = Column(Integer, primary_key=True)
    nome = Column(String(50), nullable=False, unique=True)

    produto = relationship("produtos", back_populates="categoria")


class marcas(Base):
    __tablename__ = "marcas"

    id = Column(Integer, primary_key=True)
    nome = Column(String(50), nullable=False, unique=True)

    produto = relationship("produtos", back_populates="marca")


class estadocivil(Base):
    __tablename__ = "estadocivil"

    id = Column(Integer, primary_key=True)
    nome = Column(String(20), nullable=False, unique=True)

    cliente = relationship("clientes", back_populates="estadocivil")


class emailmarketing(Base):
    __tablename__ = "emailmarketing"

    id = Column(Integer, primary_key=True)
    nome = Column(String(255), nullable=False, unique=True)

    cliente = relationship("clientes", back_populates="emailmarketing")


class entregue(Base):
    __tablename__ = "entregue"

    id = Column(Integer, primary_key=True)
    nome = Column(String(255), nullable=False, unique=True)

    pedido = relationship("pedidos", back_populates="entregue")


class itenspedido(Base):
    __tablename__ = "itenspedido"

    id = Column(Integer, primary_key=True)
    id_pedido = Column(Integer, ForeignKey("pedidos.id"), nullable=False)
    id_produto = Column(Integer, ForeignKey("produtos.id"), nullable=False)
    quantidade = Column(Integer, nullable=False)
    preco_unitario = Column(Float, nullable=False)
    subtotal = Column(Float, nullable=False)

    pedido = relationship("pedidos", back_populates="itens")
    produto = relationship("produtos", back_populates="itens")
