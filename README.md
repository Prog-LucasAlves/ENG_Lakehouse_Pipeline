# Lakehouse Pipeline
Projeto de Engenharia de Dados que simula uma arquitetura moderna de Lakehouse para e-commerce, usando Supabase como origem, Fivetran para ingestão e Databricks para processamento e análises.

**Stack principal**
1. **Supabase (Postgres)**: base transacional e origem dos dados.
1. **Fivetran**: ingestão ELT da origem para o Lakehouse.
1. **Databricks**: transformação, modelagem e camadas analíticas.

![](https://github.com/Prog-LucasAlves/ENG_Lakehouse_Pipeline/blob/main/image/projeto.png?raw=true)

---

## Visão geral
Este repositório contém um gerador de dados transacionais em Postgres (Supabase) com tabelas de clientes, produtos e pedidos. A intenção é alimentar um pipeline ELT, onde:
1. Os dados são gerados no Supabase.
1. O Fivetran replica para o ambiente de analytics.
1. O Databricks transforma e publica camadas analíticas.

---

## Estrutura do projeto
- `data_sql.py`: cria tabelas e popula o banco com dados sintéticos.
- `pyproject.toml`: dependências do projeto.
- `README.md`: documentação do projeto.

---

## Pré-requisitos
- Python `>= 3.14`
- Conta no Supabase (Postgres)
- Conta no Fivetran
- Workspace no Databricks

---

## Configuração local
Crie um arquivo `.env` na raiz do projeto com as variáveis de conexão do Postgres (Supabase):

```env
user=SEU_USUARIO
password=SUA_SENHA
host=SEU_HOST
port=5432
dbname=SEU_DB
```

---

## Instalação

1. Instale o `UV` globalmente (se ainda não tiver)
2. Inicie o ambiente virtual e instale dependências:

```bash
uv init    # Inicializa o uv no diretório atual
uv venv    # Cria o ambiente virtual
uv sync    # Instala as dependências do pyproject.toml
```

Documentação do `UV` -> [LINK](https://docs.astral.sh/uv/guides/install-python/)
---

## Como gerar dados no Supabase
Execute o script:

```bash
python data_sql.py
```

O script irá:
1. Dropar e recriar tabelas.
1. Inserir dados sintéticos de clientes, produtos e pedidos.
1. Ajustar pedidos com base nos itens de pedido.

---

## Pipeline com Fivetran + Databricks
### 1. Fivetran
Configure um conector Postgres apontando para o Supabase:
- Host, porta, usuário e senha do seu Supabase.
- Habilite CDC (se disponível) para replicação incremental.

### 2. Databricks
No Databricks, crie camadas típicas:
1. **Bronze**: dados crus replicados pelo Fivetran.
1. **Silver**: dados limpos e normalizados.
1. **Gold**: visões analíticas para BI.

Na pasta `utlis` (arquivo **create_catalog_schemas.ipynb**), tem os codigos para criar o `catalog` de cada camada.

Na pasta `utils` (arquivo **create_grant.ipynb**), tem os códigos para criar as permissões de acesso.

![](https://github.com/Prog-LucasAlves/ENG_Lakehouse_Pipeline/blob/main/image/acesso_aos_dados.png?raw=true)

Exemplo de transformações:
- Normalizar dimensões de clientes e produtos.
- Agregar receita por período, categoria, marca e região.
- Criar métricas de churn e retenção.

---

## Modelo de dados (simplificado)
Tabelas geradas pelo script:
- `clientes`
- `produtos`
- `pedidos`
- `itens_pedido`

---

## Próximos passos sugeridos
1. Criar schemas e notebooks no Databricks.
1. Publicar dashboards em BI (Power BI, Tableau ou Superset).
1. Automatizar a geração de dados e a ingestão via agendamentos.

---

## Licença
Veja [`LICENSE`](https://github.com/Prog-LucasAlves/ENG_Lakehouse_Pipeline/blob/main/LICENSE).
