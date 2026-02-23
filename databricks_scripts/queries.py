class LakehouseQueries:
    """Queries para acessar dados das camadas silver"""

    @staticmethod
    def get_kpis_executive(data_inicio, data_fim):
        """KPIs principais para o dashboard executivo"""
        return f"""
        WITH kpis AS (
            SELECT
                COUNT(DISTINCT id_cliente) AS total_clientes,
                COUNT(DISTINCT id_pedido) AS total_pedidos,
                SUM(total_pedido) as receita_total,
                AVG(total_pedido) as ticket_medio,
                SUM(CASE WHEN status_pedido = "Entregue" THEN 1 ELSE 0 END) AS pedidos_entregues,
                COUNT(DISTINCT CASE WHEN data_pedido >= DATE_ADD(CURRENT_DATE, -90) THEN id_cliente END) as clientes_ativos_30d,
                SUM(CASE WHEN data_pedido >= DATE_ADD(CURRENT_DATE, -90) THEN total_pedido ELSE 0 END) as receita_30d
            FROM lakehouse.silver.fact_pedido
            WHERE data_pedido BETWEEN '{data_inicio}' AND '{data_fim}'
        ),
        kpis_anterior AS (
            SELECT
                SUM(total_pedido) as receita_periodo_anterior,
                AVG(total_pedido) as ticket_medio_anterior
            FROM lakehouse.silver.fact_pedido
            WHERE data_pedido BETWEEN
                DATE_ADD('{data_inicio}', -90) AND
                DATE_ADD('{data_fim}', -90)
        )
        SELECT
            k.*,
            ka.receita_periodo_anterior,
            ka.ticket_medio_anterior,
            CASE
                WHEN ka.receita_periodo_anterior > 0
                THEN ((k.receita_30d - ka.receita_periodo_anterior) / ka.receita_periodo_anterior * 100)
                ELSE 0
            END as variacao_receita,
            CASE
                WHEN ka.ticket_medio_anterior > 0
                THEN ((k.ticket_medio - ka.ticket_medio_anterior) / ka.ticket_medio_anterior * 100)
                ELSE 0
            END as variacao_ticket
        FROM kpis k
        CROSS JOIN kpis_anterior ka
        """

    @staticmethod
    def get_vendas_por_dia(data_inicio, data_fim):
        """Vendas diárias para gráfico de tendência"""
        return f"""
        SELECT
            DATE(data_pedido) as data,
            COUNT(DISTINCT id_pedido) as total_pedidos,
            COUNT(DISTINCT id_cliente) as clientes_unicos,
            SUM(total_pedido) as receita_total,
            AVG(total_pedido) as ticket_medio_dia,
            SUM(quantidade) as itens_vendidos
        FROM lakehouse.silver.fact_pedido
        WHERE data_pedido BETWEEN '{data_inicio}' AND '{data_fim}'
        GROUP BY DATE(data_pedido)
        ORDER BY data
        """

    @staticmethod
    def get_top_categorias(data_inicio, data_fim, limite=10):
        """Top categorias por receita"""
        return f"""
        SELECT
            p.categoria,
            COUNT(DISTINCT fp.id_pedido) as total_pedidos,
            SUM(fp.quantidade) as unidades_vendidas,
            SUM(fp.total_pedido) as receita_total,
            AVG(fp.total_pedido) as ticket_medio_categoria,
            SUM(fp.total_pedido) * 100.0 / SUM(SUM(fp.total_pedido)) OVER() as percentual_receita
        FROM lakehouse.silver.fact_pedido fp
        JOIN lakehouse.silver.dim_produto p ON fp.id_produto = p.id_produto
        WHERE fp.data_pedido BETWEEN '{data_inicio}' AND '{data_fim}'
        GROUP BY p.categoria
        ORDER BY receita_total DESC
        LIMIT {limite}
        """

    @staticmethod
    def get_vendas_por_regiao(data_inicio, data_fim):
        """Análise de vendas por região"""
        return f"""
        SELECT
            c.estado,
            c.cidade,
            COUNT(DISTINCT c.id_cliente) as clientes_regiao,
            COUNT(DISTINCT fp.id_pedido) as total_pedidos,
            SUM(fp.total_pedido) as receita_total,
            AVG(fp.total_pedido) as ticket_medio_regiao,
            SUM(fp.quantidade) as unidades_vendidas
        FROM lakehouse.silver.fact_pedido fp
        JOIN lakehouse.silver.dim_cliente c ON fp.id_cliente = c.id_cliente
        WHERE fp.data_pedido BETWEEN '{data_inicio}' AND '{data_fim}'
        GROUP BY c.estado, c.cidade
        ORDER BY receita_total DESC
        """

    @staticmethod
    def get_top_produtos(data_inicio, data_fim, limite=20):
        """Top produtos mais vendidos"""
        return f"""
        SELECT
            p.nome_produto,
            p.categoria,
            p.marca,
            COUNT(DISTINCT fp.id_pedido) as total_pedidos,
            SUM(fp.quantidade) as unidades_vendidas,
            SUM(fp.total_pedido) as receita_total,
            p.estoque as estoque_atual,
            CASE
                WHEN p.estoque > 0
                THEN SUM(fp.quantidade) * 1.0 / p.estoque
                ELSE 0
            END as giro_estoque
        FROM lakehouse.silver.fact_pedido fp
        JOIN lakehouse.silver.dim_produto p ON fp.id_produto = p.id_produto
        WHERE fp.data_pedido BETWEEN '{data_inicio}' AND '{data_fim}'
        GROUP BY p.nome_produto, p.categoria, p.marca, p.estoque
        ORDER BY receita_total DESC
        LIMIT {limite}
        """

    @staticmethod
    def get_analise_canais_venda(data_inicio, data_fim):
        """Performance por canal de venda"""
        return f"""
        SELECT
            canal_venda,
            COUNT(DISTINCT id_pedido) as total_pedidos,
            COUNT(DISTINCT id_cliente) as clientes_unicos,
            SUM(total_pedido) as receita_total,
            AVG(total_pedido) as ticket_medio,
            SUM(quantidade) as itens_vendidos,
            AVG(frete) as frete_medio,
            SUM(CASE WHEN status_entrega = 'Entregue' THEN 1 ELSE 0 END) as entregas_sucesso
        FROM lakehouse.silver.fact_pedido
        WHERE data_pedido BETWEEN '{data_inicio}' AND '{data_fim}'
        GROUP BY canal_venda
        ORDER BY receita_total DESC
        """

    @staticmethod
    def get_tempo_medio_entrega(data_inicio, data_fim):
        """Tempo médio de entrega por canal"""
        return f"""
        SELECT
            canal_venda,
            AVG(DATEDIFF(data_entrega, data_pedido)) as dias_medios_entrega,
            AVG(CASE WHEN status_pedido = 'Entregue' THEN DATEDIFF(data_entrega, data_pedido) END) as dias_medios_entrega_sucesso,
            COUNT(*) as total_entregas,
            COUNT(CASE WHEN status_pedido = 'Entregue' THEN 1 END) as entregas_concluidas
        FROM lakehouse.silver.fact_pedido
        WHERE data_pedido BETWEEN '{data_inicio}' AND '{data_fim}'
            AND data_entrega IS NOT NULL
        GROUP BY canal_venda
        ORDER BY dias_medios_entrega
        """

    @staticmethod
    def get_rfv_analysis(data_inicio=None, data_fim=None, segmento=None):
        """Query principal para análise RFV"""
        filtro_segmento = ""
        if segmento and segmento != "Todos":
            filtro_segmento = f"AND segmento_cliente = '{segmento}'"

        return f"""
        WITH vendas_cliente AS (
    SELECT
        c.id_cliente,
        c.nome_completo,
        c.email,
        c.cidade,
        c.estado,
        c.genero,
        c.data_cadastro,

        MAX(fp.data_pedido)                         AS ultima_compra,
        COUNT(DISTINCT fp.id_pedido)               AS frequencia_compras,
        COALESCE(SUM(fp.total_pedido), 0)          AS valor_total_gasto,
        AVG(fp.total_pedido)                       AS ticket_medio,
        COALESCE(SUM(fp.quantidade), 0)            AS total_itens_comprados,

        DATEDIFF('{data_fim}', MAX(fp.data_pedido)) AS dias_ultima_compra,
        DATEDIFF('{data_fim}', c.data_cadastro)     AS dias_desde_cadastro

    FROM lakehouse.silver.dim_cliente c
    LEFT JOIN lakehouse.silver.fact_pedido fp
        ON c.id_cliente = fp.id_cliente
       AND fp.data_pedido BETWEEN '{data_inicio}' AND '{data_fim}'

    GROUP BY
        c.id_cliente,
        c.nome_completo,
        c.email,
        c.cidade,
        c.estado,
        c.genero,
        c.data_cadastro
),

rfv_calculos AS (
    SELECT
        *,

        CASE
            WHEN dias_ultima_compra <= 30  THEN 5
            WHEN dias_ultima_compra <= 60  THEN 4
            WHEN dias_ultima_compra <= 90  THEN 3
            WHEN dias_ultima_compra <= 180 THEN 2
            ELSE 1
        END AS score_recencia,

        CASE
            WHEN frequencia_compras >= 10 THEN 5
            WHEN frequencia_compras >= 5  THEN 4
            WHEN frequencia_compras >= 3  THEN 3
            WHEN frequencia_compras >= 1  THEN 2
            ELSE 1
        END AS score_frequencia,

        CASE
            WHEN valor_total_gasto >= 5000 THEN 5
            WHEN valor_total_gasto >= 2000 THEN 4
            WHEN valor_total_gasto >= 1000 THEN 3
            WHEN valor_total_gasto >= 500  THEN 2
            ELSE 1
        END AS score_valor

    FROM vendas_cliente
),

rfv_final AS (
    SELECT
        *,

        (score_recencia + score_frequencia + score_valor) AS rfv_score_total,

        CASE
            WHEN score_recencia = 5 AND score_frequencia >= 4 AND score_valor >= 4 THEN '🏆 Campeões'
            WHEN score_recencia >= 4 AND score_frequencia >= 4 AND score_valor >= 4 THEN '💎 Clientes VIP'
            WHEN score_recencia >= 4 AND score_frequencia >= 3 AND score_valor >= 3 THEN '⭐ Clientes Fieis'
            WHEN score_recencia >= 3 AND score_frequencia >= 2 AND score_valor >= 2 THEN '📈 Potencial Crescimento'
            WHEN score_recencia <= 2 AND score_frequencia >= 3 AND score_valor >= 3 THEN '⚠️ Em Risco'
            WHEN score_recencia <= 2 AND score_frequencia >= 2 AND score_valor >= 2 THEN '😴 Hibernando'
            WHEN score_recencia <= 1 AND frequencia_compras = 0 THEN '🆕 Novo Cliente'
            WHEN score_recencia <= 1 AND frequencia_compras = 1 THEN '🔁 Uma Compra'
            WHEN score_recencia >= 3 AND frequencia_compras <= 1 AND valor_total_gasto <= 500 THEN '🌱 Cliente Inicial'
            ELSE '📊 Cliente Regular'
        END AS segmento_cliente,

        CASE
            WHEN score_recencia = 5 AND score_frequencia >= 4 AND score_valor >= 4
                THEN 'Programa de fidelidade VIP, acesso antecipado a lançamentos'
            WHEN score_recencia >= 4 AND score_frequencia >= 4 AND score_valor >= 4
                THEN 'Ofertas exclusivas, programa de indicação'
            WHEN score_recencia >= 4 AND score_frequencia >= 3 AND score_valor >= 3
                THEN 'Cross-sell e upsell, newsletter personalizada'
            WHEN score_recencia >= 3 AND score_frequencia >= 2 AND score_valor >= 2
                THEN 'Campanhas de reengajamento, cupons de desconto'
            WHEN score_recencia <= 2 AND score_frequencia >= 3 AND score_valor >= 3
                THEN 'Recuperação urgente, ofertas especiais'
            WHEN score_recencia <= 2 AND score_frequencia >= 2 AND score_valor >= 2
                THEN 'Email marketing, pesquisa de satisfação'
            WHEN score_recencia <= 1 AND frequencia_compras = 0
                THEN 'Onboarding, primeiro desconto'
            WHEN score_recencia <= 1 AND frequencia_compras = 1
                THEN 'Cupom para segunda compra, follow-up'
            WHEN score_recencia >= 3 AND frequencia_compras <= 1 AND valor_total_gasto <= 500
                THEN 'Nutrir com conteúdo relevante'
            ELSE 'Manter engajamento regular'
        END AS recomendacao_acao

    FROM rfv_calculos
)

SELECT *
FROM rfv_final
WHERE 1 = 1
  {filtro_segmento}
ORDER BY valor_total_gasto DESC;
        """

    @staticmethod
    def get_status(data_inicio, data_fim):
        """Status do pipeline"""
        return f"""
        SELECT
            status_entrega,
            COUNT(*) as quantidade,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentual,
            AVG(dias_em_transporte) as tempo_medio,
            AVG(frete) as frete_medio
        FROM lakehouse.silver.fact_pedido
        WHERE data_pedido BETWEEN '{data_inicio}' AND '{data_fim}'
        GROUP BY status_entrega
        ORDER BY quantidade DESC
        """

    @staticmethod
    def get_region(data_inicio, data_fim):
        """Vendas por região"""
        return f"""
        SELECT
            c.estado,
            COUNT(*) as total_entregas,
            AVG(fp.dias_em_transporte) as tempo_medio,
            SUM(CASE WHEN fp.dias_em_transporte <= 3 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as taxa_rapida,
            AVG(fp.frete) as frete_medio
        FROM lakehouse.silver.fact_pedido fp
        JOIN lakehouse.silver.dim_cliente c ON fp.id_cliente = c.id_cliente
        WHERE fp.data_pedido BETWEEN '{data_inicio}' AND '{data_fim}'
        AND fp.data_entrega IS NOT NULL
        GROUP BY c.estado
        ORDER BY total_entregas DESC
        """

    @staticmethod
    def get_timeline(data_inicio, data_fim):
        """Timeline de vendas"""
        return f"""
        SELECT
            DATE(data_pedido) as data_pedido,
            AVG(dias_em_transporte) as dias_entrega,
            COUNT(*) as total_entregas_dia,
            AVG(frete) as frete_medio_dia
        FROM lakehouse.silver.fact_pedido
        WHERE data_pedido BETWEEN '{data_inicio}' AND '{data_fim}'
        AND data_entrega IS NOT NULL
        GROUP BY DATE(data_pedido)
        ORDER BY data_pedido
        """
