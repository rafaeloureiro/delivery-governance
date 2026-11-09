# Databricks notebook source
# Bibliotecas necessárias para os testes

from pyspark.sql import functions as F

# Carrega as tabelas do projeto
bronze = spark.table(
    "delivery_governance.bronze.pedidos_raw"
)

silver_validos = spark.table(
    "delivery_governance.silver.pedidos_validos"
)

silver_rejeitados = spark.table(
    "delivery_governance.silver.pedidos_rejeitados"
)

gold = spark.table(
    "delivery_governance.gold.indicadores_delivery"
)

pedidos_analiticos = spark.table(
    "delivery_governance.gold.pedidos_analiticos"
)

print("Ambiente de testes preparado!")

# COMMAND ----------

# Teste 1: quantidade de registros

assert bronze.count() == 100, "Bronze deveria ter 100 pedidos."
assert silver_validos.count() == 95, "Silver deveria ter 95 pedidos válidos."
assert silver_rejeitados.count() == 5, "Silver deveria ter 5 pedidos rejeitados."
assert gold.count() == 5, "Gold deveria ter 5 restaurantes."

print("TESTE 1 APROVADO: quantidades corretas em todas as camadas.")

# COMMAND ----------

# Teste 2: integridade entre Bronze e Silver

total_bronze = bronze.count()

total_silver = (
    silver_validos.count()
    + silver_rejeitados.count()
)

assert total_bronze == total_silver, \
    "Existem registros perdidos entre Bronze e Silver."

print(f"Bronze: {total_bronze}")
print(f"Silver: {total_silver}")
print("TESTE 2 APROVADO: integridade dos registros confirmada.")

# COMMAND ----------

# Teste 3: qualidade dos pedidos aprovados

problemas = silver_validos.filter(
    (F.col("valor_pedido") <= 0) |
    F.col("email").isNull() |
    (F.trim(F.col("email")) == "") |
    F.col("restaurante").isNull() |
    (F.trim(F.col("restaurante")) == "") |
    F.col("cliente_id").isNull()
).count()

assert problemas == 0, \
    f"Existem {problemas} pedidos inválidos na Silver."

print("TESTE 3 APROVADO: pedidos válidos passaram pelas regras de qualidade.")

# COMMAND ----------

# Teste 4: status permitidos

status_validos = [
    "entregue",
    "cancelado",
    "em_preparo",
    "em_transito",
]

status_invalidos = silver_validos.filter(
    F.col("status").isNull() |
    (~F.col("status").isin(status_validos))
).count()

assert status_invalidos == 0, \
    f"Existem {status_invalidos} pedidos com status inválido."

print("TESTE 4 APROVADO: todos os status são permitidos.")

# COMMAND ----------

# Teste 5: consistência dos indicadores Gold

indicadores_obrigatorios = [
    "total_pedidos",
    "faturamento_total",
    "ticket_medio",
    "pedidos_entregues",
    "pedidos_cancelados",
]

# Verifica se existem valores nulos
nulos = gold.filter(
    F.expr(
        " OR ".join(
            [f"{coluna} IS NULL" for coluna in indicadores_obrigatorios]
        )
    )
).count()

assert nulos == 0, "Existem indicadores nulos na Gold."

# Verifica se todos os restaurantes possuem pedidos
sem_pedidos = gold.filter(
    F.col("total_pedidos") <= 0
).count()

assert sem_pedidos == 0, "Existe restaurante sem pedidos."

# Verifica a consistência dos status
status_inconsistentes = gold.filter(
    (F.col("pedidos_entregues") + F.col("pedidos_cancelados"))
    > F.col("total_pedidos")
).count()

assert status_inconsistentes == 0, "Existem indicadores inconsistentes."

print("TESTE 5 APROVADO: indicadores Gold consistentes.")

# COMMAND ----------

# Teste 6: reconciliação entre Silver e Gold

total_silver = silver_validos.count()

total_gold = gold.agg(
    F.sum("total_pedidos").alias("total")
).first()["total"]

assert total_silver == total_gold, (
    f"Divergência: Silver possui {total_silver} pedidos "
    f"e Gold contabiliza {total_gold}."
)

print(f"Pedidos na Silver: {total_silver}")
print(f"Pedidos contabilizados na Gold: {total_gold}")
print("TESTE 6 APROVADO: reconciliação confirmada.")

# COMMAND ----------

# Teste 7: justificativas das rejeições

sem_justificativa = silver_rejeitados.filter(
    F.col("motivos_rejeicao").isNull() |
    (F.size(F.col("motivos_rejeicao")) == 0)
).count()

assert sem_justificativa == 0, (
    f"Existem {sem_justificativa} pedidos rejeitados sem justificativa."
)

print("TESTE 7 APROVADO: todas as rejeições possuem justificativa.")

# COMMAND ----------

# Teste 8: proteção de dados pessoais

colunas = pedidos_analiticos.columns

# Verifica se os campos originais não estão expostos
assert "cpf" not in colunas, \
    "Erro: CPF original está exposto."

assert "email" not in colunas, \
    "Erro: email original está exposto."

# Verifica se os campos mascarados existem
assert "cpf_mascarado" in colunas, \
    "Erro: CPF mascarado não encontrado."

assert "email_mascarado" in colunas, \
    "Erro: email mascarado não encontrado."

print("TESTE 8 APROVADO: estrutura de proteção de dados pessoais confirmada.")

# COMMAND ----------

# Teste 9: cálculo correto do ticket médio

gold_comparacao = gold.withColumn(
    "ticket_medio_calculado",
    F.round(
        F.col("faturamento_total") / F.col("total_pedidos"),
        2
    )
)

divergencias = gold_comparacao.filter(
    F.abs(
        F.col("ticket_medio") - F.col("ticket_medio_calculado")
    ) > 0.01
).count()

assert divergencias == 0, (
    f"Existem {divergencias} restaurantes com ticket médio incorreto."
)

print("TESTE 9 APROVADO: cálculo do ticket médio confirmado.")

# COMMAND ----------

# Teste 10: faturamento por restaurante

faturamento_silver = (
    silver_validos
    .groupBy("restaurante")
    .agg(
        F.round(F.sum("valor_pedido"), 2)
        .alias("faturamento_calculado")
    )
)

comparacao = gold.join(
    faturamento_silver,
    on="restaurante",
    how="full_outer"
)

divergencias = comparacao.filter(
    F.col("faturamento_total").isNull() |
    F.col("faturamento_calculado").isNull() |
    (
        F.abs(
            F.col("faturamento_total") -
            F.col("faturamento_calculado")
        ) > 0.01
    )
).count()

assert divergencias == 0, (
    f"Existem {divergencias} divergências de faturamento."
)

print("TESTE 10 APROVADO: faturamento por restaurante confirmado.")

# COMMAND ----------

# Relatório final da suíte de testes

print("=" * 50)
print("RELATÓRIO DE TESTES — DELIVERY GOVERNANCE")
print("=" * 50)

print("Total de testes: 10")
print("Testes aprovados: 10")
print("Testes reprovados: 0")
print("Resultado geral: APROVADO")

print("=" * 50)
