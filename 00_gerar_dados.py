# Databricks notebook source
# DBTITLE 1,Gerar dados de pedidos
from datetime import datetime, timedelta
import random
import uuid

import pandas as pd

# Reprodutibilidade: os mesmos dados serão gerados em cada execução.
random.seed(42)

NUM_PEDIDOS = 100

status_possiveis = [
    "entregue",
    "cancelado",
    "em_preparo",
    "em_transito",
]

restaurantes = [
    "Burguer House",
    "Pizza Central",
    "Veggie Garden",
    "Sushi Express",
    "Pasta & Cia",
]

pedidos = []

for i in range(NUM_PEDIDOS):
    data_pedido = datetime.now() - timedelta(
        days=random.randint(0, 30),
        hours=random.randint(0, 23),
    )

    pedido = {
        "pedido_id": str(uuid.uuid4()),
        "cliente_id": random.randint(1, 40),
        "restaurante": random.choice(restaurantes),
        "data_pedido": data_pedido,
        "valor_pedido": round(random.uniform(20, 180), 2),
        "status": random.choice(status_possiveis),
        "cpf": f"{random.randint(10000000000, 99999999999)}",
        "email": f"cliente{random.randint(1, 40)}@exemplo.com",
    }

    pedidos.append(pedido)

df_pedidos = pd.DataFrame(pedidos)

print(f"Total de pedidos gerados: {len(df_pedidos)}")
display(df_pedidos.head(10))

# COMMAND ----------

# Aqui começa o seu novo bloco de código



# COMMAND ----------

# Criar uma cópia para preservar os dados originais.
df_pedidos_sujos = df_pedidos.copy()

# Introduzir problemas intencionais em alguns registros.
df_pedidos_sujos.loc[0, "valor_pedido"] = -25.90
df_pedidos_sujos.loc[1, "email"] = None
df_pedidos_sujos.loc[2, "status"] = "desconhecido"
df_pedidos_sujos.loc[3, "restaurante"] = None
df_pedidos_sujos.loc[4, "cliente_id"] = None

# Visualizar os registros afetados.
display(df_pedidos_sujos.head(10))


# COMMAND ----------

print("Pedidos originais:", len(df_pedidos))
print("Pedidos com problemas:", len(df_pedidos_sujos))

print("\nValores negativos:")
print((df_pedidos_sujos["valor_pedido"] < 0).sum())

print("\nE-mails ausentes:")
print(df_pedidos_sujos["email"].isna().sum())

print("\nStatus inválidos:")
print((~df_pedidos_sujos["status"].isin([
    "entregue",
    "cancelado",
    "em_preparo",
    "em_transito"
])).sum())

print("\nRestaurantes ausentes:")
print(df_pedidos_sujos["restaurante"].isna().sum())

print("\nIDs de clientes ausentes:")
print(df_pedidos_sujos["cliente_id"].isna().sum())


# COMMAND ----------

# Define os status aceitos pelo sistema.
status_validos = [
    "entregue",
    "cancelado",
    "em_preparo",
    "em_transito",
]

# Cria uma cópia dos dados com os problemas introduzidos.
df_validacao = df_pedidos_sujos.copy()

# Define as regras de qualidade.
df_validacao["erro_valor"] = (
    df_validacao["valor_pedido"].isna()
    | (df_validacao["valor_pedido"] <= 0)
)

df_validacao["erro_email"] = (
    df_validacao["email"].isna()
    | (df_validacao["email"].str.strip() == "")
)

df_validacao["erro_status"] = (
    ~df_validacao["status"].isin(status_validos)
    | df_validacao["status"].isna()
)

df_validacao["erro_restaurante"] = (
    df_validacao["restaurante"].isna()
    | (df_validacao["restaurante"].str.strip() == "")
)

df_validacao["erro_cliente"] = (
    df_validacao["cliente_id"].isna()
)

# Identifica quais pedidos apresentam pelo menos um erro.
colunas_erros = [
    "erro_valor",
    "erro_email",
    "erro_status",
    "erro_restaurante",
    "erro_cliente",
]

df_validacao["registro_invalido"] = df_validacao[colunas_erros].any(axis=1)

# Separa os registros válidos dos inválidos.
df_pedidos_validos = df_validacao[
    ~df_validacao["registro_invalido"]
].copy()

df_pedidos_invalidos = df_validacao[
    df_validacao["registro_invalido"]
].copy()

print("Pedidos recebidos:", len(df_validacao))
print("Pedidos válidos:", len(df_pedidos_validos))
print("Pedidos inválidos:", len(df_pedidos_invalidos))

display(df_pedidos_invalidos)

# COMMAND ----------

# Mapeia cada regra para uma descrição legível.
mensagens_erros = {
    "erro_valor": "Valor do pedido inválido",
    "erro_email": "E-mail ausente ou vazio",
    "erro_status": "Status inválido",
    "erro_restaurante": "Restaurante ausente ou vazio",
    "erro_cliente": "ID do cliente ausente",
}

# Constrói uma lista de motivos para cada registro.
def identificar_motivos(linha):
    motivos = [
        mensagem
        for coluna, mensagem in mensagens_erros.items()
        if linha[coluna]
    ]
    return motivos

df_validacao["motivos_rejeicao"] = df_validacao.apply(
    identificar_motivos,
    axis=1
)

# Exibe os registros inválidos e seus motivos.
display(
    df_validacao.loc[
        df_validacao["registro_invalido"],
        ["pedido_id", "valor_pedido", "status", "motivos_rejeicao"]
    ]
)

# COMMAND ----------

# Converte o DataFrame Pandas em um Spark DataFrame.
df_bronze = spark.createDataFrame(df_pedidos_sujos)

# Grava os dados brutos na tabela Bronze.
(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("delivery_governance.bronze.pedidos_raw")
)

print("Tabela Bronze criada com sucesso!")


# COMMAND ----------

# Converte os DataFrames validados para Spark.
df_silver_validos = spark.createDataFrame(df_pedidos_validos)

df_silver_rejeitados = spark.createDataFrame(
    df_validacao[df_validacao["registro_invalido"]].copy()
)

# Grava os pedidos válidos na camada Silver.
(
    df_silver_validos.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("delivery_governance.silver.pedidos_validos")
)

# Grava os pedidos rejeitados, incluindo os motivos.
(
    df_silver_rejeitados.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("delivery_governance.silver.pedidos_rejeitados")
)

print("Tabelas Silver criadas com sucesso!")

# COMMAND ----------

from pyspark.sql import functions as F

# Lê a tabela Silver com os pedidos aprovados.
df_silver = spark.table(
    "delivery_governance.silver.pedidos_validos"
)

# Calcula os indicadores por restaurante.
df_gold = (
    df_silver
    .groupBy("restaurante")
    .agg(
        F.count("*").alias("total_pedidos"),
        F.round(F.sum("valor_pedido"), 2).alias("faturamento_total"),
        F.round(F.avg("valor_pedido"), 2).alias("ticket_medio"),
        F.sum(
            F.when(F.col("status") == "entregue", 1).otherwise(0)
        ).alias("pedidos_entregues"),
        F.sum(
            F.when(F.col("status") == "cancelado", 1).otherwise(0)
        ).alias("pedidos_cancelados"),
    )
)

# Grava os indicadores na camada Gold.
(
    df_gold.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("delivery_governance.gold.indicadores_delivery")
)

display(df_gold.orderBy(F.desc("faturamento_total")))

# COMMAND ----------

# Testes básicos do pipeline

assert spark.table("delivery_governance.bronze.pedidos_raw").count() == 100, \
    "Erro: a camada Bronze deveria ter 100 pedidos."

assert spark.table("delivery_governance.silver.pedidos_validos").count() == 95, \
    "Erro: a camada Silver deveria ter 95 pedidos válidos."

assert spark.table("delivery_governance.silver.pedidos_rejeitados").count() == 5, \
    "Erro: a camada Silver deveria ter 5 pedidos rejeitados."

assert spark.table("delivery_governance.gold.indicadores_delivery").count() == 5, \
    "Erro: a camada Gold deveria ter 5 restaurantes."

print("Todos os testes básicos passaram!")

# COMMAND ----------

# Teste de integridade: nenhum pedido pode desaparecer

total_bronze = spark.table(
    "delivery_governance.bronze.pedidos_raw"
).count()

total_silver_validos = spark.table(
    "delivery_governance.silver.pedidos_validos"
).count()

total_silver_rejeitados = spark.table(
    "delivery_governance.silver.pedidos_rejeitados"
).count()

assert total_bronze == total_silver_validos + total_silver_rejeitados, \
    "Erro: a quantidade de pedidos não fecha entre Bronze e Silver."

print(f"Bronze: {total_bronze}")
print(f"Silver válidos: {total_silver_validos}")
print(f"Silver rejeitados: {total_silver_rejeitados}")
print("Integridade dos registros confirmada!")

# COMMAND ----------

# Teste de consistência dos indicadores da Gold

from pyspark.sql import functions as F

gold = spark.table("delivery_governance.gold.indicadores_delivery")

# Nenhum indicador pode ser nulo
assert gold.filter(
    F.col("total_pedidos").isNull() |
    F.col("faturamento_total").isNull() |
    F.col("ticket_medio").isNull() |
    F.col("pedidos_entregues").isNull() |
    F.col("pedidos_cancelados").isNull()
).count() == 0, "Erro: existem indicadores nulos."

# A quantidade de pedidos deve ser positiva
assert gold.filter(
    F.col("total_pedidos") <= 0
).count() == 0, "Erro: existe restaurante sem pedidos."

# Entregues + cancelados não podem superar o total
assert gold.filter(
    (F.col("pedidos_entregues") + F.col("pedidos_cancelados"))
    > F.col("total_pedidos")
).count() == 0, "Erro: indicadores de status inconsistentes."

print("Consistência dos indicadores Gold confirmada!")

# COMMAND ----------

# Teste de proteção de dados pessoais

view = spark.table("delivery_governance.gold.pedidos_analiticos")

colunas = view.columns

# A visualização não deve expor CPF e email originais
assert "cpf" not in colunas, \
    "Erro: a coluna CPF original está exposta."

assert "email" not in colunas, \
    "Erro: a coluna de email original está exposta."

# As colunas mascaradas devem existir
assert "cpf_mascarado" in colunas, \
    "Erro: coluna de CPF mascarado não encontrada."

assert "email_mascarado" in colunas, \
    "Erro: coluna de email mascarado não encontrada."

print("Proteção de dados pessoais confirmada!")

# COMMAND ----------

# Teste de reconciliação entre Silver e Gold

total_silver = spark.table(
    "delivery_governance.silver.pedidos_validos"
).count()

total_gold = spark.table(
    "delivery_governance.gold.indicadores_delivery"
).agg(
    F.sum("total_pedidos").alias("total")
).first()["total"]

assert total_silver == total_gold, \
    f"Erro: Silver possui {total_silver} pedidos, mas Gold possui {total_gold}."

print(f"Pedidos válidos na Silver: {total_silver}")
print(f"Pedidos contabilizados na Gold: {total_gold}")
print("Reconciliação entre Silver e Gold confirmada!")

# COMMAND ----------

# Teste de justificativa das rejeições

rejeitados = spark.table(
    "delivery_governance.silver.pedidos_rejeitados"
)

sem_justificativa = rejeitados.filter(
    F.col("motivos_rejeicao").isNull() |
    (F.size(F.col("motivos_rejeicao")) == 0)
).count()

assert sem_justificativa == 0, \
    "Erro: existem pedidos rejeitados sem justificativa."

print("Justificativas das rejeições confirmadas!")

# COMMAND ----------

# Teste de qualidade dos pedidos aprovados

validos = spark.table(
    "delivery_governance.silver.pedidos_validos"
)

problemas = validos.filter(
    (F.col("valor_pedido") <= 0) |
    F.col("email").isNull() |
    F.col("restaurante").isNull() |
    F.col("cliente_id").isNull()
).count()

assert problemas == 0, \
    f"Erro: {problemas} pedidos inválidos foram aprovados."

print("Qualidade dos pedidos aprovados confirmada!")

# COMMAND ----------

# Teste de status permitidos na Silver

status_validos = [
    "entregue",
    "cancelado",
    "em_preparo",
    "em_transito",
]

validos = spark.table(
    "delivery_governance.silver.pedidos_validos"
)

status_invalidos = validos.filter(
    F.col("status").isNull() |
    (~F.col("status").isin(status_validos))
).count()

assert status_invalidos == 0, \
    f"Erro: existem {status_invalidos} pedidos com status inválido."

print("Status dos pedidos aprovados confirmados!")