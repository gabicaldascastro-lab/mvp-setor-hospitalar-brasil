# Databricks notebook source
# MAGIC %md
# MAGIC # 04. Integração CNES + IBGE — Camada Silver

# COMMAND ----------

from pyspark.sql import functions as F

df_cnes = spark.table(
    "workspace.silver.cnes_hospitais_leitos"
)

df_ibge = spark.table(
    "workspace.silver.ibge_populacao_municipal"
)

# COMMAND ----------

# Padronização dos nomes dos municípios para integração

def criar_chave_municipio(coluna):
    return F.regexp_replace(
        F.translate(
            F.upper(F.col(coluna)),
            "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ",
            "AAAAAEEEEIIIIOOOOOUUUUC"
        ),
        r"[^A-Z0-9]",
        ""
    )

df_cnes_integracao = (
    df_cnes
    .withColumn(
        "MUNICIPIO_CHAVE",
        criar_chave_municipio("MUNICIPIO")
    )
)

df_ibge_integracao = (
    df_ibge
    .withColumn(
        "MUNICIPIO_CHAVE",
        criar_chave_municipio("MUNICIPIO")
    )
)

# COMMAND ----------

# Validação da correspondência dos municípios entre CNES e IBGE

municipios_cnes = (
    df_cnes_integracao
    .select("UF", "MUNICIPIO_CHAVE")
    .distinct()
)

municipios_ibge = (
    df_ibge_integracao
    .select("UF", "MUNICIPIO_CHAVE")
    .distinct()
)

municipios_sem_match = (
    municipios_cnes
    .join(
        municipios_ibge,
        on=["UF", "MUNICIPIO_CHAVE"],
        how="left_anti"
    )
)

print(
    f"Municípios distintos no CNES: {municipios_cnes.count()}"
)

print(
    f"Municípios do CNES sem correspondência no IBGE: {municipios_sem_match.count()}"
)

# COMMAND ----------

# Investigação dos municípios do CNES sem correspondência no IBGE

municipios_sem_match_detalhe = (
    df_cnes_integracao
    .select(
        "UF",
        "MUNICIPIO",
        "MUNICIPIO_CHAVE"
    )
    .distinct()
    .join(
        municipios_ibge,
        on=["UF", "MUNICIPIO_CHAVE"],
        how="left_anti"
    )
    .orderBy("UF", "MUNICIPIO")
)

display(municipios_sem_match_detalhe)

# COMMAND ----------

# Identificação dos nomes correspondentes no IBGE

from pyspark.sql import Window

cnes_sem_match = (
    municipios_sem_match_detalhe
    .select(
        "UF",
        F.col("MUNICIPIO").alias("MUNICIPIO_CNES"),
        F.col("MUNICIPIO_CHAVE").alias("CHAVE_CNES")
    )
)

ibge_nomes = (
    df_ibge_integracao
    .select(
        "UF",
        F.col("MUNICIPIO").alias("MUNICIPIO_IBGE"),
        F.col("MUNICIPIO_CHAVE").alias("CHAVE_IBGE")
    )
    .distinct()
)

candidatos = (
    cnes_sem_match
    .join(ibge_nomes, on="UF", how="inner")
    .withColumn(
        "DISTANCIA",
        F.levenshtein(
            F.col("CHAVE_CNES"),
            F.col("CHAVE_IBGE")
        )
    )
)

janela = Window.partitionBy(
    "UF",
    "MUNICIPIO_CNES"
).orderBy("DISTANCIA")

melhores_candidatos = (
    candidatos
    .withColumn(
        "ORDEM",
        F.row_number().over(janela)
    )
    .filter(F.col("ORDEM") <= 3)
    .select(
        "UF",
        "MUNICIPIO_CNES",
        "MUNICIPIO_IBGE",
        "DISTANCIA"
    )
    .orderBy(
        "UF",
        "MUNICIPIO_CNES",
        "DISTANCIA"
    )
)

display(melhores_candidatos)

# COMMAND ----------

# Correspondências de nomenclatura entre CNES e IBGE

equivalencias_municipios = {
    ("BA", "SANTATERESINHA"): "SANTATEREZINHA",
    ("CE", "ITAPAGE"): "ITAPAJE",
    ("MG", "BRASOPOLIS"): "BRAZOPOLIS",
    ("MT", "POXOREO"): "POXOREU",
    ("PA", "ELDORADODOSCARAJAS"): "ELDORADODOCARAJAS",
    ("PB", "SERIDO"): "SAOVICENTEDOSERIDO",
    ("PE", "BELEMDESAOFRANCISCO"): "BELEMDOSAOFRANCISCO",
    ("PE", "IGUARACI"): "IGUARACY",
    ("PE", "LAGOADOITAENGA"): "LAGOADEITAENGA",
    ("RJ", "PARATI"): "PARATY",
    ("RJ", "TRAJANODEMORAIS"): "TRAJANODEMORAES",
    ("RN", "AUGUSTOSEVERO"): "CAMPOGRANDE",
    ("RN", "PRESIDENTEJUSCELINO"): "SERRACAIADA",
    ("SP", "MOJIMIRIM"): "MOGIMIRIM",
    ("SP", "SAOLUISDOPARAITINGA"): "SAOLUIZDOPARAITINGA"
}

for (uf, chave_cnes), chave_ibge in equivalencias_municipios.items():
    df_cnes_integracao = df_cnes_integracao.withColumn(
        "MUNICIPIO_CHAVE",
        F.when(
            (F.col("UF") == uf) &
            (F.col("MUNICIPIO_CHAVE") == chave_cnes),
            chave_ibge
        ).otherwise(F.col("MUNICIPIO_CHAVE"))
    )

# COMMAND ----------

# Validação das correspondências após tratamento das divergências

municipios_cnes = (
    df_cnes_integracao
    .select("UF", "MUNICIPIO_CHAVE")
    .distinct()
)

municipios_ibge = (
    df_ibge_integracao
    .select("UF", "MUNICIPIO_CHAVE")
    .distinct()
)

municipios_sem_match = (
    municipios_cnes
    .join(
        municipios_ibge,
        on=["UF", "MUNICIPIO_CHAVE"],
        how="left_anti"
    )
)

print(f"Municípios distintos no CNES: {municipios_cnes.count()}")
print(f"Municípios do CNES sem correspondência no IBGE: {municipios_sem_match.count()}")

# COMMAND ----------

# Integração dos dados CNES com a população municipal do IBGE

df_integrado = (
    df_cnes_integracao.alias("cnes")
    .join(
        df_ibge_integracao.alias("ibge"),
        on=[
            F.col("cnes.ANO") == F.col("ibge.ANO"),
            F.col("cnes.UF") == F.col("ibge.UF"),
            F.col("cnes.MUNICIPIO_CHAVE") == F.col("ibge.MUNICIPIO_CHAVE")
        ],
        how="left"
    )
    .select(
        "cnes.*",
        F.col("ibge.COD_MUNICIPIO").alias("COD_MUNICIPIO"),
        F.col("ibge.POPULACAO").alias("POPULACAO")
    )
)

print(f"Registros após integração: {df_integrado.count()}")

# COMMAND ----------

# Validação do preenchimento após a integração

validacao_integracao = df_integrado.agg(
    F.count("*").alias("REGISTROS"),
    F.sum(
        F.when(F.col("COD_MUNICIPIO").isNull(), 1).otherwise(0)
    ).alias("SEM_COD_MUNICIPIO"),
    F.sum(
        F.when(F.col("POPULACAO").isNull(), 1).otherwise(0)
    ).alias("SEM_POPULACAO")
)

display(validacao_integracao)

# COMMAND ----------

# Investigação dos registros sem correspondência populacional

registros_sem_populacao = (
    df_integrado
    .filter(F.col("POPULACAO").isNull())
    .select(
        "ANO",
        "UF",
        "MUNICIPIO",
        "MUNICIPIO_CHAVE",
        "CNES",
        "NOME_ESTABELECIMENTO"
    )
    .orderBy(
        "ANO",
        "UF",
        "MUNICIPIO",
        "CNES"
    )
)

display(registros_sem_populacao)

# COMMAND ----------

# Comparação da nomenclatura dos municípios com falha temporal

display(
    df_ibge_integracao
    .filter(
        F.col("UF").isin("MT", "RN", "RR")
    )
    .filter(
        F.col("MUNICIPIO_CHAVE").isin(
            "SANTOANTONIODOLEVERGER",
            "ACU",
            "ARES",
            "SAOLUIZ"
        ) |
        F.col("MUNICIPIO").contains("Leverger") |
        F.col("MUNICIPIO").contains("Açu") |
        F.col("MUNICIPIO").contains("Are") |
        F.col("MUNICIPIO").contains("Luiz")
    )
    .select(
        "ANO",
        "UF",
        "COD_MUNICIPIO",
        "MUNICIPIO",
        "MUNICIPIO_CHAVE",
        "POPULACAO"
    )
    .orderBy("UF", "MUNICIPIO", "ANO")
)

# COMMAND ----------

# Verificação histórica do município de Açu pelo código IBGE

display(
    df_ibge
    .filter(F.col("COD_MUNICIPIO") == "2400208")
    .select(
        "ANO",
        "COD_MUNICIPIO",
        "MUNICIPIO",
        "UF",
        "POPULACAO"
    )
    .orderBy("ANO")
)

# COMMAND ----------

# Construção do mapa entre municípios do CNES e códigos oficiais do IBGE

mapa_municipios_ibge = (
    df_ibge_integracao
    .select(
        "UF",
        "MUNICIPIO_CHAVE",
        "COD_MUNICIPIO"
    )
    .distinct()
)

mapa_cnes_ibge = (
    municipios_cnes
    .join(
        mapa_municipios_ibge,
        on=["UF", "MUNICIPIO_CHAVE"],
        how="left"
    )
    .select(
        "UF",
        "MUNICIPIO_CHAVE",
        "COD_MUNICIPIO"
    )
    .distinct()
)

# COMMAND ----------

# Validação do mapa de códigos municipais

validacao_mapa = (
    mapa_cnes_ibge
    .groupBy("UF", "MUNICIPIO_CHAVE")
    .agg(
        F.countDistinct("COD_MUNICIPIO").alias("QTD_CODIGOS")
    )
)

print(
    "Municípios sem código IBGE:",
    validacao_mapa.filter(F.col("QTD_CODIGOS") == 0).count()
)

print(
    "Municípios associados a mais de um código IBGE:",
    validacao_mapa.filter(F.col("QTD_CODIGOS") > 1).count()
)

# COMMAND ----------

# Associação dos estabelecimentos CNES aos códigos oficiais dos municípios

df_cnes_com_codigo = (
    df_cnes_integracao
    .join(
        mapa_cnes_ibge,
        on=["UF", "MUNICIPIO_CHAVE"],
        how="left"
    )
)

print(f"Registros CNES após associação do código: {df_cnes_com_codigo.count()}")

# COMMAND ----------

# Integração definitiva CNES + população municipal

df_integrado = (
    df_cnes_com_codigo.alias("cnes")
    .join(
        df_ibge.alias("ibge"),
        on=[
            F.col("cnes.ANO") == F.col("ibge.ANO"),
            F.col("cnes.COD_MUNICIPIO") == F.col("ibge.COD_MUNICIPIO")
        ],
        how="left"
    )
    .select(
        "cnes.*",
        F.col("ibge.POPULACAO").alias("POPULACAO")
    )
)

print(f"Registros após integração: {df_integrado.count()}")

# COMMAND ----------

# Validação da integração por código municipal

validacao_integracao = df_integrado.agg(
    F.count("*").alias("REGISTROS"),
    F.sum(
        F.when(F.col("COD_MUNICIPIO").isNull(), 1).otherwise(0)
    ).alias("SEM_COD_MUNICIPIO"),
    F.sum(
        F.when(F.col("POPULACAO").isNull(), 1).otherwise(0)
    ).alias("SEM_POPULACAO")
)

display(validacao_integracao)

# COMMAND ----------

# Validação da granularidade após a integração

validacao_granularidade = (
    df_integrado
    .agg(
        F.count("*").alias("REGISTROS"),
        F.countDistinct("ANO", "CNES").alias("CHAVES_ANO_CNES"),
        F.countDistinct("COD_MUNICIPIO").alias("MUNICIPIOS"),
        F.min("ANO").alias("ANO_MIN"),
        F.max("ANO").alias("ANO_MAX")
    )
)

display(validacao_granularidade)

# COMMAND ----------

# Seleção das colunas finais da base integrada

df_integrado_final = (
    df_integrado
    .drop("MUNICIPIO_CHAVE")
)

print(f"Registros: {df_integrado_final.count()}")
print(f"Colunas: {len(df_integrado_final.columns)}")

# COMMAND ----------

# Gravação da tabela integrada na camada Silver

df_integrado_final.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.cnes_ibge_integrado")

print("Tabela workspace.silver.cnes_ibge_integrado criada com sucesso.")

# COMMAND ----------

# Validação final da tabela Silver integrada

df_integrado_validacao = spark.table(
    "workspace.silver.cnes_ibge_integrado"
)

df_integrado_validacao.agg(
    F.count("*").alias("REGISTROS"),
    F.countDistinct("ANO", "CNES").alias("CHAVES_ANO_CNES"),
    F.countDistinct("COD_MUNICIPIO").alias("MUNICIPIOS"),
    F.sum(F.when(F.col("POPULACAO").isNull(), 1).otherwise(0)).alias("SEM_POPULACAO"),
    F.min("ANO").alias("ANO_MIN"),
    F.max("ANO").alias("ANO_MAX")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo da integração — CNES + IBGE
# MAGIC
# MAGIC Foi feita a integração dos dados de Hospitais e Leitos do CNES e a população municipal do IBGE para o período de 2019 a 2024.
# MAGIC
# MAGIC Como a base CNES não possui o código do município utilizado na base populacional, inicialmente foi criada uma chave textual a partir do nome do município, com padronização de caixa alta, acentuação e caracteres especiais. A correspondência entre as fontes foi realizada considerando conjuntamente UF + MUNICIPIO_CHAVE
# MAGIC
# MAGIC A normalização automática permitiu a correspondência da maior parte dos municípios, sobrando 15 divergências de nomenclatura. Esses casos foram investigados por similaridade textual utilizando distância de Levenshtein e tratados por correspondências controladas, evitando associações automáticas incorretas.
# MAGIC
# MAGIC Durante a validação temporal foram identificadas ainda diferenças de denominação para um mesmo município entre os anos das próprias bases do IBGE, como `Assú` e `Açu`, `Arez` e `Arês`, além de outras alterações de nomenclatura.
# MAGIC
# MAGIC Diante dessas variações, a chave textual foi utilizada apenas para identificar inicialmente a correspondência entre os municípios das duas fontes. Após essa identificação, o `COD_MUNICIPIO` oficial do IBGE foi adotado como chave estável para a integração temporal da população, utilizando `ANO + COD_MUNICIPIO`.
# MAGIC
# MAGIC A integração final preservou a granularidade original da base CNES, sem criação de duplicidades e sem perda de informações populacionais.
# MAGIC
# MAGIC A tabela `workspace.silver.cnes_ibge_integrado` possui **42.234 registros**, com **42.234 chaves únicas `ANO + CNES`**, abrangendo **3.647 municípios entre 2019 e 2024**, sem registros com população ausente.
# MAGIC
# MAGIC A base integrada está preparada para a construção dos indicadores analíticos na camada Gold.