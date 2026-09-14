# Databricks notebook source
# MAGIC %md
# MAGIC # 03. Tratamento dos dados populacionais — Camada Silver

# COMMAND ----------

# Importação das funções e leitura das tabelas Bronze do IBGE

from pyspark.sql import functions as F

df_ibge_2019_2021_2024 = spark.table(
    "workspace.bronze.ibge_populacao_2019_2021_2024"
)

df_ibge_2022 = spark.table(
    "workspace.bronze.ibge_populacao_2022"
)

df_ibge_2023 = spark.table(
    "workspace.bronze.ibge_populacao_2023"
)

# COMMAND ----------

# Inspeção das estruturas das tabelas populacionais

print("IBGE — 2019, 2020, 2021 e 2024")
df_ibge_2019_2021_2024.printSchema()

print("\nIBGE — 2022")
df_ibge_2022.printSchema()

print("\nIBGE — 2023")
df_ibge_2023.printSchema()

# COMMAND ----------

# Tratamento da tabela IBGE 2019, 2020, 2021 e 2024

df_ibge_6579_municipios = (
    df_ibge_2019_2021_2024
    .filter(F.col("NIVEL") == "MU")
    .select(
        F.col("CODIGO").alias("COD_MUNICIPIO"),
        F.col("LOCALIDADE").alias("MUNICIPIO"),
        "POPULACAO_2019",
        "POPULACAO_2020",
        "POPULACAO_2021",
        "POPULACAO_2024"
    )
)

df_ibge_6579_pad = (
    df_ibge_6579_municipios
    .selectExpr(
        "COD_MUNICIPIO",
        "MUNICIPIO",
        """
        stack(
            4,
            2019, POPULACAO_2019,
            2020, POPULACAO_2020,
            2021, POPULACAO_2021,
            2024, POPULACAO_2024
        ) as (ANO, POPULACAO)
        """
    )
)

display(df_ibge_6579_pad.limit(10))

# COMMAND ----------

# Padronização dos municípios — 2019, 2020, 2021 e 2024

df_ibge_6579_pad = (
    df_ibge_6579_pad
    .withColumn(
        "UF",
        F.regexp_extract(F.col("MUNICIPIO"), r"\(([A-Z]{2})\)$", 1)
    )
    .withColumn(
        "MUNICIPIO",
        F.trim(
            F.regexp_replace(
                F.col("MUNICIPIO"),
                r"\s*\([A-Z]{2}\)$",
                ""
            )
        )
    )
    .withColumn(
        "POPULACAO",
        F.expr("try_cast(POPULACAO as long)")
    )
    .filter(F.col("POPULACAO").isNotNull())
    .select(
        "ANO",
        "COD_MUNICIPIO",
        "MUNICIPIO",
        "UF",
        "POPULACAO"
    )
)

display(df_ibge_6579_pad.limit(10))

# COMMAND ----------

# Padronização dos dados populacionais de 2022

df_ibge_2022_pad = (
    df_ibge_2022
    .withColumn(
        "COD_MUNICIPIO",
        F.concat(
            F.col("`COD. UF`"),
            F.lpad(F.col("`COD. MUNIC`"), 5, "0")
        )
    )
    .withColumn(
        "MUNICIPIO",
        F.col("`NOME DO MUNICÍPIO`")
    )
    .withColumn(
        "POPULACAO",
        F.regexp_extract(
            F.col("`POPULAÇÃO`"),
            r"^([\d\.]+)",
            1
        )
    )
    .withColumn(
        "POPULACAO",
        F.regexp_replace(
            F.col("POPULACAO"),
            r"\.",
            ""
        ).cast("long")
    )
    .withColumn(
        "ANO",
        F.lit(2022)
    )
    .select(
        "ANO",
        "COD_MUNICIPIO",
        "MUNICIPIO",
        "UF",
        "POPULACAO"
    )
)

display(df_ibge_2022_pad.limit(10))

# COMMAND ----------

# Padronização dos dados populacionais de 2023

df_ibge_2023_pad = (
    df_ibge_2023
    .filter(
        F.col("UF").rlike(r"^[A-Z]{2}$") &
        F.col("`COD. UF`").rlike(r"^\d{2}$") &
        F.col("`COD. MUNIC`").rlike(r"^\d{5}$")
    )
    .withColumn(
        "COD_MUNICIPIO",
        F.concat(
            F.col("`COD. UF`"),
            F.lpad(F.col("`COD. MUNIC`"), 5, "0")
        )
    )
    .withColumn(
        "MUNICIPIO",
        F.col("`NOME DO MUNICÍPIO`")
    )
    .withColumn(
        "POPULACAO",
        F.col("`POPULAÇÃO`").cast("double").cast("long")
    )
    .withColumn(
        "ANO",
        F.lit(2023)
    )
    .select(
        "ANO",
        "COD_MUNICIPIO",
        "MUNICIPIO",
        "UF",
        "POPULACAO"
    )
)

display(df_ibge_2023_pad.limit(10))

# COMMAND ----------

# Unificação dos dados populacionais de 2019 a 2024

df_ibge_silver = (
    df_ibge_6579_pad
    .unionByName(df_ibge_2022_pad)
    .unionByName(df_ibge_2023_pad)
)

print(f"Total de registros: {df_ibge_silver.count()}")
print(f"Total de colunas: {len(df_ibge_silver.columns)}")

# COMMAND ----------

# Validação da cobertura anual da base populacional

cobertura_anual = (
    df_ibge_silver
    .groupBy("ANO")
    .agg(
        F.count("*").alias("REGISTROS"),
        F.countDistinct("COD_MUNICIPIO").alias("MUNICIPIOS")
    )
    .orderBy("ANO")
)

display(cobertura_anual)

# COMMAND ----------

# Validações de qualidade da base populacional

validacao_ibge = df_ibge_silver.agg(
    F.count("*").alias("REGISTROS"),
    F.countDistinct("ANO", "COD_MUNICIPIO").alias("CHAVES_ANO_MUNICIPIO"),
    F.sum(F.when(F.col("COD_MUNICIPIO").isNull(), 1).otherwise(0)).alias("NULOS_CODIGO"),
    F.sum(F.when(F.col("MUNICIPIO").isNull(), 1).otherwise(0)).alias("NULOS_MUNICIPIO"),
    F.sum(F.when(F.col("UF").isNull(), 1).otherwise(0)).alias("NULOS_UF"),
    F.sum(F.when(F.col("POPULACAO").isNull(), 1).otherwise(0)).alias("NULOS_POPULACAO"),
    F.min("POPULACAO").alias("POPULACAO_MIN"),
    F.max("POPULACAO").alias("POPULACAO_MAX"),
    F.sum(F.when(F.col("POPULACAO") <= 0, 1).otherwise(0)).alias("POPULACAO_NAO_POSITIVA")
)

display(validacao_ibge)

# COMMAND ----------

# Validação da população agregada por UF e ano

validacao_pop_uf = (
    df_ibge_silver
    .groupBy("ANO", "UF")
    .agg(
        F.sum("POPULACAO").alias("POPULACAO_UF")
    )
    .orderBy("ANO", "UF")
)

display(validacao_pop_uf)

# COMMAND ----------

# Persistência da base populacional na camada Silver

df_ibge_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.ibge_populacao_municipal")

print("Tabela workspace.silver.ibge_populacao_municipal criada com sucesso.")

# COMMAND ----------

# Validação final da tabela Silver

df_ibge_final = spark.table(
    "workspace.silver.ibge_populacao_municipal"
)

df_ibge_final.agg(
    F.count("*").alias("REGISTROS"),
    F.countDistinct("ANO").alias("ANOS"),
    F.countDistinct("COD_MUNICIPIO").alias("MUNICIPIOS"),
    F.min("ANO").alias("ANO_MIN"),
    F.max("ANO").alias("ANO_MAX")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo do tratamento — IBGE
# MAGIC
# MAGIC Os dados populacionais do IBGE foram tratados e padronizados para o período de 2019 a 2024. Como os arquivos utilizados apresentavam estruturas distintas, os registros foram convertidos para uma estrutura única composta por `ANO`, `COD_MUNICIPIO`, `MUNICIPIO`, `UF` e `POPULACAO`.
# MAGIC
# MAGIC Na Tabela 6579, utilizada para 2019, 2020, 2021 e 2024, foi identificado o registro de Boa Esperança do Norte (MT) sem informação populacional (`...`). Esses registros foram desconsiderados por não apresentarem valor populacional válido.
# MAGIC
# MAGIC No arquivo de 2023 foram identificadas quatro linhas administrativas ao final da planilha, referentes à fonte, nota metodológica e linhas sem dados municipais. Esses registros foram removidos durante a padronização.
# MAGIC
# MAGIC Após o tratamento, todos os anos apresentaram 5.570 municípios, sem duplicidades na chave `ANO + COD_MUNICIPIO`, valores nulos ou populações não positivas.
# MAGIC
# MAGIC A tabela final `workspace.silver.ibge_populacao_municipal` possui **33.420 registros**, abrangendo **5.570 municípios entre 2019 e 2024**, e está preparada para integração com os dados de Hospitais e Leitos do CNES.