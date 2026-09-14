# Databricks notebook source
# MAGIC %md
# MAGIC # 02. Tratamento dos dados CNES — Camada Silver
# MAGIC
# MAGIC Os arquivos de 2019–2022 e 2023–2024 têm o mesmo conjunto de informações, porém a nomenclatura das colunas está diferente. O tratamento tem como objetivo unificar esses layouts, adequar os tipos de dados e preparar uma base única para as análises.
# MAGIC

# COMMAND ----------

# Importação das funções e leitura das tabelas Bronze do CNES

from pyspark.sql import functions as F

df_cnes_2019_2022 = spark.table("workspace.bronze.cnes_leitos_2019_2022")
df_cnes_2023_2024 = spark.table("workspace.bronze.cnes_leitos_2023_2024")

# COMMAND ----------

# Comparação dos nomes das colunas entre os dois períodos

print("2019–2022".ljust(35), "|", "2023–2024")
print("-" * 75)

for antiga, nova in zip(
    df_cnes_2019_2022.columns,
    df_cnes_2023_2024.columns
):
    marcador = "OK" if antiga == nova else "DIF"
    print(f"{antiga:<35} | {nova:<35} | {marcador}")

# COMMAND ----------

# Padronização dos nomes das colunas de 2019–2022

mapeamento_colunas = {
    "MOTIVO DESABILITACAO": "MOTIVO_DESABILITACAO",
    "NOME ESTABELECIMENTO": "NOME_ESTABELECIMENTO",
    "RAZAO SOCIAL": "RAZAO_SOCIAL",
    "LEITOS EXISTENTE": "LEITOS_EXISTENTES",
    "LEITOS SUS": "LEITOS_SUS",
    "UTI TOTAL - EXIST": "UTI_TOTAL_EXIST",
    "UTI TOTAL - SUS": "UTI_TOTAL_SUS",
    "UTI ADULTO - EXIST": "UTI_ADULTO_EXIST",
    "UTI ADULTO - SUS": "UTI_ADULTO_SUS",
    "UTI PEDIATRICO - EXIST": "UTI_PEDIATRICO_EXIST",
    "UTI PEDIATRICO - SUS": "UTI_PEDIATRICO_SUS",
    "UTI NEONATAL - EXIST": "UTI_NEONATAL_EXIST",
    "UTI NEONATAL - SUS": "UTI_NEONATAL_SUS",
    "UTI QUEIMADO - EXIST": "UTI_QUEIMADO_EXIST",
    "UTI QUEIMADO - SUS": "UTI_QUEIMADO_SUS",
    "UTI CORONARIANA - EXIST": "UTI_CORONARIANA_EXIST",
    "UTI CORONARIANA - SUS": "UTI_CORONARIANA_SUS"
}

df_cnes_2019_2022_pad = df_cnes_2019_2022

for coluna_antiga, coluna_nova in mapeamento_colunas.items():
    df_cnes_2019_2022_pad = (
        df_cnes_2019_2022_pad
        .withColumnRenamed(coluna_antiga, coluna_nova)
    )

print("Colunas padronizadas.")
print(f"2019–2022: {len(df_cnes_2019_2022_pad.columns)} colunas")
print(f"2023–2024: {len(df_cnes_2023_2024.columns)} colunas")

# COMMAND ----------

# Validação da padronização dos layouts

colunas_iguais = (
    df_cnes_2019_2022_pad.columns
    == df_cnes_2023_2024.columns
)

print(f"Layouts idênticos: {colunas_iguais}")

if not colunas_iguais:
    print("\nDiferenças encontradas:")
    
    for antiga, nova in zip(
        df_cnes_2019_2022_pad.columns,
        df_cnes_2023_2024.columns
    ):
        if antiga != nova:
            print(f"{antiga} != {nova}")

# COMMAND ----------

# Unificação dos dados CNES de 2019 a 2024

df_cnes = (
    df_cnes_2019_2022_pad
    .unionByName(df_cnes_2023_2024)
)

print(f"Total de registros: {df_cnes.count()}")
print(f"Total de colunas: {len(df_cnes.columns)}")

# COMMAND ----------

# Inspeção dos tipos de dados após a unificação

df_cnes.printSchema()

# COMMAND ----------

# Adequação dos tipos de dados e criação das variáveis temporais

df_cnes = (
    df_cnes
    .withColumn("CNES", F.col("CNES").cast("string"))
    .withColumn("CO_CEP", F.col("CO_CEP").cast("string"))
    .withColumn("CO_TIPO_UNIDADE", F.col("CO_TIPO_UNIDADE").cast("string"))
    .withColumn("NATUREZA_JURIDICA", F.col("NATUREZA_JURIDICA").cast("string"))
    .withColumn("ANO", (F.col("COMP") / 100).cast("integer"))
    .withColumn("MES", (F.col("COMP") % 100).cast("integer"))
)

df_cnes.select(
    "COMP",
    "ANO",
    "MES",
    "CNES",
    "CO_TIPO_UNIDADE",
    "NATUREZA_JURIDICA",
    "CO_CEP",
    "LEITOS_EXISTENTES",
    "LEITOS_SUS"
).printSchema()

# COMMAND ----------

# Verificação da permanência mensal dos estabelecimentos no CNES

df_granularidade = (
    df_cnes
    .groupBy("ANO", "CNES")
    .agg(
        F.countDistinct("MES").alias("MESES_COM_REGISTRO")
    )
)

display(
    df_granularidade
    .orderBy("ANO", "CNES")
    .limit(20)
)

# COMMAND ----------

# Distribuição anual da permanência dos estabelecimentos

resumo_permanencia = (
    df_granularidade
    .groupBy("ANO")
    .agg(
        F.sum(
            F.when(F.col("MESES_COM_REGISTRO") == 12, 1).otherwise(0)
        ).alias("CNES_12_MESES"),

        F.sum(
            F.when(F.col("MESES_COM_REGISTRO") < 12, 1).otherwise(0)
        ).alias("CNES_MENOS_12_MESES"),

        F.count("*").alias("TOTAL_CNES")
    )
    .withColumn(
        "PCT_12_MESES",
        F.round(
            F.col("CNES_12_MESES") / F.col("TOTAL_CNES") * 100,
            2
        )
    )
    .orderBy("ANO")
)

display(resumo_permanencia)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definição da referência anual
# MAGIC
# MAGIC A análise mostrou que a maior parte dos estabelecimentos possui registros nas 12 competências anuais.
# MAGIC
# MAGIC Como o objetivo do projeto é analisar a evolução da infraestrutura hospitalar, foi adotada a **competência de dezembro como fotografia anual da oferta**, permitindo comparar a estrutura disponível ao final de cada ano entre 2019 e 2024.
# MAGIC
# MAGIC Essa abordagem evita a soma indevida dos registros mensais, que superestimaria a quantidade de estabelecimentos e leitos.

# COMMAND ----------

# Seleção da competência de dezembro como referência anual

df_cnes_anual = df_cnes.filter(F.col("MES") == 12)

print(f"Registros na base anual: {df_cnes_anual.count()}")

# COMMAND ----------

# Validação das principais medidas de leitos

validacao_leitos = (
    df_cnes_anual
    .groupBy("ANO")
    .agg(
        F.min("LEITOS_EXISTENTES").alias("MIN_LEITOS_EXISTENTES"),
        F.max("LEITOS_EXISTENTES").alias("MAX_LEITOS_EXISTENTES"),
        F.min("LEITOS_SUS").alias("MIN_LEITOS_SUS"),
        F.max("LEITOS_SUS").alias("MAX_LEITOS_SUS"),
        
        F.sum(
            F.when(F.col("LEITOS_EXISTENTES") < 0, 1).otherwise(0)
        ).alias("NEGATIVOS_EXISTENTES"),
        
        F.sum(
            F.when(F.col("LEITOS_SUS") < 0, 1).otherwise(0)
        ).alias("NEGATIVOS_SUS"),
        
        F.sum(
            F.when(
                F.col("LEITOS_SUS") > F.col("LEITOS_EXISTENTES"), 1
            ).otherwise(0)
        ).alias("SUS_MAIOR_EXISTENTE")
    )
    .orderBy("ANO")
)

display(validacao_leitos)

# COMMAND ----------

# Investigação da inconsistência identificada em 2020

inconsistencia_leitos = (
    df_cnes_anual
    .filter(
        F.col("LEITOS_SUS") > F.col("LEITOS_EXISTENTES")
    )
    .select(
        "ANO",
        "COMP",
        "CNES",
        "NOME_ESTABELECIMENTO",
        "UF",
        "MUNICIPIO",
        "DS_TIPO_UNIDADE",
        "LEITOS_EXISTENTES",
        "LEITOS_SUS"
    )
)

display(inconsistencia_leitos)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validação das medidas de leitos
# MAGIC
# MAGIC Não foram identificados valores negativos nas variáveis `LEITOS_EXISTENTES` e `LEITOS_SUS`.
# MAGIC
# MAGIC Foi identificado um registro em dezembro de 2020 no qual a quantidade de leitos SUS é superior à quantidade de leitos existentes. O caso corresponde ao Hospital Dr. Oswaldo Diesel, em Três Coroas (RS), com 57 leitos existentes e 66 leitos SUS.
# MAGIC
# MAGIC Por se tratar de uma inconsistência pontual da fonte e não haver informação suficiente para determinar o valor correto, o registro foi preservado sem alteração e a ocorrência foi documentada como regra de qualidade dos dados.

# COMMAND ----------

# Análise de valores nulos na base anual

total_registros = df_cnes_anual.count()

expressoes_nulos = [
    F.sum(
        F.when(F.col(c).isNull(), 1).otherwise(0)
    ).alias(c)
    for c in df_cnes_anual.columns
]

nulos = df_cnes_anual.agg(*expressoes_nulos).collect()[0].asDict()

resultado_nulos = [
    (coluna, quantidade, round(quantidade / total_registros * 100, 2))
    for coluna, quantidade in nulos.items()
    if quantidade > 0
]

df_nulos = spark.createDataFrame(
    resultado_nulos,
    ["COLUNA", "NULOS", "PERCENTUAL_NULOS"]
).orderBy(F.desc("PERCENTUAL_NULOS"))

display(df_nulos)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise de valores nulos
# MAGIC
# MAGIC Os valores nulos identificados estão concentrados em campos administrativos e de contato, sem impacto direto nas variáveis utilizadas para análise da oferta hospitalar.
# MAGIC
# MAGIC O campo `MOTIVO_DESABILITACAO` apresentou 100% de valores nulos. Os campos `NO_COMPLEMENTO`, `NO_EMAIL` e `NU_TELEFONE` apresentaram ausência parcial. Por não serem necessários às análises propostas, esses campos não foram incluídos na base Silver final.
# MAGIC
# MAGIC As variáveis centrais para identificação, localização, classificação dos estabelecimentos e mensuração de leitos não apresentaram valores nulos.

# COMMAND ----------

# Distribuição dos tipos de unidade na base anual

tipos_unidade = (
    df_cnes_anual
    .groupBy("DS_TIPO_UNIDADE")
    .agg(
        F.countDistinct("CNES").alias("ESTABELECIMENTOS")
    )
    .orderBy(F.desc("ESTABELECIMENTOS"))
)

display(tipos_unidade)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recorte dos estabelecimentos hospitalares
# MAGIC
# MAGIC A base de Hospitais e Leitos do CNES inclui diferentes tipos de unidades. Para as análises específicas de infraestrutura hospitalar, foram considerados como hospitais os estabelecimentos classificados como `HOSPITAL GERAL` ou `HOSPITAL ESPECIALIZADO`.
# MAGIC
# MAGIC As categorias `UNIDADE MISTA`, `PRONTO SOCORRO GERAL` e `PRONTO SOCORRO ESPECIALIZADO` foram preservadas na base tratada, porém não foram consideradas no indicador principal de quantidade de hospitais.
# MAGIC
# MAGIC O recorte permite manter uma definição consistente de estabelecimento hospitalar ao longo do período analisado.

# COMMAND ----------

# Classificação dos estabelecimentos utilizados no recorte hospitalar

df_cnes_anual = (
    df_cnes_anual
    .withColumn(
        "FL_HOSPITAL",
        F.when(
            F.col("DS_TIPO_UNIDADE").isin(
                "HOSPITAL GERAL",
                "HOSPITAL ESPECIALIZADO"
            ),
            1
        ).otherwise(0)
    )
)

# COMMAND ----------

# Seleção das colunas da base Silver final

df_cnes_silver = df_cnes_anual.select(
    "COMP",
    "ANO",
    "MES",
    "CNES",
    "NOME_ESTABELECIMENTO",
    "RAZAO_SOCIAL",
    "REGIAO",
    "UF",
    "MUNICIPIO",
    "TP_GESTAO",
    "CO_TIPO_UNIDADE",
    "DS_TIPO_UNIDADE",
    "NATUREZA_JURIDICA",
    "DESC_NATUREZA_JURIDICA",
    "FL_HOSPITAL",
    "LEITOS_EXISTENTES",
    "LEITOS_SUS",
    "UTI_TOTAL_EXIST",
    "UTI_TOTAL_SUS",
    "UTI_ADULTO_EXIST",
    "UTI_ADULTO_SUS",
    "UTI_PEDIATRICO_EXIST",
    "UTI_PEDIATRICO_SUS",
    "UTI_NEONATAL_EXIST",
    "UTI_NEONATAL_SUS",
    "UTI_QUEIMADO_EXIST",
    "UTI_QUEIMADO_SUS",
    "UTI_CORONARIANA_EXIST",
    "UTI_CORONARIANA_SUS"
)

print(
    f"Base Silver preparada: "
    f"{df_cnes_silver.count()} registros | "
    f"{len(df_cnes_silver.columns)} colunas"
)

# COMMAND ----------

# Criação do schema Silver

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.silver
COMMENT 'Dados tratados, padronizados e preparados para integração e análise.'
""")

# Gravação da tabela Silver do CNES

(
    df_cnes_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.silver.cnes_hospitais_leitos")
)

print("Tabela workspace.silver.cnes_hospitais_leitos criada com sucesso.")

# COMMAND ----------

# Validação final da tabela Silver CNES

validacao_silver_cnes = spark.sql("""
SELECT
    COUNT(*) AS registros,
    COUNT(DISTINCT CNES) AS cnes_distintos_periodo,
    COUNT(DISTINCT CONCAT(CAST(ANO AS STRING), '-', CNES)) AS chaves_ano_cnes,
    MIN(ANO) AS ano_min,
    MAX(ANO) AS ano_max,
    SUM(FL_HOSPITAL) AS registros_hospitalares
FROM workspace.silver.cnes_hospitais_leitos
""")

display(validacao_silver_cnes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo do tratamento — CNES
# MAGIC
# MAGIC Os dados de Hospitais e Leitos do CNES/DATASUS foram tratados e padronizados para o período de 2019 a 2024. Os diferentes layouts encontrados entre 2019–2022 e 2023–2024 foram compatibilizados e unificados em uma única estrutura.
# MAGIC
# MAGIC Devido à granularidade mensal da fonte, foi adotada a competência de dezembro como referência anual da infraestrutura disponível. A base resultante possui granularidade de um registro por estabelecimento CNES por ano, sem duplicidades na chave `ANO + CNES`.
# MAGIC
# MAGIC Para o recorte hospitalar, foram classificados como hospitais os estabelecimentos dos tipos `HOSPITAL GERAL` e `HOSPITAL ESPECIALIZADO`, por meio da variável `FL_HOSPITAL`.
# MAGIC
# MAGIC As validações de qualidade contemplaram tipos de dados, valores nulos, duplicidades e consistência das principais medidas de leitos. A inconsistência pontual identificada nos dados de 2020 foi preservada e documentada, sem alteração do valor original.
# MAGIC
# MAGIC A tabela final `workspace.silver.cnes_hospitais_leitos` possui **42.234 registros e 29 atributos**, abrangendo o período de 2019 a 2024, e está preparada para a etapa de integração com os dados populacionais do IBGE.