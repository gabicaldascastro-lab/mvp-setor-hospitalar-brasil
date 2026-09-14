# Databricks notebook source
# MAGIC %md
# MAGIC # MVP — Expansão e presença do setor hospitalar no Brasil
# MAGIC
# MAGIC ## 01. Ingestão dos dados — Camada Bronze
# MAGIC
# MAGIC Nesta etapa foram ingeridos os dados brutos utilizados no projeto, mantive a estrutura original das fontes para garantir a rastreabilidade do pipeline.
# MAGIC
# MAGIC ### Fontes
# MAGIC
# MAGIC **CNES/DATASUS — Hospitais e Leitos**
# MAGIC - Período: 2019 a 2024
# MAGIC - Abrangência: Brasil
# MAGIC - Arquivos anuais com registros por competência mensal
# MAGIC
# MAGIC **IBGE — População dos Municípios**
# MAGIC - Período: 2019 a 2024
# MAGIC - Abrangência: Brasil
# MAGIC - Dados populacionais em nível municipal
# MAGIC
# MAGIC Os tratamentos, padronizações e integrações entre as fontes serão realizados nas etapas seguintes do pipeline.

# COMMAND ----------

caminho_cnes = "/Volumes/workspace/bronze/dados_brutos/cnes"
caminho_ibge = "/Volumes/workspace/bronze/dados_brutos/ibge"

print("Arquivos CNES:")
for arquivo in dbutils.fs.ls(caminho_cnes):
    print(arquivo.name)

print("\nArquivos IBGE:")
for arquivo in dbutils.fs.ls(caminho_ibge):
    print(arquivo.name)

# COMMAND ----------

# Leitura inicial do arquivo CNES 2019
df_cnes_2019 = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .csv(f"{caminho_cnes}/Leitos_2019.csv")
)

display(df_cnes_2019.limit(10))

# COMMAND ----------

df_cnes_2019.printSchema()

# COMMAND ----------

# Validação da estrutura dos arquivos CNES (2019–2024)

for ano in range(2019, 2025):
    caminho = f"{caminho_cnes}/Leitos_{ano}.csv"

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", ",")
        .csv(caminho)
    )

    print(f"{ano}: {len(df.columns)} colunas")
    print(df.columns)
    print("-" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validação estrutural — CNES
# MAGIC
# MAGIC Os seis arquivos possuem 34 colunas. Porém, foi identificada uma alteração na nomenclatura dos campos. Os arquivos de 2019 a 2022 utilizam espaços e hífens nos nomes das colunas, enquanto 2023 e 2024 utilizam nomes com `_`.
# MAGIC
# MAGIC Os layouts serão preservados na camada Bronze e padronizados posteriormente na Silver.

# COMMAND ----------

# Ingestão CNES 2019–2022
df_cnes_2019_2022 = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .csv([
        f"{caminho_cnes}/Leitos_2019.csv",
        f"{caminho_cnes}/Leitos_2020.csv",
        f"{caminho_cnes}/Leitos_2021.csv",
        f"{caminho_cnes}/Leitos_2022.csv"
    ])
)

# Ingestão CNES 2023–2024
df_cnes_2023_2024 = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .csv([
        f"{caminho_cnes}/Leitos_2023.csv",
        f"{caminho_cnes}/Leitos_2024.csv"
    ])
)

# Remove eventuais tabelas criadas parcialmente em tentativas anteriores
spark.sql("DROP TABLE IF EXISTS workspace.bronze.cnes_leitos_2019_2022")
spark.sql("DROP TABLE IF EXISTS workspace.bronze.cnes_leitos_2023_2024")

# Gravação CNES 2019–2022 preservando os nomes originais das colunas
(
    df_cnes_2019_2022.write
    .format("delta")
    .mode("overwrite")
    .option("delta.columnMapping.mode", "name")
    .option("delta.minReaderVersion", "2")
    .option("delta.minWriterVersion", "5")
    .saveAsTable("workspace.bronze.cnes_leitos_2019_2022")
)

# Gravação CNES 2023–2024
(
    df_cnes_2023_2024.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.bronze.cnes_leitos_2023_2024")
)

print("Tabelas Bronze do CNES criadas com sucesso.")

# COMMAND ----------

# Validação das tabelas Bronze do CNES

validacao_cnes = spark.sql("""
SELECT
    '2019-2022' AS grupo,
    COUNT(*) AS registros,
    MIN(COMP) AS competencia_min,
    MAX(COMP) AS competencia_max
FROM workspace.bronze.cnes_leitos_2019_2022

UNION ALL

SELECT
    '2023-2024' AS grupo,
    COUNT(*) AS registros,
    MIN(COMP) AS competencia_min,
    MAX(COMP) AS competencia_max
FROM workspace.bronze.cnes_leitos_2023_2024
""")

display(validacao_cnes)

# COMMAND ----------

# Verificação dos arquivos IBGE

arquivos_ibge = dbutils.fs.ls(caminho_ibge)

for arquivo in arquivos_ibge:
    print(f"{arquivo.name} | {arquivo.size} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestão — IBGE
# MAGIC
# MAGIC Os dados populacionais estão em formatos diferentes, a ingestão foi realizada na camada bronze respeitando esse formato de origem, posteriormente, serão padronizados para uma estrutura única.

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

import pandas as pd

arquivo_ibge_principal = f"{caminho_ibge}/tabela6579.xlsx"

excel_ibge = pd.ExcelFile(arquivo_ibge_principal)

print("Abas disponíveis:")
print(excel_ibge.sheet_names)

# COMMAND ----------

# Leitura inicial da tabela de população do IBGE
df_ibge_excel = pd.read_excel(
    arquivo_ibge_principal,
    sheet_name="Tabela"
)

print(f"Quantidade de linhas: {len(df_ibge_excel)}")
print(f"Quantidade de colunas: {len(df_ibge_excel.columns)}")

print("\nColunas identificadas:")
print(df_ibge_excel.columns.tolist())

print("\nPrimeiras 10 linhas:")
print(df_ibge_excel.head(10).to_string())

# COMMAND ----------

# MAGIC %pip install xlrd

# COMMAND ----------

# Leitura inicial da população municipal de 2023

arquivo_ibge_2023 = f"{caminho_ibge}/POP_DOU_2023_Municipios_POP2022_Malha2023.xls"

excel_ibge_2023 = pd.ExcelFile(
    arquivo_ibge_2023,
    engine="xlrd"
)

print("Abas disponíveis:")
print(excel_ibge_2023.sheet_names)

# COMMAND ----------

# Inspeção da estrutura do arquivo IBGE 2023

df_ibge_2023 = pd.read_excel(
    arquivo_ibge_2023,
    sheet_name="Municípios",
    engine="xlrd"
)

print(f"Quantidade de linhas: {len(df_ibge_2023)}")
print(f"Quantidade de colunas: {len(df_ibge_2023.columns)}")

print("\nColunas identificadas:")
print(df_ibge_2023.columns.tolist())

print("\nPrimeiras 10 linhas:")
print(df_ibge_2023.head(10).to_string())

# COMMAND ----------

# MAGIC %pip install pymupdf

# COMMAND ----------

import pymupdf

arquivo_ibge_2022 = f"{caminho_ibge}/POP2022_Municipios_Primeiros_Resultados_20231222.pdf"

doc_2022 = pymupdf.open(arquivo_ibge_2022)

print(f"Quantidade de páginas: {len(doc_2022)}")

# Inspeção do texto da primeira página
texto_primeira_pagina = doc_2022[0].get_text()

print(texto_primeira_pagina[:2000])

# COMMAND ----------

# Localizar o início da relação de municípios no PDF

for numero_pagina in range(len(doc_2022)):
    texto = doc_2022[numero_pagina].get_text()

    if "COD. MUNIC" in texto.upper():
        print(f"Cabeçalho municipal encontrado na página {numero_pagina + 1}")
        print(texto[:2500])
        break

# COMMAND ----------

import re
import pandas as pd

registros_2022 = []

# Páginas municipais: da página 2 até o final do documento
for numero_pagina in range(1, len(doc_2022)):
    
    texto = doc_2022[numero_pagina].get_text()
    
    # Cada linha do PDF é recuperada separadamente
    linhas = [linha.strip() for linha in texto.splitlines() if linha.strip()]
    
    # Procura o início de cada registro pela sigla da UF
    i = 0
    
    while i < len(linhas):
        
        # Um registro municipal começa por uma UF com duas letras
        if re.fullmatch(r"[A-Z]{2}", linhas[i]):
            
            # Verifica se as duas posições seguintes são os códigos
            if (
                i + 4 < len(linhas)
                and re.fullmatch(r"\d{2}", linhas[i + 1])
                and re.fullmatch(r"\d{5}", linhas[i + 2])
            ):
                uf = linhas[i]
                cod_uf = linhas[i + 1]
                cod_municip = linhas[i + 2]
                municipio = linhas[i + 3]
                populacao = linhas[i + 4]
                
                registros_2022.append([
                    uf,
                    cod_uf,
                    cod_municip,
                    municipio,
                    populacao
                ])
                
                i += 5
                continue
        
        i += 1

df_ibge_2022 = pd.DataFrame(
    registros_2022,
    columns=[
        "UF",
        "COD. UF",
        "COD. MUNIC",
        "NOME DO MUNICÍPIO",
        "POPULAÇÃO"
    ]
)

print(f"Registros extraídos: {len(df_ibge_2022)}")

print("\nPrimeiros 10 registros:")
print(df_ibge_2022.head(10).to_string(index=False))

print("\nÚltimos 10 registros:")
print(df_ibge_2022.tail(10).to_string(index=False))

# COMMAND ----------

# Leitura tabular do arquivo IBGE 2023
df_ibge_2023 = pd.read_excel(
    arquivo_ibge_2023,
    sheet_name="Municípios",
    engine="xlrd",
    skiprows=1,
    dtype={
        "UF": str,
        "COD. UF": str,
        "COD. MUNIC": str
    }
)

print(f"Registros: {len(df_ibge_2023)}")
print(df_ibge_2023.columns.tolist())
print(df_ibge_2023.head(5).to_string(index=False))

# COMMAND ----------

# Leitura tabular da Tabela 6579 — IBGE

df_ibge_6579 = pd.read_excel(
    arquivo_ibge_principal,
    sheet_name="Tabela",
    skiprows=4,
    header=None,
    dtype=str
)

df_ibge_6579.columns = [
    "NIVEL",
    "CODIGO",
    "LOCALIDADE",
    "POPULACAO_2019",
    "UNIDADE_2019",
    "POPULACAO_2020",
    "UNIDADE_2020",
    "POPULACAO_2021",
    "UNIDADE_2021",
    "POPULACAO_2024",
    "UNIDADE_2024"
]

print(f"Registros: {len(df_ibge_6579)}")
print(f"Colunas: {len(df_ibge_6579.columns)}")

print("\nPrimeiros 5 registros:")
print(df_ibge_6579.head(5).to_string(index=False))

print("\nÚltimos 5 registros:")
print(df_ibge_6579.tail(5).to_string(index=False))

# COMMAND ----------

# Remoção do rodapé da Tabela 6579
df_ibge_6579_bronze = df_ibge_6579[
    df_ibge_6579["NIVEL"].isin(["BR", "UF", "MU"])
].copy()

print(f"Registros válidos Tabela 6579: {len(df_ibge_6579_bronze)}")

# COMMAND ----------

# Conversão dos DataFrames Pandas para Spark
spark_ibge_6579 = spark.createDataFrame(df_ibge_6579_bronze)
spark_ibge_2022 = spark.createDataFrame(df_ibge_2022.astype(str))
spark_ibge_2023 = spark.createDataFrame(df_ibge_2023.astype(str))

# Remove tabelas caso tenham sido parcialmente criadas
spark.sql("DROP TABLE IF EXISTS workspace.bronze.ibge_populacao_2019_2021_2024")
spark.sql("DROP TABLE IF EXISTS workspace.bronze.ibge_populacao_2022")
spark.sql("DROP TABLE IF EXISTS workspace.bronze.ibge_populacao_2023")

# Tabela 6579
(
    spark_ibge_6579.write
    .format("delta")
    .mode("overwrite")
    .option("delta.columnMapping.mode", "name")
    .option("delta.minReaderVersion", "2")
    .option("delta.minWriterVersion", "5")
    .saveAsTable("workspace.bronze.ibge_populacao_2019_2021_2024")
)

# IBGE 2022
(
    spark_ibge_2022.write
    .format("delta")
    .mode("overwrite")
    .option("delta.columnMapping.mode", "name")
    .option("delta.minReaderVersion", "2")
    .option("delta.minWriterVersion", "5")
    .saveAsTable("workspace.bronze.ibge_populacao_2022")
)

# IBGE 2023
(
    spark_ibge_2023.write
    .format("delta")
    .mode("overwrite")
    .option("delta.columnMapping.mode", "name")
    .option("delta.minReaderVersion", "2")
    .option("delta.minWriterVersion", "5")
    .saveAsTable("workspace.bronze.ibge_populacao_2023")
)

print("Tabelas Bronze do IBGE criadas com sucesso.")

# COMMAND ----------

validacao_ibge = spark.sql("""
SELECT
    '2019_2021_2024' AS fonte,
    COUNT(*) AS registros
FROM workspace.bronze.ibge_populacao_2019_2021_2024

UNION ALL

SELECT
    '2022' AS fonte,
    COUNT(*) AS registros
FROM workspace.bronze.ibge_populacao_2022

UNION ALL

SELECT
    '2023' AS fonte,
    COUNT(*) AS registros
FROM workspace.bronze.ibge_populacao_2023
""")

display(validacao_ibge)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação da camada Bronze
# MAGIC
# MAGIC A ingestão dos dados foi concluída com a preservação dos arquivos originais no Volume e a criação das tabelas Delta utilizadas nas etapas seguintes do pipeline.
# MAGIC
# MAGIC ### CNES/DATASUS
# MAGIC - 2019–2022: 336.067 registros
# MAGIC - 2023–2024: 169.696 registros
# MAGIC - Total: 505.763 registros
# MAGIC - Cobertura: janeiro de 2019 a dezembro de 2024
# MAGIC
# MAGIC ### IBGE
# MAGIC - Tabela 6579 (2019, 2020, 2021 e 2024): 5.599 registros
# MAGIC - Censo Demográfico 2022: 5.570 registros municipais
# MAGIC - População para publicação no DOU 2023: 5.574 registros
# MAGIC
# MAGIC Os arquivos apresentaram diferenças de formato, estrutura e nomenclatura entre os períodos analisados. Essas características foram preservadas na camada Bronze e serão tratadas e padronizadas na camada Silver.