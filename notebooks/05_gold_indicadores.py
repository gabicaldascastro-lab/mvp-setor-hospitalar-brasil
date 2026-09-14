# Databricks notebook source
# MAGIC %md
# MAGIC # 05. Indicadores Analíticos — Camada Gold

# COMMAND ----------

# Importação das funções e leitura da base integrada

from pyspark.sql import functions as F

df_base = spark.table(
    "workspace.silver.cnes_ibge_integrado"
)

# COMMAND ----------

# Evolução anual da infraestrutura hospitalar no Brasil

gold_evolucao_hospitais = (
    df_base
    .filter(F.col("FL_HOSPITAL") == 1)
    .groupBy("ANO")
    .agg(
        F.countDistinct("CNES").alias("TOTAL_HOSPITAIS"),
        F.sum("LEITOS_EXISTENTES").alias("TOTAL_LEITOS"),
        F.sum("LEITOS_SUS").alias("TOTAL_LEITOS_SUS")
    )
    .orderBy("ANO")
)

display(gold_evolucao_hospitais)

# COMMAND ----------

# Evolução da infraestrutura hospitalar por região

gold_regiao = (
    df_base
    .filter(F.col("FL_HOSPITAL") == 1)
    .groupBy("ANO", "REGIAO")
    .agg(
        F.countDistinct("CNES").alias("TOTAL_HOSPITAIS"),
        F.sum("LEITOS_EXISTENTES").alias("TOTAL_LEITOS"),
        F.sum("LEITOS_SUS").alias("TOTAL_LEITOS_SUS")
    )
    .orderBy("ANO", "REGIAO")
)

display(gold_regiao)

# COMMAND ----------

# Indicadores de infraestrutura hospitalar por UF

infra_uf = (
    df_base
    .filter(F.col("FL_HOSPITAL") == 1)
    .groupBy("ANO", "UF")
    .agg(
        F.countDistinct("CNES").alias("TOTAL_HOSPITAIS"),
        F.sum("LEITOS_EXISTENTES").alias("TOTAL_LEITOS"),
        F.sum("LEITOS_SUS").alias("TOTAL_LEITOS_SUS")
    )
)

pop_uf = (
    spark.table("workspace.silver.ibge_populacao_municipal")
    .groupBy("ANO", "UF")
    .agg(
        F.sum("POPULACAO").alias("POPULACAO")
    )
)

gold_uf = (
    infra_uf
    .join(
        pop_uf,
        on=["ANO", "UF"],
        how="left"
    )
    .withColumn(
        "HOSPITAIS_100MIL_HAB",
        F.round(
            F.col("TOTAL_HOSPITAIS") / F.col("POPULACAO") * 100000,
            2
        )
    )
    .withColumn(
        "LEITOS_100MIL_HAB",
        F.round(
            F.col("TOTAL_LEITOS") / F.col("POPULACAO") * 100000,
            2
        )
    )
    .withColumn(
        "LEITOS_SUS_100MIL_HAB",
        F.round(
            F.col("TOTAL_LEITOS_SUS") / F.col("POPULACAO") * 100000,
            2
        )
    )
    .orderBy("ANO", "UF")
)

display(gold_uf)

# COMMAND ----------

# Crescimento da infraestrutura hospitalar por região — 2019 a 2024

regiao_2019 = (
    gold_regiao
    .filter(F.col("ANO") == 2019)
    .select(
        "REGIAO",
        F.col("TOTAL_HOSPITAIS").alias("HOSPITAIS_2019"),
        F.col("TOTAL_LEITOS").alias("LEITOS_2019")
    )
)

regiao_2024 = (
    gold_regiao
    .filter(F.col("ANO") == 2024)
    .select(
        "REGIAO",
        F.col("TOTAL_HOSPITAIS").alias("HOSPITAIS_2024"),
        F.col("TOTAL_LEITOS").alias("LEITOS_2024")
    )
)

gold_crescimento_regiao = (
    regiao_2019
    .join(regiao_2024, on="REGIAO")
    .withColumn(
        "VAR_HOSPITAIS",
        F.col("HOSPITAIS_2024") - F.col("HOSPITAIS_2019")
    )
    .withColumn(
        "CRESC_HOSPITAIS_PCT",
        F.round(
            (F.col("HOSPITAIS_2024") / F.col("HOSPITAIS_2019") - 1) * 100,
            2
        )
    )
    .withColumn(
        "VAR_LEITOS",
        F.col("LEITOS_2024") - F.col("LEITOS_2019")
    )
    .withColumn(
        "CRESC_LEITOS_PCT",
        F.round(
            (F.col("LEITOS_2024") / F.col("LEITOS_2019") - 1) * 100,
            2
        )
    )
    .orderBy(F.col("CRESC_HOSPITAIS_PCT").desc())
)

display(gold_crescimento_regiao)

# COMMAND ----------

# Evolução dos hospitais por tipo de unidade

gold_tipo_hospital = (
    df_base
    .filter(F.col("FL_HOSPITAL") == 1)
    .groupBy(
        "ANO",
        "DS_TIPO_UNIDADE"
    )
    .agg(
        F.countDistinct("CNES").alias("TOTAL_HOSPITAIS"),
        F.sum("LEITOS_EXISTENTES").alias("TOTAL_LEITOS"),
        F.sum("LEITOS_SUS").alias("TOTAL_LEITOS_SUS")
    )
    .orderBy(
        "ANO",
        "DS_TIPO_UNIDADE"
    )
)

display(gold_tipo_hospital)

# COMMAND ----------

# Evolução anual da oferta de leitos de UTI

gold_evolucao_uti = (
    df_base
    .filter(F.col("FL_HOSPITAL") == 1)
    .groupBy("ANO")
    .agg(
        F.sum("UTI_TOTAL_EXIST").alias("TOTAL_UTI"),
        F.sum("UTI_TOTAL_SUS").alias("TOTAL_UTI_SUS")
    )
    .withColumn(
        "PCT_UTI_SUS",
        F.round(
            F.col("TOTAL_UTI_SUS") / F.col("TOTAL_UTI") * 100,
            2
        )
    )
    .orderBy("ANO")
)

display(gold_evolucao_uti)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise municipal e possíveis lacunas de oferta
# MAGIC
# MAGIC Para complementar a análise nacional, regional e estadual, foi utilizado o ano de 2024 como fotografia mais recente da infraestrutura hospitalar.
# MAGIC
# MAGIC A análise municipal considera dois aspectos complementares: a oferta relativa de leitos nos municípios que possuem Hospital Geral ou Hospital Especializado e a identificação dos municípios mais populosos que não possuem estabelecimentos dessas categorias no recorte adotado.
# MAGIC
# MAGIC Para o ranking de menor oferta relativa, foram considerados municípios com população igual ou superior a 50 mil habitantes, reduzindo distorções decorrentes de taxas calculadas sobre populações muito pequenas.
# MAGIC
# MAGIC Os resultados devem ser interpretados como indicadores de possíveis lacunas de oferta e pontos para investigação, e não como evidência isolada da necessidade de implantação de novos hospitais. A análise não contempla demanda efetiva, ocupação dos leitos, deslocamento de pacientes, redes regionais de atendimento ou outros tipos de estabelecimentos de saúde.

# COMMAND ----------

# Indicadores municipais de infraestrutura hospitalar — 2024

infra_municipio_2024 = (
    df_base
    .filter(
        (F.col("ANO") == 2024) &
        (F.col("FL_HOSPITAL") == 1)
    )
    .groupBy(
        "COD_MUNICIPIO",
        "MUNICIPIO",
        "UF"
    )
    .agg(
        F.countDistinct("CNES").alias("TOTAL_HOSPITAIS"),
        F.sum("LEITOS_EXISTENTES").alias("TOTAL_LEITOS"),
        F.sum("LEITOS_SUS").alias("TOTAL_LEITOS_SUS")
    )
)

pop_municipio_2024 = (
    spark.table("workspace.silver.ibge_populacao_municipal")
    .filter(F.col("ANO") == 2024)
    .select(
        "COD_MUNICIPIO",
        F.col("POPULACAO")
    )
)

gold_municipio_2024 = (
    infra_municipio_2024
    .join(
        pop_municipio_2024,
        on="COD_MUNICIPIO",
        how="left"
    )
    .withColumn(
        "LEITOS_100MIL_HAB",
        F.round(
            F.col("TOTAL_LEITOS") / F.col("POPULACAO") * 100000,
            2
        )
    )
    .withColumn(
        "HOSPITAIS_100MIL_HAB",
        F.round(
            F.col("TOTAL_HOSPITAIS") / F.col("POPULACAO") * 100000,
            2
        )
    )
)

print(f"Municípios com hospitais em 2024: {gold_municipio_2024.count()}")

# COMMAND ----------

# Municípios com menor oferta relativa de leitos — 2024
# Recorte: municípios com 50 mil ou mais habitantes e presença de hospital no recorte adotado (Hospital Geral ou Hospital Especializado)

gold_menor_oferta_2024 = (
    gold_municipio_2024
    .filter(F.col("POPULACAO") >= 50000)
    .orderBy(
        F.col("LEITOS_100MIL_HAB").asc()
    )
    .select(
        "UF",
        "MUNICIPIO",
        "POPULACAO",
        "TOTAL_HOSPITAIS",
        "TOTAL_LEITOS",
        "TOTAL_LEITOS_SUS",
        "HOSPITAIS_100MIL_HAB",
        "LEITOS_100MIL_HAB"
    )
    .limit(20)
)

display(gold_menor_oferta_2024)

# COMMAND ----------

# Municípios mais populosos sem hospital no recorte adotado — 2024

municipios_com_hospital_2024 = (
    gold_municipio_2024
    .select("COD_MUNICIPIO")
    .distinct()
)

gold_sem_hospital_2024 = (
    spark.table("workspace.silver.ibge_populacao_municipal")
    .filter(F.col("ANO") == 2024)
    .join(
        municipios_com_hospital_2024,
        on="COD_MUNICIPIO",
        how="left_anti"
    )
    .select(
        "UF",
        "MUNICIPIO",
        "POPULACAO"
    )
    .orderBy(F.col("POPULACAO").desc())
)

print(
    f"Municípios sem Hospital Geral ou Hospital Especializado em 2024: "
    f"{gold_sem_hospital_2024.count()}"
)

display(gold_sem_hospital_2024.limit(20))

# COMMAND ----------

# Participação dos leitos SUS na oferta hospitalar por UF — 2024

gold_sus_uf_2024 = (
    gold_uf
    .filter(F.col("ANO") == 2024)
    .withColumn(
        "PCT_LEITOS_SUS",
        F.round(
            F.col("TOTAL_LEITOS_SUS") /
            F.col("TOTAL_LEITOS") * 100,
            2
        )
    )
    .select(
        "UF",
        "TOTAL_LEITOS",
        "TOTAL_LEITOS_SUS",
        "PCT_LEITOS_SUS"
    )
    .orderBy(F.col("PCT_LEITOS_SUS").desc())
)

display(gold_sus_uf_2024)

# COMMAND ----------

# Criação do schema Gold

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")

print("Schema workspace.gold criado/verificado com sucesso.")

# COMMAND ----------

# Persistência das principais tabelas analíticas na camada Gold

tabelas_gold = {
    "evolucao_nacional": gold_evolucao_hospitais,
    "infraestrutura_regiao": gold_regiao,
    "infraestrutura_uf": gold_uf,
    "crescimento_regiao": gold_crescimento_regiao,
    "tipo_hospital": gold_tipo_hospital,
    "evolucao_uti": gold_evolucao_uti,
    "menor_oferta_municipal_2024": gold_menor_oferta_2024,
    "municipios_sem_hospital_2024": gold_sem_hospital_2024,
    "participacao_sus_uf_2024": gold_sus_uf_2024
}

for nome_tabela, dataframe in tabelas_gold.items():
    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"workspace.gold.{nome_tabela}")
    )

    print(f"Tabela workspace.gold.{nome_tabela} criada com sucesso.")

# COMMAND ----------

# Validação final das tabelas Gold

tabelas_gold_validacao = [
    "evolucao_nacional",
    "infraestrutura_regiao",
    "infraestrutura_uf",
    "crescimento_regiao",
    "tipo_hospital",
    "evolucao_uti",
    "menor_oferta_municipal_2024",
    "municipios_sem_hospital_2024",
    "participacao_sus_uf_2024"
]

for tabela in tabelas_gold_validacao:
    df = spark.table(f"workspace.gold.{tabela}")
    print(
        f"{tabela}: "
        f"{df.count()} registros | "
        f"{len(df.columns)} colunas"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consultas analíticas em SQL
# MAGIC
# MAGIC Após a construção e persistência das tabelas na camada Gold, foram realizadas consultas em SQL para explorar os principais indicadores do projeto.
# MAGIC
# MAGIC As consultas utilizam dados previamente tratados e consolidados, permitindo analisar a evolução da infraestrutura hospitalar, as diferenças regionais e estaduais e a oferta de leitos em relação à população.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Evolução da infraestrutura hospitalar no Brasil
# MAGIC
# MAGIC SELECT
# MAGIC     ANO,
# MAGIC     TOTAL_HOSPITAIS,
# MAGIC     TOTAL_LEITOS,
# MAGIC     TOTAL_LEITOS_SUS,
# MAGIC     ROUND(
# MAGIC         TOTAL_LEITOS_SUS * 100.0 / TOTAL_LEITOS,
# MAGIC         2
# MAGIC     ) AS PCT_LEITOS_SUS
# MAGIC FROM workspace.gold.evolucao_nacional
# MAGIC ORDER BY ANO;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Ranking das regiões por crescimento percentual de leitos — 2019 a 2024
# MAGIC
# MAGIC SELECT
# MAGIC     REGIAO,
# MAGIC     LEITOS_2019,
# MAGIC     LEITOS_2024,
# MAGIC     VAR_LEITOS,
# MAGIC     CRESC_LEITOS_PCT
# MAGIC FROM workspace.gold.crescimento_regiao
# MAGIC ORDER BY CRESC_LEITOS_PCT DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- UFs com oferta inferior a 200 leitos por 100 mil habitantes — 2024
# MAGIC
# MAGIC SELECT
# MAGIC     UF,
# MAGIC     POPULACAO,
# MAGIC     TOTAL_HOSPITAIS,
# MAGIC     TOTAL_LEITOS,
# MAGIC     LEITOS_100MIL_HAB
# MAGIC FROM workspace.gold.infraestrutura_uf
# MAGIC WHERE ANO = 2024
# MAGIC   AND LEITOS_100MIL_HAB < 200
# MAGIC ORDER BY LEITOS_100MIL_HAB ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Ranking das UFs pela participação de leitos SUS — 2024
# MAGIC
# MAGIC SELECT
# MAGIC     UF,
# MAGIC     TOTAL_LEITOS,
# MAGIC     TOTAL_LEITOS_SUS,
# MAGIC     PCT_LEITOS_SUS
# MAGIC FROM workspace.gold.participacao_sus_uf_2024
# MAGIC ORDER BY PCT_LEITOS_SUS DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Ranking das UFs pela oferta de leitos por 100 mil habitantes — 2024
# MAGIC
# MAGIC SELECT
# MAGIC     RANK() OVER (
# MAGIC         ORDER BY LEITOS_100MIL_HAB DESC
# MAGIC     ) AS POSICAO,
# MAGIC     UF,
# MAGIC     POPULACAO,
# MAGIC     TOTAL_LEITOS,
# MAGIC     LEITOS_100MIL_HAB
# MAGIC FROM workspace.gold.infraestrutura_uf
# MAGIC WHERE ANO = 2024
# MAGIC ORDER BY POSICAO;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo das análises
# MAGIC
# MAGIC Neste notebook foram construídos os principais indicadores para analisar a infraestrutura hospitalar no Brasil entre 2019 e 2024. A partir dos dados tratados e integrados nas etapas anteriores, foram criadas as tabelas da camada Gold e realizadas consultas em SQL para explorar os resultados.
# MAGIC
# MAGIC No período analisado, o número de hospitais passou de 6.041 em 2019 para 6.505 em 2024, enquanto o total de leitos passou de 461.708 para 506.167. A participação dos leitos vinculados ao SUS permaneceu relativamente estável durante os anos analisados, em torno de 67% do total.
# MAGIC
# MAGIC Na análise regional, Norte e Centro-Oeste apresentaram os maiores crescimentos percentuais no número de leitos entre 2019 e 2024, com 15,66% e 15,61%, respectivamente. Em números absolutos, porém, o Nordeste apresentou o maior aumento, com 15.815 novos leitos no período.
# MAGIC
# MAGIC A relação entre infraestrutura e população também mostrou diferenças importantes entre as UFs. Em 2024, o Distrito Federal apresentou a maior quantidade de leitos por 100 mil habitantes (358,25), enquanto Sergipe apresentou o menor valor (162,81). Também foram observadas diferenças na participação dos leitos SUS, que variou de 89,51% em Roraima a 51,98% no Distrito Federal.
# MAGIC
# MAGIC Na análise municipal de 2024, também foram identificadas diferenças importantes na oferta de infraestrutura hospitalar. Entre os municípios com 50 mil habitantes ou mais e presença de Hospital Geral ou Hospital Especializado, foram observados casos com baixa oferta relativa de leitos. Além disso, foram identificados municípios populosos sem estabelecimentos dessas categorias no recorte analisado. Esses resultados representam possíveis lacunas de oferta para investigação, não sendo suficientes, isoladamente, para indicar a necessidade de implantação de novos hospitais.
# MAGIC
# MAGIC Esses resultados mostram que a infraestrutura hospitalar cresceu no período analisado, mas esse crescimento não ocorreu da mesma forma em todas as regiões, estados e municípios. A utilização de indicadores proporcionais à população ajudou a comparar melhor territórios de tamanhos diferentes e identificar diferenças na distribuição da oferta hospitalar no país.