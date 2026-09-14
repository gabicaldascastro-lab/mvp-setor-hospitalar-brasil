# Databricks notebook source
# MAGIC %md
# MAGIC # Catálogo de Dados
# MAGIC
# MAGIC Este notebook apresenta o catálogo de dados das principais tabelas analíticas construídas no projeto.
# MAGIC
# MAGIC As tabelas apresentadas fazem parte da camada Gold e foram construídas a partir dos dados do CNES e das estimativas populacionais do IBGE, após as etapas de tratamento, padronização e integração realizadas nos notebooks anteriores.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Tabelas disponíveis na camada Gold
# MAGIC SHOW TABLES IN workspace.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Domínio dos valores da tabela evolucao_nacional
# MAGIC
# MAGIC SELECT
# MAGIC     MIN(ANO) AS ANO_MIN,
# MAGIC     MAX(ANO) AS ANO_MAX,
# MAGIC     MIN(TOTAL_HOSPITAIS) AS HOSPITAIS_MIN,
# MAGIC     MAX(TOTAL_HOSPITAIS) AS HOSPITAIS_MAX,
# MAGIC     MIN(TOTAL_LEITOS) AS LEITOS_MIN,
# MAGIC     MAX(TOTAL_LEITOS) AS LEITOS_MAX,
# MAGIC     MIN(TOTAL_LEITOS_SUS) AS LEITOS_SUS_MIN,
# MAGIC     MAX(TOTAL_LEITOS_SUS) AS LEITOS_SUS_MAX
# MAGIC FROM workspace.gold.evolucao_nacional;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Evolução Nacional (`evolucao_nacional`)
# MAGIC
# MAGIC Tabela analítica que apresenta a evolução anual da infraestrutura hospitalar no Brasil entre 2019 e 2024. Os indicadores foram construídos a partir dos dados do CNES após as etapas de tratamento e consolidação realizadas nas camadas anteriores.
# MAGIC
# MAGIC | Campo | Tipo | Chave | Domínio observado | Descrição e linhagem |
# MAGIC |---|---|---|---|---|
# MAGIC | ANO | Inteiro | PK lógica | 2019 a 2024 | Ano de referência dos dados, obtido a partir do período analisado nos arquivos do CNES. |
# MAGIC | TOTAL_HOSPITAIS | Inteiro longo | — | 6.041 a 6.505 | Quantidade de estabelecimentos classificados como Hospital Geral ou Hospital Especializado. Calculado a partir dos estabelecimentos do CNES consolidados por ano. |
# MAGIC | TOTAL_LEITOS | Inteiro longo | — | 461.708 a 516.408 | Quantidade total de leitos existentes nos estabelecimentos hospitalares. Derivado da soma dos leitos registrados no CNES para cada ano. |
# MAGIC | TOTAL_LEITOS_SUS | Inteiro longo | — | 309.415 a 346.480 | Quantidade de leitos vinculados ao SUS. Derivado da soma dos leitos SUS registrados no CNES para cada ano. |
# MAGIC
# MAGIC **Chave lógica:** `ANO`.
# MAGIC
# MAGIC **Linhagem:** CNES → camada Bronze → tratamento e padronização na camada Silver → agregação anual → camada Gold.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Domínio dos valores da tabela infraestrutura_regiao
# MAGIC
# MAGIC SELECT
# MAGIC     MIN(ANO) AS ANO_MIN,
# MAGIC     MAX(ANO) AS ANO_MAX,
# MAGIC     CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(REGIAO))) AS REGIOES,
# MAGIC     MIN(TOTAL_HOSPITAIS) AS HOSPITAIS_MIN,
# MAGIC     MAX(TOTAL_HOSPITAIS) AS HOSPITAIS_MAX,
# MAGIC     MIN(TOTAL_LEITOS) AS LEITOS_MIN,
# MAGIC     MAX(TOTAL_LEITOS) AS LEITOS_MAX,
# MAGIC     MIN(TOTAL_LEITOS_SUS) AS LEITOS_SUS_MIN,
# MAGIC     MAX(TOTAL_LEITOS_SUS) AS LEITOS_SUS_MAX
# MAGIC FROM workspace.gold.infraestrutura_regiao;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Infraestrutura por Região (`infraestrutura_regiao`)
# MAGIC
# MAGIC Tabela analítica que apresenta a infraestrutura hospitalar agregada por região brasileira e ano, permitindo acompanhar a distribuição de hospitais e leitos entre 2019 e 2024.
# MAGIC
# MAGIC | Campo | Tipo | Chave | Domínio observado | Descrição e linhagem |
# MAGIC |---|---|---|---|---|
# MAGIC | ANO | Inteiro | PK lógica | 2019 a 2024 | Ano de referência dos dados, obtido a partir do período analisado nos arquivos do CNES. |
# MAGIC | REGIAO | Texto | PK lógica | CENTRO-OESTE, NORDESTE, NORTE, SUDESTE, SUL | Região geográfica do estabelecimento hospitalar, proveniente dos dados do CNES. |
# MAGIC | TOTAL_HOSPITAIS | Inteiro longo | — | 528 a 2.157 | Quantidade de estabelecimentos classificados como Hospital Geral ou Hospital Especializado em cada região e ano. |
# MAGIC | TOTAL_LEITOS | Inteiro longo | — | 32.607 a 215.937 | Quantidade total de leitos existentes, agregada por região e ano a partir dos registros do CNES. |
# MAGIC | TOTAL_LEITOS_SUS | Inteiro longo | — | 24.401 a 129.389 | Quantidade de leitos vinculados ao SUS, agregada por região e ano a partir dos registros do CNES. |
# MAGIC
# MAGIC **Chave lógica:** combinação de `ANO` + `REGIAO`.
# MAGIC
# MAGIC **Linhagem:** CNES → camada Bronze → tratamento e padronização na camada Silver → agregação por ano e região → camada Gold.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Domínio dos valores da tabela infraestrutura_uf
# MAGIC
# MAGIC SELECT
# MAGIC     MIN(ANO) AS ANO_MIN,
# MAGIC     MAX(ANO) AS ANO_MAX,
# MAGIC     COUNT(DISTINCT UF) AS TOTAL_UFS,
# MAGIC     MIN(TOTAL_HOSPITAIS) AS HOSPITAIS_MIN,
# MAGIC     MAX(TOTAL_HOSPITAIS) AS HOSPITAIS_MAX,
# MAGIC     MIN(TOTAL_LEITOS) AS LEITOS_MIN,
# MAGIC     MAX(TOTAL_LEITOS) AS LEITOS_MAX,
# MAGIC     MIN(TOTAL_LEITOS_SUS) AS LEITOS_SUS_MIN,
# MAGIC     MAX(TOTAL_LEITOS_SUS) AS LEITOS_SUS_MAX,
# MAGIC     MIN(POPULACAO) AS POPULACAO_MIN,
# MAGIC     MAX(POPULACAO) AS POPULACAO_MAX,
# MAGIC     MIN(HOSPITAIS_100MIL_HAB) AS HOSP_100MIL_MIN,
# MAGIC     MAX(HOSPITAIS_100MIL_HAB) AS HOSP_100MIL_MAX,
# MAGIC     MIN(LEITOS_100MIL_HAB) AS LEITOS_100MIL_MIN,
# MAGIC     MAX(LEITOS_100MIL_HAB) AS LEITOS_100MIL_MAX,
# MAGIC     MIN(LEITOS_SUS_100MIL_HAB) AS LEITOS_SUS_100MIL_MIN,
# MAGIC     MAX(LEITOS_SUS_100MIL_HAB) AS LEITOS_SUS_100MIL_MAX
# MAGIC FROM workspace.gold.infraestrutura_uf;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Infraestrutura por UF (`infraestrutura_uf`)
# MAGIC
# MAGIC Tabela analítica que apresenta a infraestrutura hospitalar por Unidade da Federação e ano. Nesta tabela, os dados hospitalares do CNES foram integrados aos dados populacionais do IBGE, permitindo calcular indicadores proporcionais à população e comparar UFs de diferentes tamanhos.
# MAGIC
# MAGIC | Campo | Tipo | Chave | Domínio observado | Descrição e linhagem |
# MAGIC |---|---|---|---|---|
# MAGIC | ANO | Inteiro | PK lógica | 2019 a 2024 | Ano de referência utilizado na integração dos dados do CNES e IBGE. |
# MAGIC | UF | Texto | PK lógica | 27 UFs | Sigla da Unidade da Federação, utilizada na agregação territorial dos dados. |
# MAGIC | TOTAL_HOSPITAIS | Inteiro longo | — | 12 a 954 | Quantidade de estabelecimentos classificados como Hospital Geral ou Hospital Especializado em cada UF e ano. |
# MAGIC | TOTAL_LEITOS | Inteiro longo | — | 1.103 a 111.340 | Quantidade total de leitos existentes, agregada por UF e ano a partir dos dados do CNES. |
# MAGIC | TOTAL_LEITOS_SUS | Inteiro longo | — | 879 a 63.603 | Quantidade de leitos vinculados ao SUS, agregada por UF e ano a partir dos dados do CNES. |
# MAGIC | POPULACAO | Inteiro longo | — | 605.761 a 46.649.132 | População da UF obtida pela agregação dos dados populacionais municipais do IBGE para cada ano. |
# MAGIC | HOSPITAIS_100MIL_HAB | Decimal | — | 1,42 a 6,07 | Indicador calculado pela razão entre o total de hospitais e a população da UF, multiplicada por 100.000. |
# MAGIC | LEITOS_100MIL_HAB | Decimal | — | 130,42 a 358,77 | Indicador calculado pela razão entre o total de leitos e a população da UF, multiplicada por 100.000. |
# MAGIC | LEITOS_SUS_100MIL_HAB | Decimal | — | 103,93 a 233,81 | Indicador calculado pela razão entre os leitos vinculados ao SUS e a população da UF, multiplicada por 100.000. |
# MAGIC
# MAGIC **Chave lógica:** combinação de `ANO` + `UF`.
# MAGIC
# MAGIC **Linhagem:** dados hospitalares do CNES + dados populacionais do IBGE → tratamento e padronização nas camadas anteriores → integração por município e ano → agregação por UF → cálculo dos indicadores por 100 mil habitantes → camada Gold.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Domínio dos valores da tabela crescimento_regiao
# MAGIC
# MAGIC SELECT
# MAGIC     CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(REGIAO))) AS REGIOES,
# MAGIC     MIN(HOSPITAIS_2019) AS HOSPITAIS_2019_MIN,
# MAGIC     MAX(HOSPITAIS_2019) AS HOSPITAIS_2019_MAX,
# MAGIC     MIN(HOSPITAIS_2024) AS HOSPITAIS_2024_MIN,
# MAGIC     MAX(HOSPITAIS_2024) AS HOSPITAIS_2024_MAX,
# MAGIC     MIN(VAR_HOSPITAIS) AS VAR_HOSPITAIS_MIN,
# MAGIC     MAX(VAR_HOSPITAIS) AS VAR_HOSPITAIS_MAX,
# MAGIC     MIN(CRESC_HOSPITAIS_PCT) AS CRESC_HOSPITAIS_MIN,
# MAGIC     MAX(CRESC_HOSPITAIS_PCT) AS CRESC_HOSPITAIS_MAX,
# MAGIC     MIN(LEITOS_2019) AS LEITOS_2019_MIN,
# MAGIC     MAX(LEITOS_2019) AS LEITOS_2019_MAX,
# MAGIC     MIN(LEITOS_2024) AS LEITOS_2024_MIN,
# MAGIC     MAX(LEITOS_2024) AS LEITOS_2024_MAX,
# MAGIC     MIN(VAR_LEITOS) AS VAR_LEITOS_MIN,
# MAGIC     MAX(VAR_LEITOS) AS VAR_LEITOS_MAX,
# MAGIC     MIN(CRESC_LEITOS_PCT) AS CRESC_LEITOS_MIN,
# MAGIC     MAX(CRESC_LEITOS_PCT) AS CRESC_LEITOS_MAX
# MAGIC FROM workspace.gold.crescimento_regiao;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Crescimento da Infraestrutura por Região (`crescimento_regiao`)
# MAGIC
# MAGIC Tabela analítica que compara a infraestrutura hospitalar das regiões brasileiras entre 2019 e 2024. A tabela permite observar tanto a variação absoluta quanto o crescimento percentual do número de hospitais e de leitos no período analisado.
# MAGIC
# MAGIC | Campo | Tipo | Chave | Domínio observado | Descrição e linhagem |
# MAGIC |---|---|---|---|---|
# MAGIC | REGIAO | Texto | PK lógica | CENTRO-OESTE, NORDESTE, NORTE, SUDESTE, SUL | Região geográfica utilizada para comparação da infraestrutura hospitalar. |
# MAGIC | HOSPITAIS_2019 | Inteiro longo | — | 528 a 2.033 | Quantidade de hospitais existentes em cada região no ano de 2019. |
# MAGIC | LEITOS_2019 | Inteiro longo | — | 32.607 a 196.365 | Quantidade total de leitos existentes em cada região em 2019. |
# MAGIC | HOSPITAIS_2024 | Inteiro longo | — | 596 a 2.157 | Quantidade de hospitais existentes em cada região no ano de 2024. |
# MAGIC | LEITOS_2024 | Inteiro longo | — | 37.714 a 210.563 | Quantidade total de leitos existentes em cada região em 2024. |
# MAGIC | VAR_HOSPITAIS | Inteiro longo | — | 8 a 213 | Variação absoluta do número de hospitais entre 2019 e 2024. |
# MAGIC | CRESC_HOSPITAIS_PCT | Decimal | — | 0,83% a 12,88% | Crescimento percentual do número de hospitais entre 2019 e 2024. |
# MAGIC | VAR_LEITOS | Inteiro longo | — | 3.066 a 15.815 | Variação absoluta do número de leitos entre 2019 e 2024. |
# MAGIC | CRESC_LEITOS_PCT | Decimal | — | 3,88% a 15,66% | Crescimento percentual do número de leitos entre 2019 e 2024. |
# MAGIC
# MAGIC **Chave lógica:** `REGIAO`.
# MAGIC
# MAGIC **Linhagem:** dados consolidados do CNES → tratamento e padronização nas camadas anteriores → agregação da infraestrutura por região → seleção dos anos de 2019 e 2024 → cálculo das variações absolutas e percentuais → camada Gold.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Domínio dos valores da tabela tipo_hospital
# MAGIC
# MAGIC SELECT
# MAGIC     MIN(ANO) AS ANO_MIN,
# MAGIC     MAX(ANO) AS ANO_MAX,
# MAGIC     CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(DS_TIPO_UNIDADE))) AS TIPOS_HOSPITAL,
# MAGIC     MIN(TOTAL_HOSPITAIS) AS HOSPITAIS_MIN,
# MAGIC     MAX(TOTAL_HOSPITAIS) AS HOSPITAIS_MAX,
# MAGIC     MIN(TOTAL_LEITOS) AS LEITOS_MIN,
# MAGIC     MAX(TOTAL_LEITOS) AS LEITOS_MAX,
# MAGIC     MIN(TOTAL_LEITOS_SUS) AS LEITOS_SUS_MIN,
# MAGIC     MAX(TOTAL_LEITOS_SUS) AS LEITOS_SUS_MAX
# MAGIC FROM workspace.gold.tipo_hospital;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Infraestrutura por Tipo de Hospital (`tipo_hospital`)
# MAGIC
# MAGIC Tabela analítica que apresenta a evolução da infraestrutura hospitalar de acordo com o tipo de estabelecimento. Foram considerados os estabelecimentos classificados no CNES como Hospital Geral ou Hospital Especializado, conforme o recorte definido para este projeto.
# MAGIC
# MAGIC | Campo | Tipo | Chave | Domínio observado | Descrição e linhagem |
# MAGIC |---|---|---|---|---|
# MAGIC | ANO | Inteiro | PK lógica | 2019 a 2024 | Ano de referência dos dados analisados. |
# MAGIC | DS_TIPO_UNIDADE | Texto | PK lógica | HOSPITAL ESPECIALIZADO, HOSPITAL GERAL | Classificação do tipo de estabelecimento hospitalar proveniente do CNES. |
# MAGIC | TOTAL_HOSPITAIS | Inteiro longo | — | 953 a 5.460 | Quantidade de estabelecimentos hospitalares de cada tipo em cada ano. |
# MAGIC | TOTAL_LEITOS | Inteiro longo | — | 72.475 a 440.695 | Quantidade total de leitos existentes nos estabelecimentos de cada tipo hospitalar. |
# MAGIC | TOTAL_LEITOS_SUS | Inteiro longo | — | 45.663 a 298.717 | Quantidade de leitos vinculados ao SUS nos estabelecimentos de cada tipo hospitalar. |
# MAGIC
# MAGIC **Chave lógica:** combinação de `ANO` + `DS_TIPO_UNIDADE`.
# MAGIC
# MAGIC **Linhagem:** CNES → camada Bronze → tratamento e padronização na camada Silver → seleção dos estabelecimentos classificados como Hospital Geral ou Hospital Especializado → agregação por ano e tipo de unidade → camada Gold.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Domínio dos valores da tabela evolucao_uti
# MAGIC
# MAGIC SELECT
# MAGIC     MIN(ANO) AS ANO_MIN,
# MAGIC     MAX(ANO) AS ANO_MAX,
# MAGIC     MIN(TOTAL_UTI) AS UTI_MIN,
# MAGIC     MAX(TOTAL_UTI) AS UTI_MAX,
# MAGIC     MIN(TOTAL_UTI_SUS) AS UTI_SUS_MIN,
# MAGIC     MAX(TOTAL_UTI_SUS) AS UTI_SUS_MAX,
# MAGIC     MIN(PCT_UTI_SUS) AS PCT_UTI_SUS_MIN,
# MAGIC     MAX(PCT_UTI_SUS) AS PCT_UTI_SUS_MAX
# MAGIC FROM workspace.gold.evolucao_uti;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Evolução dos Leitos de UTI (`evolucao_uti`)
# MAGIC
# MAGIC Tabela analítica que apresenta a evolução anual dos leitos de UTI nos estabelecimentos hospitalares analisados, incluindo a quantidade vinculada ao SUS e sua participação percentual no total de leitos de UTI.
# MAGIC
# MAGIC | Campo | Tipo | Chave | Domínio observado | Descrição e linhagem |
# MAGIC |---|---|---|---|---|
# MAGIC | ANO | Inteiro | PK lógica | 2019 a 2024 | Ano de referência dos dados analisados. |
# MAGIC | TOTAL_UTI | Inteiro longo | — | 45.538 a 63.236 | Quantidade total de leitos de UTI existentes nos estabelecimentos hospitalares analisados. |
# MAGIC | TOTAL_UTI_SUS | Inteiro longo | — | 22.637 a 31.696 | Quantidade de leitos de UTI vinculados ao SUS. |
# MAGIC | PCT_UTI_SUS | Decimal | — | 47,22% a 50,12% | Percentual de leitos de UTI vinculados ao SUS em relação ao total de leitos de UTI. |
# MAGIC
# MAGIC **Chave lógica:** `ANO`.
# MAGIC
# MAGIC **Linhagem:** CNES → camada Bronze → tratamento e padronização na camada Silver → consolidação dos leitos de UTI e leitos de UTI vinculados ao SUS por ano → cálculo da participação percentual do SUS → camada Gold.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Domínio dos valores da tabela participacao_sus_uf_2024
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT UF) AS TOTAL_UFS,
# MAGIC     MIN(TOTAL_LEITOS) AS LEITOS_MIN,
# MAGIC     MAX(TOTAL_LEITOS) AS LEITOS_MAX,
# MAGIC     MIN(TOTAL_LEITOS_SUS) AS LEITOS_SUS_MIN,
# MAGIC     MAX(TOTAL_LEITOS_SUS) AS LEITOS_SUS_MAX,
# MAGIC     MIN(PCT_LEITOS_SUS) AS PCT_LEITOS_SUS_MIN,
# MAGIC     MAX(PCT_LEITOS_SUS) AS PCT_LEITOS_SUS_MAX
# MAGIC FROM workspace.gold.participacao_sus_uf_2024;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Participação dos Leitos SUS por UF em 2024 (`participacao_sus_uf_2024`)
# MAGIC
# MAGIC Tabela analítica que apresenta a participação dos leitos vinculados ao SUS em relação ao total de leitos hospitalares de cada Unidade da Federação no ano de 2024.
# MAGIC
# MAGIC | Campo | Tipo | Chave | Domínio observado | Descrição e linhagem |
# MAGIC |---|---|---|---|---|
# MAGIC | UF | Texto | PK lógica | 27 UFs | Sigla da Unidade da Federação utilizada para agregação territorial dos dados. |
# MAGIC | TOTAL_LEITOS | Inteiro longo | — | 1.506 a 108.014 | Quantidade total de leitos existentes nos estabelecimentos hospitalares de cada UF em 2024. |
# MAGIC | TOTAL_LEITOS_SUS | Inteiro longo | — | 1.348 a 61.018 | Quantidade de leitos vinculados ao SUS em cada UF no ano de 2024. |
# MAGIC | PCT_LEITOS_SUS | Decimal | — | 51,98% a 89,51% | Percentual de leitos vinculados ao SUS em relação ao total de leitos hospitalares da UF. |
# MAGIC
# MAGIC **Chave lógica:** `UF`.
# MAGIC
# MAGIC **Linhagem:** CNES → camada Bronze → tratamento e padronização na camada Silver → agregação dos leitos por UF → seleção do ano de 2024 → cálculo da participação percentual dos leitos SUS → camada Gold.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Domínio dos valores da tabela menor_oferta_municipal_2024
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS TOTAL_MUNICIPIOS,
# MAGIC     MIN(POPULACAO) AS POPULACAO_MIN,
# MAGIC     MAX(POPULACAO) AS POPULACAO_MAX,
# MAGIC     MIN(TOTAL_HOSPITAIS) AS HOSPITAIS_MIN,
# MAGIC     MAX(TOTAL_HOSPITAIS) AS HOSPITAIS_MAX,
# MAGIC     MIN(TOTAL_LEITOS) AS LEITOS_MIN,
# MAGIC     MAX(TOTAL_LEITOS) AS LEITOS_MAX,
# MAGIC     MIN(LEITOS_100MIL_HAB) AS LEITOS_100MIL_MIN,
# MAGIC     MAX(LEITOS_100MIL_HAB) AS LEITOS_100MIL_MAX
# MAGIC FROM workspace.gold.menor_oferta_municipal_2024;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Menor Oferta Hospitalar Municipal em 2024 (`menor_oferta_municipal_2024`)
# MAGIC
# MAGIC Tabela analítica que identifica os 20 municípios com menor oferta relativa de leitos hospitalares em 2024, considerando a quantidade de leitos por 100 mil habitantes. O indicador permite comparar municípios de diferentes tamanhos populacionais e identificar localidades com menor disponibilidade relativa de infraestrutura hospitalar.
# MAGIC
# MAGIC | Campo | Tipo | Chave | Domínio observado | Descrição e linhagem |
# MAGIC |---|---|---|---|---|
# MAGIC | MUNICIPIO | Texto | — | Municípios presentes no ranking | Nome do município. Proveniente dos dados tratados do CNES e padronizado nas etapas anteriores do pipeline. |
# MAGIC | UF | Texto | — | Siglas das UFs presentes no ranking | Unidade da Federação à qual o município pertence. Proveniente dos dados tratados do CNES. |
# MAGIC | POPULACAO | Inteiro longo | — | 53.083 a 344.828 | População estimada do município utilizada no cálculo do indicador. Proveniente dos dados populacionais do IBGE integrados à infraestrutura hospitalar. |
# MAGIC | TOTAL_HOSPITAIS | Inteiro longo | — | 1 a 3 | Quantidade de estabelecimentos hospitalares identificados no município em 2024. Derivada dos registros consolidados do CNES. |
# MAGIC | TOTAL_LEITOS | Inteiro longo | — | 10 a 109 | Quantidade total de leitos hospitalares existentes no município em 2024. Derivada da consolidação dos registros do CNES. |
# MAGIC | LEITOS_100MIL_HAB | Decimal | — | 8,96 a 42,03 | Indicador de oferta hospitalar calculado pela razão entre o total de leitos e a população do município, multiplicada por 100.000. |
# MAGIC
# MAGIC **Chave lógica:** combinação de `MUNICIPIO` + `UF`.
# MAGIC
# MAGIC **Linhagem:** dados hospitalares do CNES + dados populacionais do IBGE → tratamento e padronização nas camadas anteriores → integração por município → cálculo do indicador de leitos por 100 mil habitantes → seleção dos 20 municípios com menor oferta relativa em 2024 → camada Gold.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Domínio dos valores da tabela municipios_sem_hospital_2024
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS TOTAL_MUNICIPIOS,
# MAGIC     MIN(POPULACAO) AS POPULACAO_MIN,
# MAGIC     MAX(POPULACAO) AS POPULACAO_MAX
# MAGIC FROM workspace.gold.municipios_sem_hospital_2024;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Municípios sem Hospital em 2024 (`municipios_sem_hospital_2024`)
# MAGIC
# MAGIC Tabela analítica que reúne os municípios que, em 2024, não apresentaram estabelecimentos classificados como Hospital Geral ou Hospital Especializado no recorte adotado neste projeto. A tabela permite identificar localidades que podem representar possíveis lacunas de oferta hospitalar e que merecem investigação mais detalhada.
# MAGIC
# MAGIC | Campo | Tipo | Chave | Domínio observado | Descrição e linhagem |
# MAGIC |---|---|---|---|---|
# MAGIC | UF | Texto | Parte da chave | UFs brasileiras | Unidade da Federação à qual o município pertence. Proveniente dos dados tratados e padronizados nas etapas anteriores do pipeline. |
# MAGIC | MUNICIPIO | Texto | Parte da chave | 2.463 municípios identificados | Nome do município sem Hospital Geral ou Hospital Especializado no recorte analisado em 2024. |
# MAGIC | POPULACAO | Inteiro longo | — | 854 a 124.788 | População do município em 2024. Proveniente dos dados populacionais do IBGE integrados à base hospitalar. |
# MAGIC
# MAGIC **Chave lógica:** combinação de `UF` + `MUNICIPIO`.
# MAGIC
# MAGIC **Linhagem:** dados populacionais do IBGE + dados hospitalares do CNES → tratamento e padronização nas camadas anteriores → integração das fontes → identificação dos municípios sem estabelecimentos classificados como Hospital Geral ou Hospital Especializado em 2024 → camada Gold.
# MAGIC
# MAGIC **Observação:** a ausência desses tipos de estabelecimento no recorte analisado não significa necessariamente ausência de assistência à saúde no município. Os resultados representam possíveis lacunas para investigação e não são suficientes, isoladamente, para indicar a necessidade de implantação de novos hospitais.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo
# MAGIC
# MAGIC Neste notebook foi construído o catálogo de dados das tabelas utilizadas nas análises do projeto. Para cada tabela foram documentados o contexto, os campos, tipos de dados, chaves lógicas, domínios de valores observados e a linhagem dos dados.
# MAGIC
# MAGIC Também foram realizadas consultas para verificar os valores mínimos, máximos e categorias presentes nos campos, permitindo documentar os domínios com base nos dados do próprio projeto.
# MAGIC
# MAGIC Ao final, foram catalogadas as nove tabelas analíticas utilizadas no MVP, mantendo a documentação alinhada à estrutura e aos indicadores construídos nos notebooks anteriores.