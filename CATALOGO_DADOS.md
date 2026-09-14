# Catalogo de Dados
Este documento apresenta o catálogo das principais tabelas utilizadas nas análises do projeto. As tabelas fazem parte da camada Gold e foram construídas a partir dos dados do CNES/DATASUS e dos dados populacionais do IBGE, após as etapas de tratamento, padronização e integração realizadas nos notebooks anteriores.

---

## 1. Evolução Nacional

Tabela: workspace.gold.evolucao_nacional  
Granularidade: um registro por ano  
Chave lógica: ANO

Esta tabela apresenta a evolução da infraestrutura hospitalar no Brasil entre 2019 e 2024.

| Campo | Tipo | Domínio observado | Descrição |
|---|---|---|---|
| ANO | Inteiro | 2019 a 2024 | Ano de referência dos dados |
| TOTAL_HOSPITAIS | Inteiro longo | 6.041 a 6.505 | Quantidade de estabelecimentos classificados como Hospital Geral ou Hospital Especializado |
| TOTAL_LEITOS | Inteiro longo | 461.708 a 516.408 | Quantidade total de leitos existentes |
| TOTAL_LEITOS_SUS | Inteiro longo | 309.415 a 346.480 | Quantidade de leitos vinculados ao SUS |

Linhagem: CNES → Bronze → Silver → agregação anual → Gold.

---

## 2. Infraestrutura por Região

Tabela: workspace.gold.infraestrutura_regiao  
Granularidade: um registro por região e ano  
Chave lógica: ANO + REGIAO

Esta tabela apresenta a quantidade de hospitais e leitos por região brasileira ao longo do período analisado.

| Campo | Tipo | Domínio observado | Descrição |
|---|---|---|---|
| ANO | Inteiro | 2019 a 2024 | Ano de referência |
| REGIAO | Texto | Centro-Oeste, Nordeste, Norte, Sudeste e Sul | Região do estabelecimento hospitalar |
| TOTAL_HOSPITAIS | Inteiro longo | 528 a 2.157 | Quantidade de hospitais por região e ano |
| TOTAL_LEITOS | Inteiro longo | 32.607 a 215.937 | Quantidade total de leitos |
| TOTAL_LEITOS_SUS | Inteiro longo | 24.401 a 129.389 | Quantidade de leitos vinculados ao SUS |

Linhagem: CNES → Bronze → Silver → agregação por ano e região → Gold.

---

## 3. Infraestrutura por UF

Tabela: workspace.gold.infraestrutura_uf  
Granularidade: um registro por UF e ano  
Chave lógica: ANO + UF

Nesta tabela, os dados hospitalares do CNES foram relacionados aos dados populacionais do IBGE. Com isso, além dos valores absolutos, foi possível calcular a quantidade de hospitais e leitos por 100 mil habitantes.

| Campo | Tipo | Domínio observado | Descrição |
|---|---|---|---|
| ANO | Inteiro | 2019 a 2024 | Ano de referência |
| UF | Texto | 27 UFs | Sigla da Unidade da Federação |
| TOTAL_HOSPITAIS | Inteiro longo | 12 a 954 | Quantidade de hospitais |
| TOTAL_LEITOS | Inteiro longo | 1.103 a 111.340 | Quantidade total de leitos |
| TOTAL_LEITOS_SUS | Inteiro longo | 879 a 63.603 | Quantidade de leitos vinculados ao SUS |
| POPULACAO | Inteiro longo | 605.761 a 46.649.132 | População da UF |
| HOSPITAIS_100MIL_HAB | Decimal | 1,42 a 6,07 | Quantidade de hospitais por 100 mil habitantes |
| LEITOS_100MIL_HAB | Decimal | 130,42 a 358,77 | Quantidade de leitos por 100 mil habitantes |
| LEITOS_SUS_100MIL_HAB | Decimal | 103,93 a 233,81 | Quantidade de leitos SUS por 100 mil habitantes |

Linhagem: CNES + IBGE → Bronze → Silver → integração dos dados → agregação por UF → cálculo dos indicadores por 100 mil habitantes → Gold.

---

## 4. Crescimento da Infraestrutura por Região

Tabela: workspace.gold.crescimento_regiao  
Granularidade: um registro por região  
Chave lógica: REGIAO

Esta tabela compara os dados de 2019 e 2024 para mostrar quanto o número de hospitais e leitos aumentou em cada região, tanto em valores absolutos quanto percentuais.

| Campo | Tipo | Domínio observado | Descrição |
|---|---|---|---|
| REGIAO | Texto | Centro-Oeste, Nordeste, Norte, Sudeste e Sul | Região brasileira |
| HOSPITAIS_2019 | Inteiro longo | 528 a 2.033 | Quantidade de hospitais em 2019 |
| LEITOS_2019 | Inteiro longo | 32.607 a 196.365 | Quantidade de leitos em 2019 |
| HOSPITAIS_2024 | Inteiro longo | 596 a 2.157 | Quantidade de hospitais em 2024 |
| LEITOS_2024 | Inteiro longo | 37.714 a 210.563 | Quantidade de leitos em 2024 |
| VAR_HOSPITAIS | Inteiro longo | 8 a 213 | Diferença no número de hospitais entre 2019 e 2024 |
| CRESC_HOSPITAIS_PCT | Decimal | 0,83% a 12,88% | Crescimento percentual do número de hospitais |
| VAR_LEITOS | Inteiro longo | 3.066 a 15.815 | Diferença no número de leitos entre 2019 e 2024 |
| CRESC_LEITOS_PCT | Decimal | 3,88% a 15,66% | Crescimento percentual do número de leitos |

Linhagem: CNES → Bronze → Silver → agregação por região → comparação entre 2019 e 2024 → Gold.

---

## 5. Infraestrutura por Tipo de Hospital

Tabela: workspace.gold.tipo_hospital  
Granularidade: um registro por ano e tipo de hospital  
Chave lógica: ANO + DS_TIPO_UNIDADE

Esta tabela apresenta a evolução dos dois tipos de estabelecimentos considerados como hospitais neste projeto: Hospital Geral e Hospital Especializado.

| Campo | Tipo | Domínio observado | Descrição |
|---|---|---|---|
| ANO | Inteiro | 2019 a 2024 | Ano de referência |
| DS_TIPO_UNIDADE | Texto | Hospital Especializado, Hospital Geral | Tipo de estabelecimento |
| TOTAL_HOSPITAIS | Inteiro longo | 953 a 5.460 | Quantidade de hospitais de cada tipo |
| TOTAL_LEITOS | Inteiro longo | 72.475 a 440.695 | Quantidade total de leitos |
| TOTAL_LEITOS_SUS | Inteiro longo | 45.663 a 298.717 | Quantidade de leitos vinculados ao SUS |

Linhagem: CNES → Bronze → Silver → seleção dos Hospitais Gerais e Hospitais Especializados → agregação por ano e tipo → Gold.

---

## 6. Evolução dos Leitos de UTI

Tabela: workspace.gold.evolucao_uti  
Granularidade: um registro por ano  
Chave lógica: ANO

Esta tabela apresenta a evolução dos leitos de UTI entre 2019 e 2024, incluindo a quantidade de leitos vinculados ao SUS e sua participação no total.

| Campo | Tipo | Domínio observado | Descrição |
|---|---|---|---|
| ANO | Inteiro | 2019 a 2024 | Ano de referência |
| TOTAL_UTI | Inteiro longo | 45.538 a 63.236 | Quantidade total de leitos de UTI |
| TOTAL_UTI_SUS | Inteiro longo | 22.637 a 31.696 | Quantidade de leitos de UTI vinculados ao SUS |
| PCT_UTI_SUS | Decimal | 47,22% a 50,12% | Percentual de leitos de UTI vinculados ao SUS |

Linhagem: CNES → Bronze → Silver → consolidação anual dos leitos de UTI → Gold.

---

## 7. Participação dos Leitos SUS por UF em 2024

Tabela: workspace.gold.participacao_sus_uf_2024  
Granularidade: um registro por UF  
Chave lógica: UF

Esta tabela mostra quanto os leitos vinculados ao SUS representam do total de leitos hospitalares de cada UF em 2024.

| Campo | Tipo | Domínio observado | Descrição |
|---|---|---|---|
| UF | Texto | 27 UFs | Sigla da Unidade da Federação |
| TOTAL_LEITOS | Inteiro longo | 1.506 a 108.014 | Quantidade total de leitos |
| TOTAL_LEITOS_SUS | Inteiro longo | 1.348 a 61.018 | Quantidade de leitos vinculados ao SUS |
| PCT_LEITOS_SUS | Decimal | 51,98% a 89,51% | Percentual de leitos vinculados ao SUS |

Linhagem: CNES → Bronze → Silver → agregação por UF → seleção de 2024 → cálculo da participação dos leitos SUS → Gold.

---

## 8. Menor Oferta Hospitalar Municipal em 2024

Tabela: workspace.gold.menor_oferta_municipal_2024  
Granularidade: um registro por município  
Chave lógica: MUNICIPIO + UF

Esta tabela apresenta os 20 municípios com menor oferta de leitos por 100 mil habitantes em 2024. Para evitar distorções causadas por municípios muito pequenos, foram considerados apenas municípios com 50 mil habitantes ou mais e que possuíam Hospital Geral ou Hospital Especializado.

| Campo | Tipo | Domínio observado | Descrição |
|---|---|---|---|
| UF | Texto | UFs presentes no ranking | Unidade da Federação |
| MUNICIPIO | Texto | 20 municípios | Nome do município |
| POPULACAO | Inteiro longo | 53.083 a 344.828 | População do município |
| TOTAL_HOSPITAIS | Inteiro longo | 1 a 3 | Quantidade de hospitais |
| TOTAL_LEITOS | Inteiro longo | 10 a 109 | Quantidade total de leitos |
| TOTAL_LEITOS_SUS | Inteiro longo | Valores presentes no ranking | Quantidade de leitos vinculados ao SUS |
| HOSPITAIS_100MIL_HAB | Decimal | Valores presentes no ranking | Quantidade de hospitais por 100 mil habitantes |
| LEITOS_100MIL_HAB | Decimal | 8,96 a 42,03 | Quantidade de leitos por 100 mil habitantes |

Linhagem: CNES + IBGE → Bronze → Silver → integração dos dados → cálculo dos indicadores municipais → seleção dos 20 municípios com menor oferta relativa → Gold.

Esses resultados mostram possíveis lacunas de oferta e devem ser utilizados como ponto de partida para investigação. Sozinhos, eles não são suficientes para indicar a necessidade de implantação de novos hospitais.

---

## 9. Municípios sem Hospital em 2024

Tabela: workspace.gold.municipios_sem_hospital_2024  
Granularidade: um registro por município  
Chave lógica: UF + MUNICIPIO

Esta tabela apresenta os municípios que, em 2024, não possuíam estabelecimentos classificados como Hospital Geral ou Hospital Especializado dentro do recorte utilizado no projeto.

| Campo | Tipo | Domínio observado | Descrição |
|---|---|---|---|
| UF | Texto | UFs brasileiras | Unidade da Federação |
| MUNICIPIO | Texto | 2.463 municípios | Nome do município |
| POPULACAO | Inteiro longo | 854 a 124.788 | População do município em 2024 |

Linhagem: IBGE + CNES → Bronze → Silver → integração dos dados → identificação dos municípios sem Hospital Geral ou Hospital Especializado → Gold.

É importante destacar que isso não significa que esses municípios não possuem assistência à saúde. A análise considera apenas os tipos de estabelecimento definidos no recorte do projeto.
