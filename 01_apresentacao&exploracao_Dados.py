# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Engenharia de Dados Aplicada à Produção Offshore de Petróleo e Gás Natural no Brasil: construção de um Lakehouse para análise da evolução e concentração da produção entre 2005 e 2025
# MAGIC
# MAGIC **Disciplina:** Engenharia de Dados    
# MAGIC **Aluna:** Renata Rocha de Negreiros Alcântara   
# MAGIC **Matrícula:** 4052026000390  
# MAGIC **Data:** 26/10/2026

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Apresentação, Exploração e Validação dos Dados
# MAGIC
# MAGIC # 1.1 Apresentação do Projeto
# MAGIC
# MAGIC ### 1.1.1 Contexto e importância
# MAGIC
# MAGIC A indústria de petróleo e gás natural possui grande relevância para o setor energético brasileiro, e a produção offshore representa uma parcela significativa dessa atividade. A disponibilização de dados públicos pela Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP) possibilita a análise da evolução da produção e de sua distribuição entre diferentes campos, bacias e operadores.
# MAGIC
# MAGIC Entretanto, os dados são disponibilizados em diferentes arquivos e períodos, tornando necessária a construção de um processo estruturado de ingestão, tratamento, padronização e modelagem para permitir sua análise de forma integrada.
# MAGIC
# MAGIC Este projeto aplica conceitos de Engenharia de Dados para construir um **Lakehouse no Databricks**, utilizando a **Arquitetura Medalhão (Bronze, Silver e Gold)** para organizar e transformar os dados públicos disponibilizados pela ANP.
# MAGIC
# MAGIC ### 1.1.2 Objetivo
# MAGIC
# MAGIC Construir um pipeline de dados capaz de integrar, tratar, organizar e modelar os dados históricos de produção offshore de petróleo e gás natural no Brasil entre 2005 e 2025, permitindo analisar a evolução da produção, sua distribuição geográfica e seu grau de concentração ao longo do período.
# MAGIC
# MAGIC O projeto contempla desde a ingestão dos dados brutos até a construção de tabelas analíticas, incluindo etapas de avaliação da qualidade dos dados, transformação, modelagem e análise.
# MAGIC
# MAGIC ### 1.1.3 Perguntas de negócio
# MAGIC
# MAGIC A solução será desenvolvida com o objetivo de responder às seguintes perguntas:
# MAGIC
# MAGIC 1. Como evoluiu a produção offshore brasileira de petróleo e gás natural entre 2005 e 2025?
# MAGIC 2. Como evoluiu a participação do pré-sal na produção offshore brasileira?
# MAGIC 3. Quais estados e bacias sedimentares concentram a maior parte da produção?
# MAGIC 4. Quais campos são responsáveis pela maior parcela da produção?
# MAGIC 5. Quais operadores concentram a produção de petróleo e gás offshore?
# MAGIC 6. A produção offshore está se tornando mais ou menos concentrada em poucos campos e operadores ao longo do tempo?
# MAGIC
# MAGIC ### 1.1.4 Abordagem de Engenharia de Dados
# MAGIC
# MAGIC Para responder às perguntas propostas, os dados serão organizados segundo a Arquitetura Medalhão:
# MAGIC
# MAGIC - **Bronze:** ingestão e persistência dos dados provenientes das fontes originais;
# MAGIC - **Silver:** limpeza, padronização, integração e aplicação das regras de qualidade;
# MAGIC - **Gold:** modelagem e construção das estruturas destinadas às análises de negócio.
# MAGIC
# MAGIC A solução será desenvolvida no Databricks utilizando recursos do ecossistema Lakehouse, incluindo Apache Spark, Delta Lake e Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Exploração da Fonte de Dados — Produção Offshore da ANP
# MAGIC
# MAGIC ### 1.2.1 Contexto da fonte
# MAGIC
# MAGIC Este notebook inicia a exploração dos dados utilizados no projeto de Engenharia de Dados aplicado à produção offshore de petróleo e gás natural no Brasil.
# MAGIC
# MAGIC A principal fonte utilizada é a Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP), por meio de seus conjuntos de dados abertos de produção de petróleo e gás natural por poço. (Link: https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/fase-de-desenvolvimento-e-producao)
# MAGIC
# MAGIC Para este projeto, foram selecionados os arquivos de produção marítima referentes ao período de **2005 a 2025**, em conformidade com o recorte offshore definido para a análise.
# MAGIC
# MAGIC ### 1.2.2 Armazenamento dos arquivos de origem
# MAGIC
# MAGIC Os arquivos CSV obtidos no portal de Dados Abertos da ANP foram carregados, sem transformação prévia, em um **Volume gerenciado pelo Unity Catalog**, no caminho:
# MAGIC
# MAGIC `/Volumes/workspace/default/anp_raw/`
# MAGIC
# MAGIC O Volume `anp_raw` é utilizado como área de armazenamento dos arquivos originais da fonte, preservando os dados no formato em que foram disponibilizados pela ANP.
# MAGIC
# MAGIC Como verificação inicial, é listado o conteúdo do Volume para confirmar a disponibilidade dos arquivos utilizados no projeto.

# COMMAND ----------

# Lista os arquivos disponíveis no volume 'anp_raw'
display(
    dbutils.fs.ls("/Volumes/workspace/default/anp_raw/")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2.3 Exploração inicial dos dados
# MAGIC
# MAGIC Antes da ingestão de todo o histórico, é realizada uma leitura exploratória do arquivo referente a 2025.
# MAGIC
# MAGIC O objetivo é compreender a estrutura dos dados disponibilizados pela ANP antes da construção das camadas do Lakehouse, verificando a organização das colunas, a granularidade dos registros e os tipos de dados identificados pelo Spark.
# MAGIC
# MAGIC Nesta etapa, nenhuma transformação é aplicada aos dados.

# COMMAND ----------

# Carrega o arquivo CSV de produção marítima de 2025 em um DataFrame Spark
caminho = "/Volumes/workspace/default/anp_raw/producao-mar-2025.csv"

df_mar = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true") #identifica automaticamente o tipo de cada coluna
    .option("sep", ",")
    .load(caminho)
)

# Exibe os dados carregados para validação inicial
display(df_mar)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2.4 Inspeção do schema
# MAGIC
# MAGIC Após a leitura do arquivo, é realizada a inspeção do schema inferido automaticamente pelo Spark.
# MAGIC
# MAGIC Essa etapa permite identificar os tipos atribuídos às variáveis e verificar possíveis necessidades de padronização ou conversão que deverão ser consideradas nas etapas posteriores do pipeline.

# COMMAND ----------


df_mar.printSchema()
print(df_mar.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2.5 Resultados da exploração inicial
# MAGIC
# MAGIC A exploração do arquivo de 2025 permitiu identificar características importantes da fonte antes da construção do pipeline de dados.
# MAGIC
# MAGIC O conjunto apresenta informações de produção em nível de poço, incluindo atributos de localização e identificação, como **Estado, Bacia, Campo, Poço, Ambiente e Instalação**, além das variáveis quantitativas relacionadas à produção e à injeção de fluidos.
# MAGIC
# MAGIC A inspeção do schema mostrou que o campo `Ano` foi reconhecido pelo Spark como tipo inteiro. Entretanto, as variáveis quantitativas de produção e injeção foram interpretadas como `string`, indicando a necessidade de conversão para tipos numéricos nas etapas posteriores de tratamento.
# MAGIC
# MAGIC Essa característica está associada ao formato dos valores disponibilizados na fonte, que utiliza a convenção brasileira de separadores numéricos, com vírgula como separador decimal e, quando aplicável, ponto como separador de milhares.
# MAGIC
# MAGIC Também foram observados valores nulos em algumas variáveis de produção e injeção. Neste momento, esses registros não serão removidos nem substituídos por zero, pois é necessário avaliar posteriormente se a ausência de valores representa um problema de qualidade ou uma característica válida do processo produtivo.
# MAGIC
# MAGIC Os resultados desta exploração serão utilizados como referência para a análise dos demais arquivos históricos e para a definição das regras de padronização da camada Silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Validação dos Arquivos Históricos
# MAGIC
# MAGIC ### 1.3.1 Identificação dos arquivos de origem
# MAGIC
# MAGIC Após a exploração inicial do arquivo referente a 2025, é realizada a validação dos arquivos históricos que compõem o período de análise de **2005 a 2025**.
# MAGIC
# MAGIC Os dados foram disponibilizados pela ANP em arquivos CSV com diferentes abrangências temporais. Nos períodos mais antigos, um mesmo arquivo reúne dados de vários anos, enquanto nos períodos mais recentes os arquivos são disponibilizados individualmente por ano.
# MAGIC
# MAGIC Antes da ingestão na camada Bronze, os arquivos serão avaliados quanto à estrutura e compatibilidade, buscando identificar possíveis diferenças de schema, nomenclatura de colunas e características de formatação que possam impactar sua integração.

# COMMAND ----------

# Lista os arquivos CSV armazenados no Volume de dados brutos da ANP
arquivos = dbutils.fs.ls("/Volumes/workspace/default/anp_raw/")

for arquivo in arquivos:
    print(arquivo.name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3.2 Comparação da estrutura
# MAGIC
# MAGIC Após confirmar a disponibilidade dos arquivos de origem, é realizada uma análise comparativa de suas estruturas.
# MAGIC
# MAGIC Como os dados abrangem um período de 20 anos e foram disponibilizados pela ANP em diferentes arquivos, é necessário verificar se houve alterações na quantidade ou na nomenclatura das colunas ao longo do tempo.
# MAGIC
# MAGIC Essa validação é realizada antes da integração dos arquivos, permitindo identificar eventuais diferenças de schema que possam exigir tratamento ou padronização nas etapas posteriores do pipeline.

# COMMAND ----------

# Percorre os arquivos históricos e compara suas estruturas
for arquivo in arquivos:
    
    df_temp = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", ",")
        .load(arquivo.path)
    )
    
    print(f"Arquivo: {arquivo.name}")
    print(f"Quantidade de colunas: {len(df_temp.columns)}")
    print(f"Colunas: {df_temp.columns}")
    print("-" * 100)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3.3 Inconsistências identificadas
# MAGIC
# MAGIC A comparação dos arquivos históricos demonstrou que todos os conjuntos analisados possuem **21 colunas** e mantêm a mesma estrutura lógica de informações ao longo do período de 2005 a 2025.
# MAGIC
# MAGIC Entretanto, foram identificadas diferenças de formatação entre os arquivos que impedem sua integração direta sem uma etapa prévia de padronização.
# MAGIC
# MAGIC Nos arquivos referentes aos períodos de **2016 a 2019** e ao ano de **2021**, caracteres acentuados presentes nos cabeçalhos não foram interpretados corretamente durante a leitura, indicando diferenças na codificação de caracteres (*encoding*) dos arquivos.
# MAGIC
# MAGIC Também foi identificada uma alteração na nomenclatura dos cabeçalhos a partir de **2022**, quando os nomes das colunas passaram a ser disponibilizados entre colchetes. Por exemplo, a coluna `Ano` passou a ser representada como `[Ano]`.
# MAGIC
# MAGIC Apesar dessas diferenças, a quantidade de colunas e o significado dos atributos permanecem equivalentes entre os arquivos analisados.
# MAGIC
# MAGIC Essas inconsistências deverão ser consideradas no processo de ingestão e padronização dos dados, de forma a permitir a integração do histórico em uma estrutura única.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3.4 Validação do encoding
# MAGIC
# MAGIC Durante a comparação dos arquivos históricos, foram identificados caracteres acentuados interpretados incorretamente em parte dos arquivos, indicando uma possível diferença na codificação de caracteres utilizada pela fonte.
# MAGIC
# MAGIC Antes de realizar qualquer substituição ou transformação nos nomes das colunas, será testada uma configuração alternativa de *encoding* em um dos arquivos afetados.
# MAGIC
# MAGIC O arquivo referente a 2019 será utilizado como amostra para verificar se a inconsistência pode ser corrigida durante a própria leitura do arquivo.

# COMMAND ----------

# Define o caminho do arquivo utilizado para teste de encoding
caminho_2019 = "/Volumes/workspace/default/anp_raw/producao-mar-2019.csv"

# Realiza a leitura utilizando a codificação ISO-8859-1
df_2019_teste = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .option("encoding", "ISO-8859-1")
    .load(caminho_2019)
)

# Exibe os nomes das colunas após a leitura
print(df_2019_teste.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.3.4.1 Validação do encoding nos demais arquivos afetados
# MAGIC
# MAGIC O teste realizado com o arquivo referente a 2019 demonstrou que a utilização da codificação `ISO-8859-1` permite a interpretação correta dos caracteres acentuados presentes nos cabeçalhos.
# MAGIC
# MAGIC Com essa configuração, nomes de colunas anteriormente exibidos de forma incorreta, como `Po�o` e `Produ��o`, passaram a ser interpretados corretamente como `Poço` e `Produção`.
# MAGIC
# MAGIC A partir desse resultado, a mesma configuração será testada nos demais arquivos que apresentaram problemas de codificação durante a análise histórica: **2016–2018 e 2021**.
# MAGIC
# MAGIC O objetivo é verificar se uma mesma regra de leitura pode ser utilizada para esses arquivos antes da definição do processo de ingestão dos dados.

# COMMAND ----------

# Arquivos que apresentaram problemas de codificação na leitura inicial
arquivos_encoding = [
    "producao-mar-2016-2018.csv",
    "producao-mar-2019.csv",
    "producao-mar-2021.csv"
]

for nome_arquivo in arquivos_encoding:

    caminho = f"/Volumes/workspace/default/anp_raw/{nome_arquivo}"

    df_temp = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", ",")
        .option("encoding", "ISO-8859-1")
        .load(caminho)
    )

    print(f"Arquivo: {nome_arquivo}")
    print(df_temp.columns)
    print("-" * 100)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.3.4.2 Resultado da validação do encoding
# MAGIC
# MAGIC A aplicação da codificação `ISO-8859-1` corrigiu a interpretação dos caracteres acentuados nos três arquivos que apresentaram inconsistências: **2016–2018, 2019 e 2021**.
# MAGIC
# MAGIC Dessa forma, foi confirmado que o problema identificado estava relacionado à codificação utilizada na leitura dos arquivos, e não à estrutura ou ao conteúdo dos cabeçalhos disponibilizados pela fonte.
# MAGIC
# MAGIC Os arquivos originais serão preservados sem alterações no Volume `anp_raw`, e a configuração adequada de encoding será considerada durante o processo de ingestão.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Conclusão da Exploração e Definição da Estratégia de Ingestão
# MAGIC
# MAGIC A exploração dos arquivos históricos confirmou que os dados de produção offshore disponibilizados pela ANP mantêm a mesma estrutura lógica ao longo do período de **2005 a 2025**, embora apresentem diferenças técnicas de codificação, nomenclatura dos cabeçalhos e representação dos atributos quantitativos.
# MAGIC
# MAGIC Os testes realizados permitiram definir as configurações de leitura necessárias para preservar corretamente o conteúdo dos arquivos, sem modificar os dados originais armazenados no Volume `anp_raw`.
# MAGIC
# MAGIC Com base nos resultados da exploração, definiu-se a seguinte estratégia para a Arquitetura Medalhão:
# MAGIC
# MAGIC - **Bronze:** os 11 arquivos históricos serão ingeridos separadamente e persistidos como tabelas Delta, preservando sua estrutura o mais próxima possível da fonte. Serão aplicadas apenas as configurações técnicas necessárias para sua leitura correta.
# MAGIC - **Silver:** os dados serão padronizados e consolidados em uma estrutura única, incluindo harmonização dos nomes das colunas, conversão dos tipos de dados, aplicação das regras de qualidade e integração do histórico de 2005 a 2025.
# MAGIC - **Gold:** os dados tratados serão posteriormente modelados em estruturas analíticas destinadas a responder às perguntas de negócio definidas para o projeto.
# MAGIC
# MAGIC Essa estratégia mantém a separação entre **preservação e rastreabilidade dos dados de origem**, na camada Bronze, e **tratamento, qualidade e integração**, na camada Silver.
# MAGIC
# MAGIC A próxima etapa consiste na construção da **camada Bronze**, por meio da ingestão parametrizada dos arquivos históricos e de sua persistência em tabelas Delta no Lakehouse.