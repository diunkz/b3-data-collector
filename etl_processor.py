import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, sum, avg, lit

# =================================================================
# 1. INICIALIZAÇÃO E ARGUMENTOS
# =================================================================

# Argumentos passados pela Lambda Trigger (sem hífens)
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_INPUT_PATH",  # Caminho do arquivo Parquet recém-chegado
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# 💥 CORREÇÃO CRÍTICA: Job() recebe apenas 1 argumento posicional (GlueContext) 💥
job = Job(glueContext)
job.init(args["JOB_NAME"], args)  # Inicializa o Job e carrega os argumentos

s3_input_path = args["S3_INPUT_PATH"]

# Extrai o bucket name (usado para montar o path de saída)
bucket_name = s3_input_path.split("/")[2]
s3_output_base_path = f"s3://{bucket_name}/refined/ibov_cleaned/"

print(f"Lendo dados de: {s3_input_path}")

# =================================================================
# 2. LEITURA DE DADOS (DynamicFrame e Spark DataFrame)
# =================================================================

# 2.1. Ler o arquivo Parquet recém-chegado
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="parquet", connection_options={"paths": [s3_input_path]}
)

df_spark = dynamic_frame.toDF()

# =================================================================
# 3. TRANSFORMAÇÕES OBRIGATÓRIAS (Requisito 5)
# =================================================================

# 3.1. TRATAMENTO DA DATA DE FECHAMENTO (coluna de índice do yfinance)
# O índice do DataFrame é lido como a coluna 'Date' pelo Spark
df_spark = df_spark.withColumnRenamed("Date", "data_fechamento")

# C: Realizar um Cálculo (Variação Diária)
df_spark = df_spark.withColumn(
    "variacao_diaria",
    col("Close") - col("Open"),  # Diferença entre Fechamento e Abertura
)

# B: Renomear Duas Colunas
df_spark = df_spark.withColumnRenamed("Open", "abertura_usd")
df_spark = df_spark.withColumnRenamed("Volume", "volume_negociado")


# A: Agrupamento Numérico, Sumarização (Calcula o preço médio e volume total do dia)
# Este bloco satisfaz o requisito A.
df_sumarizado = df_spark.groupBy(col("data_fechamento")).agg(
    sum(col("volume_negociado")).alias("volume_total_sumarizado"),
    avg(col("abertura_usd")).alias("preco_medio_abertura"),
)


# =================================================================
# 4. ESCRITA NO REFINED LAYER (Requisito 6)
# =================================================================

# 4.1. Preparar o DataFrame para escrita final
# Selecionamos as colunas e criamos as chaves de partição para o Data Lake
df_final = df_spark.select(
    col("data_fechamento"),
    col("abertura_usd"),
    col("Close").alias("fechamento"),
    col("volume_negociado"),
    col("variacao_diaria"),
    # Colunas para Particionamento Lógico (Criar as chaves ano=, mes=, dia=)
    col("data_fechamento").cast("string").substr(1, 4).alias("ano"),
    col("data_fechamento").cast("string").substr(6, 2).alias("mes"),
    col("data_fechamento").cast("string").substr(9, 2).alias("dia"),
)


# 4.2. Salvar o Parquet no Refined (Particionado)
print(f"Salvando dados transformados em: {s3_output_base_path}")

df_final.write.mode("overwrite").partitionBy("ano", "mes", "dia").parquet(
    s3_output_base_path
)


job.commit()
print("Job Glue concluído com sucesso!")
