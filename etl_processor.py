import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.job import Job
from pyspark.sql.functions import col, sum, avg, substring, year, lit
from urllib.parse import unquote

# =================================================================
# 1. INICIALIZAÇÃO E ARGUMENTOS
# =================================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_INPUT_PATH",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Decodifica o caminho (resolve o problema %3D)
s3_input_path_encoded = args["S3_INPUT_PATH"]
s3_input_path = unquote(s3_input_path_encoded)

# Extrai o bucket name e o base path para o refined
bucket_name = s3_input_path.split("/")[2]
s3_output_base_path = f"s3://{bucket_name}/refined/ibov_cleaned/"

# Tenta extrair o ticker do caminho (para inserção de coluna)
try:
    # Ex: .../raw/ibov/ALOS3/ano=... -> ticker é ALOS3
    ticker_value = s3_input_path.split("/")[5]
except IndexError:
    ticker_value = "UNKNOWN_TICKER"  # Fallback em caso de erro

print(f"Lendo dados de: {s3_input_path}")


# =================================================================
# 2. LEITURA DE DADOS E ACHATAMENTO
# =================================================================

try:
    # 2.1. Ler o arquivo Parquet
    df_spark = spark.read.parquet(s3_input_path)

    # 2.2. Achatamento: Força todas as colunas para minúsculas
    df_spark = df_spark.toDF(*[col.lower() for col in df_spark.columns])

    # 2.3. Limpeza: Remove a coluna lixo '__index_level_0__'
    df_spark = df_spark.drop("__index_level_0__")


except Exception as e:
    print(f"ERRO CRÍTICO NA LEITURA DO PARQUET: {e}")
    job.commit()
    sys.exit(1)


# =================================================================
# 3. TRANSFORMAÇÕES OBRIGATÓRIAS (Requisito 5)
# =================================================================

# 3.1. TRATAMENTO E RENOMEAÇÃO BASE
df_spark = df_spark.withColumnRenamed("datetime", "data_fechamento")
df_spark = df_spark.withColumnRenamed("open", "preco_abertura")
df_spark = df_spark.withColumnRenamed("volume", "volume_dia")


# C: Realizar um Cálculo (Variação Diária)
df_spark = df_spark.withColumn(
    "variacao_diaria",
    col("close") - col("preco_abertura"),
)


# A: Agrupamento Numérico, Sumarização (Requisito 5A)
df_sumarizado = df_spark.groupBy(col("data_fechamento")).agg(
    sum(col("volume_dia")).alias("volume_total_sumarizado"),
    avg(col("preco_abertura")).alias("preco_medio_abertura"),
)


# =================================================================
# 4. ESCRITA NO REFINED LAYER (Requisito 6)
# =================================================================

# 4.1. Inserir o Ticker de volta ao DataFrame (CRÍTICO para o particionamento)
df_spark = df_spark.withColumn("codigo_acao", lit(ticker_value))


# 4.2. Preparar o DataFrame para escrita final
df_final = df_spark.select(
    col("data_fechamento"),
    col("preco_abertura"),
    col("close").alias("fechamento"),
    col("high").alias("maxima"),
    col("low").alias("minima"),
    col("volume_dia"),
    col("variacao_diaria"),
    col("codigo_acao"),
    # Colunas para Particionamento Lógico
    year(col("data_fechamento")).alias("ano"),
    substring(col("data_fechamento").cast("string"), 6, 2).alias("mes"),
    substring(col("data_fechamento").cast("string"), 9, 2).alias("dia"),
)


# 4.3. Salvar o Parquet no Refined (Particionado)
print(f"Salvando dados transformados em: {s3_output_base_path}")

# Modo 'append' é obrigatório para anexar os dados, superando o problema de sobrescrita.
df_final.write.mode("append").partitionBy("ano", "mes", "dia", "codigo_acao").parquet(
    s3_output_base_path
)


job.commit()
print("Job Glue concluído com sucesso!")
