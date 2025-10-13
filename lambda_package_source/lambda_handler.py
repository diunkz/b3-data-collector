import requests
import json
import base64
import pandas as pd
from datetime import datetime
import os

# Nota: Você precisará do s3fs/pyarrow para df.to_parquet('s3://...')

# O nome do seu bucket.
BUCKET_NAME = "data-pipeline-488070626945-data-lake-bucket"


def build_encoded_params(page_number):
    params = {
        "language": "pt-br",
        "pageNumber": page_number,
        "pageSize": 20,
        "index": "IBOV",
        "segment": "1",
    }
    return base64.b64encode(json.dumps(params).encode()).decode()


def fetch_page(page_number):
    encoded = build_encoded_params(page_number)
    url = (
        "https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/"
        f"GetPortfolioDay/{encoded}"
    )
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def lambda_handler(event, context):
    """
    Ponto de entrada principal que coleta, processa e salva no S3.
    """
    all_rows = []
    first_response = fetch_page(1)
    all_rows.extend(first_response["results"])
    total_pages = first_response["page"]["totalPages"]

    carteira_date_str = first_response.get("header", {}).get("date", "")
    try:
        carteira_date = datetime.strptime(carteira_date_str, "%d/%m/%y")
    except ValueError:
        carteira_date = datetime.now()

    for page in range(2, total_pages + 1):
        resp = fetch_page(page)
        all_rows.extend(resp["results"])

    cols_to_keep = ["cod", "asset", "type", "part", "theoricalQty"]

    df = pd.DataFrame(all_rows)
    df = df[cols_to_keep].copy()
    df["data_referencia"] = carteira_date.strftime("%Y-%m-%d")

    # ----------------------------------------------------
    # CONSTRUÇÃO DO CAMINHO E S3
    # ----------------------------------------------------

    # Estrutura de Partição: raw/ibov/ano=YYYY/mes=MM/dia=DD/arquivo.parquet
    partition_path = (
        f"raw/ibov/ano={carteira_date.year}/"
        f"mes={carteira_date.month:02d}/"
        f"dia={carteira_date.day:02d}/"
    )

    s3_path = f"s3://{BUCKET_NAME}/{partition_path}ibov_carteira.parquet"

    # Salva o DataFrame como Parquet no S3
    df.to_parquet(s3_path, index=False)

    return {
        "statusCode": 200,
        "body": json.dumps(f"Sucesso! Salvo {df.shape[0]} linhas em {s3_path}"),
    }
