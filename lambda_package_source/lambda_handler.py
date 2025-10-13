import requests
import json
import time
import base64
import pandas as pd
from datetime import datetime
import os
import s3fs

BUCKET_NAME = "PUT THE BUCKET NAME HERE"


def build_encoded_params(page_number):
    params = {
        "language": "pt-br",
        "pageNumber": page_number,
        "pageSize": 120,
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

    print(url)

    user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/100.0.4896.127 Safari/537.36"
    )

    headers = {"User-Agent": user_agent}

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def lambda_handler(event, context):
    """
    Ponto de entrada principal que coleta, processa e salva no S3.
    """
    fs = s3fs.S3FileSystem()

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
        # Espera para evitar rate limiting (Erro 520)
        time.sleep(3)
        resp = fetch_page(page)
        all_rows.extend(resp["results"])

    cols_to_keep = ["cod", "asset", "type", "part", "theoricalQty"]

    df = pd.DataFrame(all_rows)
    df = df[cols_to_keep].copy()
    df["data_referencia"] = carteira_date.strftime("%Y-%m-%d")

    partition_path = (
        f"raw/ibov/ano={carteira_date.year}/"
        f"mes={carteira_date.month:02d}/"
        f"dia={carteira_date.day:02d}/"
    )

    s3_path = f"s3://{BUCKET_NAME}/{partition_path}ibov_carteira.parquet"

    df.to_parquet(s3_path, index=False, filesystem=fs)

    return {
        "statusCode": 200,
        "body": json.dumps(f"Sucesso! Salvo {df.shape[0]} linhas em {s3_path}"),
    }
