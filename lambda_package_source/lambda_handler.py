import requests
import json
import time
import base64
import pandas as pd
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
import yfinance as yf
import s3fs
import os

BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")


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


def get_tickers(json_from_bovespa):
    tickers = [x["cod"] for x in json_from_bovespa["results"]]
    # tickers = [x + ".SA" for x in tickers]

    return tickers


def get_manaus_date():
    manaus_tz = ZoneInfo("America/Manaus")
    manaus_now = datetime.now(manaus_tz)
    return manaus_now.date()


def is_market_close(date_today) -> bool:

    TICKER_VALIDATION = "PETR4.SA"

    try:
        df_validation = yf.download(
            TICKER_VALIDATION, period="1d", interval="1d", progress=False
        )

        if df_validation.empty:
            print(
                f"VALIDAÇÃO: Não há dado recente para {TICKER_VALIDATION}. \
                Possivelmente feriado ou antes do fechamento."
            )
            return False

        real_close_date = df_validation.index[-1].to_pydatetime().date()

        if real_close_date == date_today:
            print(f"VALIDAÇÃO: Dado do dia {date_today} liberado. Prosseguindo.")
            return True
        else:
            print(
                f"VALIDAÇÃO: Dado mais recente ({real_close_date}) é diferente \
                da data de hoje ({date_today}). Aguardando."
            )
            return False

    except Exception as e:
        print(f"ERRO DE VALIDAÇÃO yfinance: {e}")
        return False


def lambda_handler(event, context):

    fs = s3fs.S3FileSystem()

    first_response = fetch_page(1)
    tickers = get_tickers(first_response)
    # date_today = date(2025, 10, 12)
    date_today = get_manaus_date()
    date_end = date_today + timedelta(days=1)
    saved_tickers = 0

    if not is_market_close(date_today):
        return {
            "statusCode": 200,
            "body": json.dumps(
                f"Processo abortado: Dados de {date_today} ainda não foram\
                liberados pelo yfinance."
            ),
        }

    for ticker in tickers:

        ticker_data_df = yf.download(
            f"{ticker}.SA",
            start=f"{date_today.strftime("%Y-%m-%d")}",
            end=f"{date_end.strftime("%Y-%m-%d")}",
            interval="1h",
            progress=False,
            auto_adjust=True,
        )

        if ticker_data_df.empty:
            print(f"AVISO: {ticker} vazio após validação. Pulando.")
            continue

        partition_path = (
            f"raw/ibov/{ticker}/ano={date_today.year}/"
            f"mes={date_today.month:02d}/"
            f"dia={date_today.day:02d}/"
        )

        s3_path = f"s3://{BUCKET_NAME}/{partition_path}data.parquet"

        ticker_data_df.to_parquet(s3_path, index=True, filesystem=fs)

        saved_tickers += 1
        time.sleep(0.5)

    return {
        "statusCode": 200,
        "body": json.dumps(f"Sucesso! Salvo dados de {len(tickers)} tickers em S3."),
    }
