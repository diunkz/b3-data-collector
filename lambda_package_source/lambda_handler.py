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
import boto3

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

    user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/100.0.4896.127 Safari/537.36"
    )

    headers = {"User-Agent": user_agent}

    # 💥 LÓGICA DE RE-TENTATIVA (RETRY LOGIC) 💥
    MAX_RETRIES = 5
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers)
            # AQUI: Se o status for 200, ele sai do loop
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            # Captura erros de HTTP (520, 503, 403, timeout, etc.)
            print(
                f"AVISO: Falha na requisição (Tentativa {attempt + 1}/{MAX_RETRIES}). Erro: {e}"
            )

            # Se for a última tentativa, o erro é fatal
            if attempt == MAX_RETRIES - 1:
                print("ERRO FATAL: Requisições falharam após todas as re-tentativas.")
                raise e  # Lança o erro final para a Lambda falhar

            # Espera 5 segundos antes de tentar novamente
            time.sleep(5)

    # Este ponto é inalcançável, mas é bom para a sintaxe
    raise requests.exceptions.RequestException(
        "Erro desconhecido de requisição após retries."
    )


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


def start_job_with_retry(glue_client, job_name, args, max_attempts=10):
    """
    Tenta disparar o Job Glue com re-tentativas e espera (para evitar ConcurrentRunsExceeded).
    """
    for attempt in range(max_attempts):
        try:
            response = glue_client.start_job_run(JobName=job_name, Arguments=args)
            print(f"Job Run ID: {response['JobRunId']}")
            return response

        except glue_client.exceptions.ConcurrentRunsExceededException:
            if attempt == max_attempts - 1:
                print(
                    "ERRO FATAL: GLUE JOBS LOTADOS após 10 tentativas. Alguns Jobs não iniciaram."
                )
                raise  # Lança o erro final

            # 💥 ESPERA LONGA: Espera por 15 segundos para dar tempo de um Job terminar 💥
            print(
                f"AVISO: Concorrência excedida. Tentativa {attempt + 1}. Esperando 15s..."
            )
            time.sleep(15)

        except Exception as e:
            print(f"ERRO: Falha ao iniciar Job {job_name}. Erro: {e}")
            raise  # Lança outros erros (permissão, etc.)
    return None  # Nunca deve ser alcançado


def lambda_handler(event, context):

    fs = s3fs.S3FileSystem()
    GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME")
    glue = boto3.client("glue")

    if not GLUE_JOB_NAME:
        return {
            "statusCode": 500,
            "body": json.dumps("Erro: Variável GLUE_JOB_NAME não encontrada."),
        }

    first_response = fetch_page(1)
    tickers = get_tickers(first_response)
    tickers = tickers[:2]
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

        ticker_data_df.columns = ticker_data_df.columns.droplevel(0)
        ticker_data_df.columns = ["Close", "High", "Low", "Open", "Volume"]
        ticker_data_df = ticker_data_df.reset_index()
        ticker_data_df["Datetime"] = ticker_data_df["Datetime"].dt.tz_localize(None)
        ticker_data_df["Datetime"] = ticker_data_df["Datetime"].astype("datetime64[ms]")

        partition_path = (
            f"raw/ibov/{ticker}/ano={date_today.year}/"
            f"mes={date_today.month:02d}/"
            f"dia={date_today.day:02d}/"
        )

        s3_path = f"s3://{BUCKET_NAME}/{partition_path}data.parquet"

        ticker_data_df.to_parquet(s3_path, index=True, filesystem=fs)

        arguments = {
            "--S3_INPUT_PATH": s3_path,
            "--job-bookmark-option": "job-bookmark-disable",
        }

        try:
            print(f"Disparando Glue Job para: {ticker}")
            response = start_job_with_retry(
                glue, GLUE_JOB_NAME, arguments, max_attempts=10
            )

            # Garante que a mensagem de sucesso use o ID do Job (se a chamada não falhou)
            if response and "JobRunId" in response:
                print(f"Job Run ID: {response['JobRunId']}")

        except Exception as e:
            # Mantemos o pass para que a Lambda continue processando os outros tickers
            pass

        saved_tickers += 1
        time.sleep(0.5)

    return {
        "statusCode": 200,
        "body": json.dumps(f"Sucesso! Salvo dados de {len(tickers)} tickers em S3."),
    }
