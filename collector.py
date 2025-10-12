import requests
import json
import base64
import pandas as pd
from datetime import datetime


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


all_rows = []
first_response = fetch_page(1)
all_rows.extend(first_response["results"])
total_pages = first_response["page"]["totalPages"]
carteira_date = first_response.get("header", {}).get("date", "")
carteira_date = datetime.strptime(carteira_date, "%d/%m/%y").date()

for page in range(2, total_pages + 1):
    resp = fetch_page(page)
    all_rows.extend(resp["results"])

cols_to_keep = ["cod", "asset", "type", "part", "theoricalQty"]

df = pd.DataFrame(all_rows)
df = df[cols_to_keep].copy()
df["data"] = carteira_date

print(df)
print(df.columns)

# df.to_parquet(f"b3_{carteira_date}.parquet", index=False)

# print(f"Salvo {df.shape[0]} linhas da carteira do dia {carteira_date}")
