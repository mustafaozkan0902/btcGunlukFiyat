import requests
import pandas as pd
from datetime import datetime, timezone
import time
import sys

START_DATE = "2020-01-01"
OUTPUT_FILE = "btc_usd_try.xlsx"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"

def to_unix_seconds(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp())

start_dt = datetime.fromisoformat(START_DATE)
start_unix = to_unix_seconds(start_dt)
end_dt = datetime.now(timezone.utc)
end_unix = to_unix_seconds(end_dt)

def get_market_chart_range(vs_currency, from_unix, to_unix, max_retries=5):
    url = f"{COINGECKO_BASE}/coins/bitcoin/market_chart/range"
    params = {"vs_currency": vs_currency, "from": str(from_unix), "to": str(to_unix)}
    backoff = 1
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            else:
                print(f"HTTP {resp.status_code} — {resp.text[:200]}")
        except requests.RequestException as e:
            print(f"İstek hatası (deneme {attempt}): {e}")
        time.sleep(backoff)
        backoff *= 2
    raise RuntimeError("API isteği başarısız oldu.")

def fetch_currency(vs_currency):
    data = get_market_chart_range(vs_currency, start_unix, end_unix)
    prices = data.get("prices", [])
    df = pd.DataFrame(prices, columns=["timestamp_ms", f"price_{vs_currency}"])
    df["timestamp"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    df = df.set_index("timestamp")
    return df.resample("D").last()[[f"price_{vs_currency}"]]

def main():
    print(f"{START_DATE} → {end_dt.date()} arası BTC fiyatları çekiliyor...")

    df_usd = fetch_currency("usd")
    df_try = fetch_currency("try")

    merged = df_usd.join(df_try, how="inner").reset_index()
    merged["date"] = merged["timestamp"].dt.date
    merged = merged[["date", "price_usd", "price_try"]]
    merged.rename(columns={"price_usd": "BTC_USD", "price_try": "BTC_TRY"}, inplace=True)

    merged.to_excel(OUTPUT_FILE, index=False)
    print(f"Tamamlandı: {len(merged)} gün yazıldı → {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
