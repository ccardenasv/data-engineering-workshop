import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3

def extract_data():
    url = "https://finance.yahoo.com/quote/AAPL/history?p=AAPL"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    rows = soup.find_all("tr")[1:6]
    data = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) > 5:
            date = cols[0].text
            close_price = cols[4].text
            data.append([date, close_price])
    df = pd.DataFrame(data, columns=["Date", "Close Price"])
    df.to_csv("stock_data.csv", index=False)
    return df

def transform_load_data():
    df = pd.read_csv("stock_data.csv")
    df["Close Price"] = df["Close Price"].str.replace(",", "").astype(float)
    conn = sqlite3.connect("stocks.db")
    df.to_sql("stock_prices", conn, if_exists="replace", index=False)
    conn.close()

df = extract_data()
transform_load_data()
print("ETL Pipeline executed successfully!")
