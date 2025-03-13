from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 15),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("web_scraping_etl", default_args=default_args, schedule_interval="*/5 * * * *")

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
    df.to_csv("/tmp/stock_data.csv", index=False)

def transform_load_data():
    df = pd.read_csv("/tmp/stock_data.csv")
    df["Close Price"] = df["Close Price"].str.replace(",", "").astype(float)
    conn = sqlite3.connect("/tmp/stocks.db")
    df.to_sql("stock_prices", conn, if_exists="replace", index=False)
    conn.close()

task_extract = PythonOperator(task_id="extract_data", python_callable=extract_data, dag=dag)
task_transform_load = PythonOperator(task_id="transform_load_data", python_callable=transform_load_data, dag=dag)
task_extract >> task_transform_load
