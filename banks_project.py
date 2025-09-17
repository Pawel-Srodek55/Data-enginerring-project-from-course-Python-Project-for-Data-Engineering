import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime 
def log_progress(message):
    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("code_log.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')
def extract(url, table_attribs):
    html = BeautifulSoup(requests.get(url).text, "html.parser")
    table = html.find_all("tbody")
    rows =table[0].find_all('tr')
    df = pd.DataFrame(columns=table_attribs)
    for row in rows:
        col = row.find_all('td')
        if len(col) !=0:
            if col[1].find_all("a") is not None and len(col[1].find_all("a")) >= 1:
                dict = {"Name": col[1].find_all("a")[1].get_text().replace("\n",""),"MC_USD_Billion":float("".join(col[2].string.replace("\n","").split(",")))}
                df1 = pd.DataFrame(dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df
    
def transform(df,csv_path):
    exchange = pd.read_csv(csv_path)
    usd_list = df["MC_USD_Billion"].values.tolist()
    #usd_list = [float("".join(usd.split(","))) for usd in usd_list]
    gbd_list = [np.round(usd*exchange.iloc[1,1],2) for usd in usd_list]
    eur_list = [np.round(usd * exchange.iloc[0,1],2) for usd in usd_list]
    inr_list = [np.round(usd * exchange.iloc[2,1],2) for usd in usd_list]
    df["MC_GBP_Billion"] = gbd_list
    df["MC_EUR_Billion"] = eur_list
    df["MC_INR_Billion"] = inr_list
    #print(df['MC_EUR_Billion'][4]) 126.33
    return df
def load_to_csv(df, output_path):
    df.to_csv(output_path)
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name","MC_USD_Billion"]
output_csv_path = './Largest_banks_data.csv'
db_name = "Banks.db"
table_name =  'Largest_banks'
exchange_rate = "./exchange_rate.csv"
log_progress("Preliminaries complete. Initiating ETL process")
df =  extract(url,table_attribs)
log_progress("Data extraction complete. Initiating Transformation process")
df = transform(df,exchange_rate)
log_progress("Data transformation complete. Initiating Loading process")
load_to_csv(df, output_csv_path)
log_progress("Data saved to CSV file")
conn = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")
load_to_db(df, conn, table_name)
log_progress("Data loaded to Database as a table, Executing queries")
run_query("SELECT * FROM Largest_banks", conn)
run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", conn)
run_query("SELECT Name from Largest_banks LIMIT 5", conn)
log_progress("Process Complete")
conn.close()
log_progress("Server Connection closed")
