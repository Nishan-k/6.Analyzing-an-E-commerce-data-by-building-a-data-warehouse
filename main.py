import pandas as pd
import mysql.connector

df = pd.read_csv("./data/data.csv", encoding='iso-8859-1')

print(df.head())

conn =  mysql.connector.connect(host="127.0.0.1",user="root", password="livelifehard123@")

cursor = conn.cursor()
cursor.execute("USE ecommerce;")

