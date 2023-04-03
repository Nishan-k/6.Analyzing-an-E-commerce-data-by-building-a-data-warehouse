import pandas as pd
import mysql.connector

df = pd.read_csv("./data/data.csv", encoding='iso-8859-1')
print(df.head())