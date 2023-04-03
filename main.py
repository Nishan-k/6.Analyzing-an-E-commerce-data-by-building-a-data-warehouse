import pandas as pd
import mysql.connector
import credentials

df = pd.read_csv("./data/data.csv", encoding='iso-8859-1')

print(df.head())

conn =  mysql.connector.connect(host=credentials.host_name,user=credentials.user_name, password=credentials.password)

cursor = conn.cursor()
cursor.execute("USE ecommerce;")

# TODO:1 Create a Invoice_DIM table:
cursor.execute("""
    CREATE TABLE IF NOT EXISTS INVOICE_DIM(
        INVOICE_KEY INT NOT NULL AUTO_INCREMENT,
        INVOICE_NO  VARCHAR(230),
        PRIMARY KEY(INVOICE_KEY)

    )
""")

# TODO:2 Create StockCode_DIM table:
cursor.execute("""
    CREATE TABLE IF NOT EXISTS STOCKCODE_DIM(
        STOCK_KEY INT NOT NULL AUTO_INCREMENT,
        STOCKCODE VARCHAR(230),
        DESCRIPTION VARCHAR(300),
        PRIMARY KEY(STOCK_KEY)
    )
""")

# TODO:3 Create Date_DIM table:
cursor.execute("""
    CREATE TABLE IF NOT EXISTS DATE_DIM(
        DATE_KEY INT NOT NULL AUTO_INCREMENT,
        DATE VARCHAR(30),
        PRIMARY KEY(DATE_KEY)
    )
""")

# TODO:4 Create Customer_DIM table:
cursor.execute("""
    CREATE TABLE IF NOT EXISTS CUSTOMER_DIM(
        CUSTOMER_KEY INT NOT NULL AUTO_INCREMENT,
        CUSTOMER_ID VARCHAR(230),
        COUNTRY VARCHAR(50),
        PRIMARY KEY(CUSTOMER_KEY)
    )
""")


# TODO:5 Create Quantity_Fact table:
# cursor.execute("""
#     CREATE TABLE IF NOT EXISTS QUANTITY_FACT(
#         INVOICE_KEY 
#     ) 
# """)