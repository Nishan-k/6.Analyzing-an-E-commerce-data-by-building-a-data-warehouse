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
    CREATE TABLE  IF NOT EXISTS STOCKCODE_DIM(
        STOCK_KEY INT NOT NULL AUTO_INCREMENT,
        STOCKCODE VARCHAR(230) NULL ,
        DESCRIPTION VARCHAR(300) NULL,
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
cursor.execute("""
    CREATE TABLE IF NOT EXISTS QUANTITY_FACT(
        INVOICE_KEY INT NOT NULL,
        STOCK_KEY INT NOT NULL,
        CUSTOMER_KEY INT NOT NULL,
        DATE_KEY INT NOT NULL,
        QUANTITY INT,
        PRIMARY KEY (INVOICE_KEY, STOCK_KEY, CUSTOMER_KEY, DATE_KEY),
        FOREIGN KEY (INVOICE_KEY) REFERENCES INVOICE_DIM(INVOICE_KEY),
        FOREIGN KEY (STOCK_KEY) REFERENCES STOCKCODE_DIM(STOCK_KEY),
        FOREIGN KEY (CUSTOMER_KEY) REFERENCES CUSTOMER_DIM(CUSTOMER_KEY),
        FOREIGN KEY (DATE_KEY) REFERENCES DATE_DIM(DATE_KEY)

    ) 
""")

# TODO: 6 Create Price_Fact table:
cursor.execute("""
    CREATE TABLE IF NOT EXISTS PRICE_FACT(
        INVOICE_KEY INT NOT NULL,
        STOCK_KEY INT NOT NULL,
        CUSTOMER_KEY INT NOT NULL,
        DATE_KEY INT NOT NULL,
        PRICE FLOAT,
        PRIMARY KEY (INVOICE_KEY, STOCK_KEY, CUSTOMER_KEY, DATE_KEY),
        FOREIGN KEY (INVOICE_KEY) REFERENCES INVOICE_DIM(INVOICE_KEY),
        FOREIGN KEY (STOCK_KEY) REFERENCES STOCKCODE_DIM(STOCK_KEY),
        FOREIGN KEY (CUSTOMER_KEY) REFERENCES CUSTOMER_DIM(CUSTOMER_KEY),
        FOREIGN KEY (DATE_KEY) REFERENCES DATE_DIM(DATE_KEY)
    )
""")


           
# Print the tables created till now:
cursor.execute("SHOW tables;")
tables = cursor.fetchall()
for table in tables:
    print(table)


# Slicing the data from the dataframe to insert into particular dimension tables:
invoice_df = df[['InvoiceNo']]
stock_code_df = df[['StockCode','Description']]
stock_code_df = stock_code_df.fillna(value="NA")

date_df = df[['InvoiceDate']]
customer_df = df[['CustomerID', 'Country']]
customer_df = customer_df.fillna(value="NA")

# Insert into the INVOICE_DIM dimension tables:

invoice_df_query = ("""
    INSERT INTO invoice_dim(INVOICE_NO) VALUES(%s);
""")

for i,row in invoice_df.iterrows():
    cursor.execute(invoice_df_query, list(row))

# # Insert into the STOCKCODE_DIM table:

stock_df_query = ("""
    INSERT INTO STOCKCODE_DIM(STOCKCODE, DESCRIPTION)
    VALUES(%s, %s);
""")

for i, row in stock_code_df.iterrows():
    cursor.execute(stock_df_query, list(row))


# Insert into Customer_DIM table:
customer_df_query = (
    """
        INSERT INTO CUSTOMER_DIM(CUSTOMER_ID, COUNTRY)
        VALUES(%s,%s);
    """
)
for i,row in customer_df.iterrows():
    cursor.execute(customer_df_query, list(row))



# Insert into Date_DIM table:
date_dim_query = (
    """
        INSERT INTO DATE_DIM(DATE)
        VALUES(%s);
    """
)

for i,row in date_df.iterrows():
    cursor.execute(date_dim_query, list(row))

conn.commit()