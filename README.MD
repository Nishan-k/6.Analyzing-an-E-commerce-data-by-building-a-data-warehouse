<h2>From CSV to Data Warehouse: Modeling E-commerce Data for Analysis </h2>
<img src="./images/dimensional model.png" alt="DE-workflow" title="Dimensional model">

<h3> Goal Of The Project:</h3>
Empower Data-Driven Decisions: Create a Star Schema Data Warehouse to Store and Analyze E-commerce Data, Driving Strategic Business Decisions.


<h3>Table of Contents:</h3>
<ol>
    <li> <a href="#data">About the data  </a></li>
    <li><a href="#architecture"> The Architecture  </a></li>
    <li> <a href="#steps">Steps implemented </a></li>
    <li> <a href="#analysis">Analysis </a> </li>
</ol>


<h4 id="data">1. About the data </h4>
The data was used from the <a href="https://www.kaggle.com/datasets/carrie1/ecommerce-data"> Kaggle </a>. 
It is the dataset about an E-commerce site, which has 8 columns in total.
The columns are:
<ol>
    <li>InvoiceNo</li>
    <li>StockCode</li>
    <li>Description</li>
    <li>Quantity</li>
    <li>InvoiceDate</li>
    <li>UnitPrice</li>
    <li>CustomerID</li>
     <li>Country</li>
</ol>

<h4 id="#architecture">2. The Architecture </h4>
<p>This dimensional model consists of 4 dimension tables and 2 fact tables.
The dimension table are presented as TableName_DIM with a suffix DIM and for fact table, Fact as the suffix, TableName_Fact.
<ol type="i">
    <li>Invoice_DIM:</li> <br>
     <img src="./images/invoice.png" alt="DE-workflow" title="INVOICE"> </img>
     <br>
     Here, Invoice_key is the primary key,for primary key, I have used the surrogate key which is an auto-increment, set during this table was built.
     Where as, InvoiceNo is the Natural Key.
    <br>
    <br>
     <li> StockCode_DIM:</li>
     <br>
     <img src="./images/stockcode.png" alt="DE-workflow" title="StockCode"> </img> 
     <br>
     Here, there are three columns in total, Stock_key is the primary key which is the surrogate key (set as auto-increment). StockCode is the Natural key and description is the column that holds the information about the product.
    <br>
    <br>
    <li> Date_DIM: </li>
    <br>
    <img src="./images/date.png" title="date"> </img>  
    <br>
    It contains two columns, Date_Key is the primary key (surrogate key set to an auto-increment) and Date which hold the date information when certain event or transaction took place.
    <br>
    <br>
    <li > Customer_DIM: </li>
    <br>
    <img src="./images/customer.png" title="customer "> </img>
    <br>
    This dimensional table contains Customer_Key as the primary key(surrogate key set to an auto-increment), Customer_ID as the Natural Key and country of the customer.
    <br>
    <br>
    <li> Price_Fact:</li>
    <br>
    <img src="./images/price.png" title="customer "> </img>
     <br>
     <p>Here, this fact table contains 5 columns in total, the primary keys are all the foreign keys that are used in this table, suffixed by _Key. They are representd as PK,FK.  All these foreign keys are the primary keys of the other 4 dimensional tables and a column Price that holds our facts/measurements/metrics.
     <br>
     <br>
     <li> Quantity_Fact: </li>
     <br>
     <img src="./images/quantity.png" title="quantity "> </img>
    <br>
    <p>This fact table consists of 5 columns, the first 4 columns are the primary key of this table, which are the foreign keys of the other 4 dimensional tables, represented as PK,FK.
    And there is the quantity column, which will hold the facts/metrics/measurement.
</ol>
 </p>
 <br>
 <h3>REASON OF CREATING TWO FACT TABLES INSTEAD OF ONE: </H3>
In order for two or more facts to go into the same table,it must fulfill two conditions:
<ol>
    <li>Both of the facts should have the same level of granularity.</li>
    <li> Both of the facts must occur simultaneously </li>
</ol>