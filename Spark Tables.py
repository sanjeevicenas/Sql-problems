# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Define the schema
schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("low_fats", StringType(), True),
    StructField("recyclable", StringType(), True)
])

# Create the data
data = [
    ("0", "Y", "N"),
    ("1", "Y", "Y"),
    ("2", "N", "Y"),
    ("3", "Y", "Y"),
    ("4", "N", "N")
]

# Create a Spark DataFrame
df = spark.createDataFrame(data, schema)

# Create the table
df.write.mode("overwrite").saveAsTable("Products")

# Display the table
display(spark.table("Products"))

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Define the schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("referee_id", IntegerType(), True)
])

# Create the data
data = [
    (1, "Will", None),
    (2, "Jane", None),
    (3, "Alex", 2),
    (4, "Bill", None),
    (5, "Zack", 1),
    (6, "Jane", 2),
]

df = spark.createDataFrame(data, schema)
df.write.mode("overwrite").saveAsTable("Customer")
display(spark.table("Customer"))

# COMMAND ----------

from pyspark.sql.types import *

# Define the schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("continent", StringType(), True),
    StructField("area", IntegerType(), True),
    StructField("population", LongType(), True),
    StructField("gdp", LongType(), True)
])

# Create the data
data = [
    ("Afghanistan", "Asia", 652230, 25500100, 20343000000),
    ("Albania", "Europe", 28748, 2831741, 12960000000),
    ("Algeria", "Africa", 2381741, 37100000, 188681000000),
    ("Andorra", "Europe", 468, 78115, 3712000000),
    ("Angola", "Africa", 1246700, 20609294, 100990000000)
]

df = spark.createDataFrame(data, schema)
df.write.mode("overwrite").saveAsTable("world")
display(spark.table("world"))

# COMMAND ----------

from pyspark.sql.types import *
# Define the schema
schema = StructType([
    StructField("article_id", IntegerType(), True),
    StructField("author_id", IntegerType(), True),
    StructField("viewer_id", IntegerType(), True),
    StructField("view_date", StringType(), True)
])

# Create the data
data = [
    (1, 3, 5, "2019-08-01"),
    (1, 3, 6, "2019-08-02"),
    (2, 7, 7, "2019-08-01"),
    (2, 7, 6, "2019-08-02"),
    (4, 7, 1, "2019-07-22"),
    (3, 4, 4, "2019-07-21"),
    (3, 4, 4, "2019-07-21")
]

# Create a Spark DataFrame
df = spark.createDataFrame(data, schema)

# Create the table
df.write.mode("overwrite").saveAsTable("Views")

# Display the table
display(spark.table("Views"))

# COMMAND ----------

from pyspark.sql.types import *

# Define the schema
schema = StructType([
    StructField("tweet_id", IntegerType(), True),
    StructField("content", StringType(), True)
])

# Create the data
data = [
    (1, "Vote for Biden"),
    (2, "Let us make America great again!")
]

# Create a Spark DataFrame
df = spark.createDataFrame(data, schema)

# Create the table
df.write.mode("overwrite").saveAsTable("Tweets")

# Display the table
display(spark.table("Tweets"))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Define the schema for Employees table
employees_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

# Create the data for Employees table
employees_data = [
    (1, "Alice"),
    (7, "Bob"),
    (11, "Meir"),
    (90, "Winston"),
    (3, "Jonathan")
]

# Create a Spark DataFrame for Employees table
employees_df = spark.createDataFrame(employees_data, employees_schema)

# Create the Employees table
employees_df.write.mode("overwrite").saveAsTable("Employees")

# Define the schema for EmployeeUNI table
employeeuni_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("unique_id", IntegerType(), True)
])

# Create the data for EmployeeUNI table
employeeuni_data = [
    (3, 1),
    (11, 2),
    (90, 3)
]

# Create a Spark DataFrame for EmployeeUNI table
employeeuni_df = spark.createDataFrame(employeeuni_data, employeeuni_schema)

# Create the EmployeeUNI table
employeeuni_df.write.mode("overwrite").saveAsTable("EmployeeUNI")

# Display the tables
display(spark.table("Employees"))
display(spark.table("EmployeeUNI"))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Define the schema for Sales table
sales_schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", IntegerType(), True)
])

# Create the data for Sales table
sales_data = [
    (1, 100, 2008, 10, 5000),
    (2, 100, 2009, 12, 5000),
    (7, 200, 2011, 15, 9000)
]

# Create a Spark DataFrame for Sales table
sales_df = spark.createDataFrame(sales_data, sales_schema)

# Create the Sales table
sales_df.write.mode("overwrite").saveAsTable("Sales")

# Define the schema for Product table
product_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True)
])

# Create the data for Product table
product_data = [
    (100, "Nokia"),
    (200, "Apple"),
    (300, "Samsung")
]

# Create a Spark DataFrame for Product table
product_df = spark.createDataFrame(product_data, product_schema)

# Create the Product table
product_df.write.mode("overwrite").saveAsTable("Product")

# Display the tables
display(spark.table("Sales"))
display(spark.table("Product"))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType

# Define the schema for Visits table
visits_schema = StructType([
    StructField("visit_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True)
])

# Create the data for Visits table
visits_data = [
    (1, 101),
    (2, 102),
    (3, 103),
    (4, 104)
]

# Create a Spark DataFrame for Visits table
visits_df = spark.createDataFrame(visits_data, visits_schema)

# Create the Visits table
visits_df.write.mode("overwrite").saveAsTable("Visits")

# Define the schema for Transactions table
transactions_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("visit_id", IntegerType(), True),
    StructField("amount", IntegerType(), True)
])

# Create the data for Transactions table
transactions_data = [
    (1, 1, 500),
    (2, 2, 1500),
    (3, 3, 2000),
    (4, 4, 2500)
]

# Create a Spark DataFrame for Transactions table
transactions_df = spark.createDataFrame(transactions_data, transactions_schema)

# Create the Transactions table
transactions_df.write.mode("overwrite").saveAsTable("Transactions")

# Display the tables
display(spark.table("Visits"))
display(spark.table("Transactions"))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType

# Define the schema for Visits table
visits_schema = StructType([
    StructField("visit_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True)
])

# Create the data for Visits table
visits_data = [
    (1,23),
    (2,9),
    (4,30),
    (5,54),
    (6,96),
    (7,54),
    (8,54)
]

transactions_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("visit_id", IntegerType(), True),
    StructField("amount", IntegerType(), True)
])

transactions_data = [
(2,5,310),
(3,5,300),
(9,5,200),
(12,1,910),
(13,2,970)
]

# Create a Spark DataFrame for Visits
visits_df = spark.createDataFrame(visits_data, visits_schema)

transactions_df = spark.createDataFrame(transactions_data, transactions_schema)


# Create the Visits table
visits_df.write.mode("overwrite").saveAsTable("Visits")
transactions_df.write.mode("overwrite").saveAsTable("Transactions")

# Display the Visits table
display(spark.table("Visits"))
display(spark.table("Transactions"))

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import date

# Define the schema
weather_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("recordDate", DateType(), True),
    StructField("temperature", IntegerType(), True)
])

# Define the data
weather_data = [
    (1, date(2015, 1, 1), 10),
    (2, date(2015, 1, 2), 25),
    (3, date(2015, 1, 3), 20),
    (4, date(2015, 1, 4), 30)
]

# Create the DataFrame
weather_df = spark.createDataFrame(weather_data, schema=weather_schema)

# Save as a table
weather_df.write.mode("overwrite").saveAsTable("Weather")

# Display the table (use this if you are in a Databricks environment)
display(spark.table("Weather"))

# COMMAND ----------


