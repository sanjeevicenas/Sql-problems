# Databricks notebook source
# Write a solution to find the ids of products that are both low fat and recyclable.Return the result table in any order.

from pyspark.sql.functions import *

df = spark.read.table("nike_infy.default.products")
result_df = df.filter((col("low_fats") == 'Y') & (col("recyclable") == 'Y')).select("product_id")
display(result_df)


# COMMAND ----------

# Find the names of the customer that are not referred by the customer with id = 2. Return the result table in any order.

from pyspark.sql.functions import *
df = spark.read.table("nike_infy.default.customer")

df.filter((col("referee_id").isNull()) | (col("referee_id") != 2)).select("name").show()


# COMMAND ----------

# A country is big if:

# it has an area of at least three million (i.e., 3000000 km2), or
# it has a population of at least twenty-five million (i.e., 25000000).
# Write a solution to find the name, population, and area of the big countries.

# Return the result table in any order.

from pyspark.sql.functions import *

df = spark.read.table("world")
df.filter((col("population") >= 25000000) | (col("area")>=3000000)).select("name", "population", "area").show()

# COMMAND ----------

# Write a solution to find all the authors that viewed at least one of their own articles.

# Return the result table sorted by id in ascending order.

df = spark.read.table("views")
result = df.filter((col('author_id') == (col('viewer_id'))))\
    .distinct()\
    .select(col("author_id").alias("id"))\
    .sort(col("author_id")).show()

# COMMAND ----------

# Write a solution to find the IDs of the invalid tweets. The tweet is invalid if the number of characters used in the content of the tweet is strictly greater than 15.

df=spark.read.table("Tweets")
df.filter((length(col("content")))>15)\
    .select("tweet_id")\
        .show()

# COMMAND ----------

# Write a solution to show the unique ID of each user, If a user does not have a unique ID replace just show null.

from pyspark.sql.functions import *

df1 = spark.read.table("employees")
df2 = spark.read.table("EmployeeUNI")

df1.join(df2, df1.id == df2.id, "left")\
    .select("unique_id", "name")\
        .sort(col("name")).show()


# COMMAND ----------

from pyspark.sql.functions import *

df1 = spark.read.table("product")
df2 = spark.read.table("sales")


df1.join(df2,df1.product_id == df2.product_id, "inner")\
    .select("product_name","year","price")\
    .show()

# COMMAND ----------

# Write a solution to find the IDs of the users who visited without making any transactions and the number of times they made these types of visits.

from pyspark.sql.functions import *

df1 = spark.read.table("visits")
df2 = spark.read.table("transactions")

df1.join(df2,df1.visit_id == df2.visit_id, "left")\
    .filter(col("transactions.transaction_id").isNull())\
    .groupBy("customer_id")\
    .agg(count("visits.visit_id").alias("count_no_trans"))\
    .select("customer_id", "count_no_trans")\
    .show()

# COMMAND ----------

# Write a solution to find all dates' Id with higher temperatures compared to its previous dates (yesterday).

# Return the result table in any order.

from pyspark.sql.functions import datediff

# Read the table into two DataFrames
df1 = spark.read.table("weather")
df2 = spark.read.table("weather")

# Perform the join and apply the filtering conditions
df1.crossJoin(df2)\
    .where((datediff(df1.recordDate, df2.recordDate) == 1) & (df1.temperature > df2.temperature)) \
    .select(df1.id, df1.recordDate, df1.temperature, df2.recordDate.alias("prev_recordDate"), df2.temperature.alias("prev_temperature")) \
    .show()



# COMMAND ----------


