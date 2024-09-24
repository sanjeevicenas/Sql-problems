# Databricks notebook source
# MAGIC %md
# MAGIC # Select clause

# COMMAND ----------

# 1) 1757. Recyclable and Low Fat Products
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Recyclable and Low Fat Products").getOrCreate()

schema = StructType([
StructField("product_id", StringType(), True),
StructField("low_fats", StringType(), True),
StructField("recyclable", StringType(), True)
])

data = [
    ("0", "Y", "N"),
    ("1", "Y", "Y"),
    ("2", "N", "Y"),
    ("3", "Y", "Y"),
    ("4", "N", "N")
]

df = spark.createDataFrame(data,schema)

Products = df.write.mode("overwrite").saveAsTable('Products')
# print(spark.table('Products'))

sql = spark.sql("select product_id from Products where low_fats = 'Y' and recyclable = 'Y'")

df.select("product_id")\
  .filter((col("low_fats") == 'Y') & (col("recyclable") == 'Y'))\
  .show(100)

# COMMAND ----------

# 2) 584. Find Customer Referee
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Find Customer Referee").getOrCreate()

schema = StructType([
    StructField('id',IntegerType(),True),
    StructField('name',StringType(),True),
    StructField('referee_id',IntegerType(),True)
])

data =[
    (1, "Will", None),
    (2, "Jane", None),
    (3, "Alex", 2),
    (4, "Bill", None),
    (5, "Zack", 1),
    (6, "Mark", 2)
]

df = spark.createDataFrame(data,schema)
Customer = df.write.mode('overwrite').saveAsTable('Customer')
sql = spark.sql("select name from Customer where referee_id <>2 or referee_id is null")
df.select("name")\
  .filter(
      (col('referee_id')!=2) | (col('referee_id').isNull())
  )\
  .show()

# COMMAND ----------

# 3) 595. Big Countries

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Big Countries').getOrCreate()

schema = StructType([
   StructField('name',StringType(),True),
   StructField('continent',StringType(),True),
   StructField('area',IntegerType(),True),
   StructField('population',IntegerType(),True),
   StructField('gdp',LongType(),True) 
])

data = [
("Afghanistan", "Asia", 652230, 25500100, 20343000000),
("Albania", "Europe", 28748, 2831741, 12960000000),
("Algeria", "Africa", 2381741, 37100000, 188681000000),
("Andorra", "Europe", 468, 78115, 3712000000),
("Angola", "Africa", 1246700, 20609294, 100990000000)
]

df = spark.createDataFrame(data,schema)

world = df.write.mode('overwrite').saveAsTable('World')

sql = spark.sql("select name,population,area from  World where area >= 3000000 or population >=25000000")

df.select('name','population','area')\
  .filter(
      (col('area')>=3000000) | (col('population')>=25000000)
  )\
  .show()

# COMMAND ----------

# 4) 1148. Article Views I

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Article Views I").getOrCreate()

schema = StructType([
    StructField('article_id',IntegerType(),True),
    StructField('author_id',IntegerType(),True),
    StructField('viewer_id',IntegerType(),True),
    StructField('view_date',StringType(),True)
])

data = [
    (1, 3, 5, '2019-08-01'),
    (1, 3, 6, '2019-08-02'),
    (2, 7, 7, '2019-08-01'),
    (2, 7, 6, '2019-08-02'),
    (4, 7, 1, '2019-07-22'),
    (3, 4, 4, '2019-07-21'),
    (3, 4, 4, '2019-07-21')
]

df = spark.createDataFrame(data,schema)
Views = df.write.mode("overwrite").saveAsTable("Views")
sql = spark.sql("""
select author_id as id from Views 
where author_id = viewer_id 
group by author_id
order by author_id
                """)
df.filter(
    col('author_id') == col('viewer_id')
)\
    .select(col('author_id')).alias('id')\
        .distinct()\
        .sort('author_id')\
        .show()

# COMMAND ----------

# # To drop files
# path = '/dbfs/FileStore/tables'
# dbutils.fs.rm(path,recurse=True)

# 5) 1683. Invalid Tweets
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Invalid Tweets').getOrCreate()

schema = StructType([
    StructField('tweet_id',IntegerType(),True),
    StructField('content',StringType(),True)
])

data = [
    (1, "Let us Code"),
    (2, "More than fifteen chars are here!")
]

df = spark.createDataFrame(data,schema)
Tweets = df.write.mode('overwrite').saveAsTable('Tweets')
# Tweets = df.write.mode('overwrite').parquet(path)
# path = '/dbfs/FileStore/tables/tweets'
# Tweets = df.write.mode('overwrite').csv(path)
# api = spark.read.csv(path,schema=schema)
# api.select('tweet_id').show()
sql = spark.sql("""
select tweet_id from Tweets
where length(content)>15 
                """)
df.select('tweet_id')\
    .filter(
        length(col('content'))>15
    )\
        .show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Joins

# COMMAND ----------

# 1) 1378. Replace Employee ID With The Unique Identifier

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Replace Employee ID With The Unique Identifier').getOrCreate()

emp_schema = StructType([
StructField('id',IntegerType(),True),
StructField('name',StringType(),True)
])

uni_schema = StructType([
StructField('id',IntegerType(),True),
StructField('unique_id',IntegerType(),True)
])

emp_data = [
    (1, "Alice"),
    (7, "Bob"),
    (11, "Meir"),
    (90, "Winston"),
    (3, "Jonathan")
]

uni_data = [
    (3,1),
    (11,2),
    (90,3)
]

df1 = spark.createDataFrame(emp_data,emp_schema)
df2 = spark.createDataFrame(uni_data,uni_schema)
employees = df1.write.mode('overwrite').saveAsTable('employees')
EmployeeUNI = df2.write.mode('overwrite').saveAsTable('EmployeeUNI')

sql = spark.sql("""
                select u.unique_id,e.name from employees e left join EmployeeUNI u on e.id = u.id
                """)
df1.join(df2,df1['id']==df2['id'],'left')\
    .select(df2['unique_id'],df1['name'])\
        .show()

# COMMAND ----------

# 2) 1068. Product Sales Analysis I


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
