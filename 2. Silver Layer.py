# Databricks notebook source
#Orders.write.mode("overwrite").format("delta").mode("overwrite").save("/mnt/delta/raw_orders")

# COMMAND ----------


delta_orders_df = spark.read.format("delta").load("/mnt/delta/raw_orders")

# COMMAND ----------

delta_returns_df = spark.read.format("delta").load("/mnt/delta/raw_returns")

# COMMAND ----------

delta_people_df = spark.read.format("delta").load("/mnt/delta/raw_people")

# COMMAND ----------

delta_orders_df.createOrReplaceTempView("my_orders_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_orders_table
# MAGIC --WHERE MOD(Quantity, 1) <> 0 OR Quantity is NULL
# MAGIC
# MAGIC where rowid = 344
# MAGIC
# MAGIC --where OrderID = 'CA-2017-104220' and ProductID = 'TEC-PH-10004614'

# COMMAND ----------

display(delta_orders_df.where("RowID = '1621'"))

# COMMAND ----------

# DBTITLE 1,Bad Data Check in Dataframe
# Filter rows where 'Sales' column is not a number and display the first row
display(delta_orders_df.filter(~delta_orders_df['Sales'].cast('int').isNotNull()).limit(1))

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

# Remove extra quotes from the ProductName column
cleaned_orders = delta_orders_df.withColumn(
    "ProductName", 
    regexp_replace(col("ProductName"), '""', '"')
)

# Show the cleaned data
display(cleaned_orders)

# COMMAND ----------

cleaned_orders = cleaned_orders.withColumn("Sales", col("Sales").cast("float"))
cleaned_orders = cleaned_orders.withColumn("Quantity", col("Quantity").cast("int"))
cleaned_orders = cleaned_orders.withColumn("Discount", col("Discount").cast("float"))
cleaned_orders = cleaned_orders.withColumn("Profit", col("Profit").cast("float"))

cleaned_orders.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) as Order_Prod ,OrderID,ProductID FROM my_orders_table
# MAGIC GROUP BY OrderID,ProductID
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY count(*) DESC

# COMMAND ----------

# DBTITLE 1,Remove Duplicates
from pyspark.sql.functions import sum

silver_orders_df = delta_orders_df

# Show the result
#display(result_df)

#check = result_df.where("OrderID = 'CA-2017-152912' AND ProductID = 'OFF-ST-10003208'").collect()
#display(check)

display(silver_orders_df)   
  

#QTYCheck = result_df.agg(sum("Sales")).collect()[0][0]

#display(QTYCheck)

# COMMAND ----------

cleaned_orders_df = silver_orders_df.toDF(*[col.lower() for col in silver_orders_df.columns])

spark.sql("DROP TABLE IF EXISTS delta.`/mnt/delta/cleaned_silver_orders`")

cleaned_orders_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save("/mnt/delta/cleaned_silver_orders")

cleaned_orders_df.createOrReplaceTempView("cleaned_silver_orders")

# COMMAND ----------

silver_people_df= spark.read.format("delta").load("/mnt/delta/raw_people")
silver_returns_df= spark.read.format("delta").load("/mnt/delta/raw_returns")

# COMMAND ----------

silver_people_df = silver_people_df.toDF(*[col.lower() for col in silver_people_df.columns])

spark.sql("DROP TABLE IF EXISTS delta.`/mnt/delta/silver_people`")
silver_people_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save("/mnt/delta/cleaned_silver_people")
silver_people_df.createOrReplaceTempView("cleaned_silver_people")

silver_returns_df = silver_returns_df.toDF(*[col.lower() for col in silver_returns_df.columns])

spark.sql("DROP TABLE IF EXISTS delta.`/mnt/delta/silver_returns`")
silver_returns_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save("/mnt/delta/cleaned_silver_returns")
silver_returns_df.createOrReplaceTempView("cleaned_silver_returns")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from cleaned_silver_returns
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *from cleaned_silver_people

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleaned_silver_orders
# MAGIC --WHERE  Sales is NULL
# MAGIC --where rowid = 1621
# MAGIC --where OrderID = 'CA-2017-104220' and ProductID = 'TEC-PH-10004614'
# MAGIC