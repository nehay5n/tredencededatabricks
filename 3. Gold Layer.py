# Databricks notebook source
orders_df = spark.read.format("delta").load("/mnt/delta/cleaned_silver_orders")
people_df = spark.read.format("delta").load("/mnt/delta/cleaned_silver_people")
returns_df = spark.read.format("delta").load("/mnt/delta/cleaned_silver_returns")

display(orders_df)


# COMMAND ----------

from pyspark.sql.functions import col

# Join the orders and returns data on OrderID
orders_with_returns = orders_df.join(returns_df, orders_df.orderid == returns_df.orderid, how='left')

# Add a column to indicate if an order was returned
orders_with_returns = orders_with_returns.withColumn("is_returned", col("Returned").isNotNull())

display(orders_with_returns)

# COMMAND ----------

from pyspark.sql.functions import col

orders_df = orders_df.withColumn("sales", col("sales").cast("double"))
orders_with_returns = orders_with_returns.withColumn("sales", col("sales").cast("double"))

total_sales = orders_df.agg({"sales": "sum"}).collect()[0][0]
total_profit = orders_df.agg({"profit": "sum"}).collect()[0][0]

total_orders = orders_df.count()
returned_orders = orders_with_returns.filter(col("is_returned") == True).count()
return_rate = (returned_orders / total_orders) * 100

average_order_value = orders_df.agg({"sales": "avg"}).collect()[0][0]

sales_by_segment = orders_df.groupBy("segment").sum("sales")
display(sales_by_segment)

sales_by_states = orders_df.groupBy("state").sum("sales")
display(sales_by_states)


print(f"Total Sales: {total_sales}")
print(f"Total Profit: {total_profit}")
print(f"Return Rate: {return_rate}%")
print(f"Average Order Value: {average_order_value}")

# COMMAND ----------

sales_by_segment_df = orders_df.groupBy("segment").agg({"sales": "sum"}).toPandas()

import matplotlib.pyplot as plt

sales_by_segment_df.plot(kind='bar', x='segment', y='sum(sales)', title='Sales by Customer Segment')
plt.show()



# COMMAND ----------


import matplotlib.pyplot as plt


sales_by_state_df = orders_df.groupBy("state").agg({"sales": "sum"}).toPandas()


sales_by_state_df.plot(kind='line', x='state', y='sum(sales)', title='Sales by States')
plt.show()
