# Databricks notebook source
# DBUTILS -> Singleton that manages FS for us
# format -> container_name@storageaccount.blob.core.windows.net
#dbutils.fs.mount(
#    mount_point = "/mnt/lake",
#    source= "wasbs://globalcontainer@globalsquashstorage.blob.core.windows.net",
#    extra_configs = {
#        "fs.azure.account.key.globalsquashstorage.blob.core.windows.net":"AukEMPxMSMCR3KS2QQJoMBL8rviXaIo8IyUOIISCxf0rGYgB4gPeR1wYv8pv77c2qqvXgHNDn1A5+AStBns3ZA=="
#    }
#)

# COMMAND ----------

Orders = spark.read.csv('/mnt/lake/Orders.csv', header=True)
df = Orders.where("RowID = '1621'")
#df = Orders.count()
display(Orders.where("RowID = '1621'"))

# COMMAND ----------

from pyspark.sql.functions import sum

check = Orders.agg(sum("Sales")).collect()
display(check)

# COMMAND ----------

Orders.write.mode("overwrite").option("header", "true").csv("/dbfs/mnt/excel_files/Orders.csv")

# COMMAND ----------

Returns = spark.read.csv('/mnt/lake/Returns.csv', header=True)
#display(Returns);

# COMMAND ----------

Returns.write.mode("overwrite").option("header", "true").csv("/dbfs/mnt/excel_files/Returns.csv")

# COMMAND ----------

People = spark.read.csv('/mnt/lake/People.csv', header=True)
#display(People);

# COMMAND ----------

People.write.mode("overwrite").option("header", "true").csv("/dbfs/mnt/excel_files/People.csv")

# COMMAND ----------


dbutils.fs.ls("/dbfs/mnt/excel_files/")


# COMMAND ----------

Orders.write.mode("overwrite").format("delta").mode("overwrite").save("/mnt/delta/raw_orders")

# COMMAND ----------

Returns.write.mode("overwrite").format("delta").mode("overwrite").save("/mnt/delta/raw_returns")

# COMMAND ----------

People.write.mode("overwrite").format("delta").mode("overwrite").save("/mnt/delta/raw_people")

# COMMAND ----------

dbutils.fs.ls("/mnt/delta/")

# COMMAND ----------

delta_orders_df = spark.read.format("delta").load("/mnt/delta/raw_orders")

# COMMAND ----------

delta_returns_df = spark.read.format("delta").load("/mnt/delta/raw_returns")

# COMMAND ----------

delta_people_df = spark.read.format("delta").load("/mnt/delta/raw_people")

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/demo_pro/checkpoints/orders_silver", True)