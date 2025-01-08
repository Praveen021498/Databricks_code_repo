# Databricks notebook source
data = [("banana",1000,"USA"),
        ("oranges",2000,"india"),
        ("grapes",300,"china"),
        ("apples",400,"Russia"),
        ("kiwi",900,"USA"),
        ("lychee",700,"USA")]

columns = ["fruit","amount","country"]

df = spark.createDataFrame(data, schema = columns)
df.show()

# COMMAND ----------

df.groupBy("fruit").pivot("country").sum("amount").orderBy("fruit").show()
