# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG retail;
# MAGIC USE SCHEMA silver;

# COMMAND ----------

from pyspark.sql.functions import expr

df_product_clean = df_product \
    .withColumn("selling_price", regexp_replace("selling_price", ",", "")) \
    .withColumn("cost_price", regexp_replace("cost_price", ",", "")) \
    .withColumn("selling_price", expr("try_cast(selling_price as double)")) \
    .withColumn("cost_price", expr("try_cast(cost_price as double)"))

# COMMAND ----------

df_product_clean.write \
    .mode("overwrite") \
    .saveAsTable("retail.silver.product")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SHOW TABLES IN retail.silver;

# COMMAND ----------

display(df_product_clean)

# COMMAND ----------

from pyspark.sql.functions import col, expr, coalesce

df_sales = spark.table("retail.bronze.sales_order")

df_sales_clean = df_sales \
    .withColumn(
        "creationdate",
        coalesce(
            expr("try_to_date(creationdate, 'M/d/yyyy')"),
            expr("try_to_date(creationdate, 'd/M/yyyy')")
        )
    ) \
    .dropna(subset=["salesorderid"])

df_sales_clean.write.mode("overwrite").saveAsTable("retail.silver.sales")

# COMMAND ----------

display(df_sales_clean)

# COMMAND ----------

df_line = spark.table("retail.bronze.sales_order_line")

# COMMAND ----------

from pyspark.sql.functions import expr, trim

df_line_clean = df_line \
    .withColumn("quantity", expr("try_cast(quantity as int)")) \
    .withColumn(
        "orderedretaillineprice",
        expr("try_cast(trim(orderedretaillineprice) as double)")
    ) \
    .withColumn(
        "orderedfulllineprice",
        expr("try_cast(trim(orderedfulllineprice) as double)")
    )

# COMMAND ----------

df_line_clean.write \
    .mode("overwrite") \
    .saveAsTable("retail.silver.sales_order_line")

# COMMAND ----------

display(spark.table("retail.silver.sales_order_line"))