# Databricks notebook source
from pyspark.sql.functions import col

df_gold = df_gold \
    .withColumn("orderedretaillineprice", col("orderedretaillineprice").cast("double")) \
    .withColumn("quantity", col("quantity").cast("int"))

# COMMAND ----------

df_gold.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("retail.gold.sales_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC **_Sum of sales for each _product__**

# COMMAND ----------

from pyspark.sql.functions import sum, col

df_gold = spark.table("retail.gold.sales_summary")

df_product_sales = df_gold.groupBy("product_name") \
    .agg(
        sum("revenue").alias("total_sales")
    ) \
    .orderBy("total_sales", ascending=False)

display(df_product_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC **_Product wise sales vs Margin_**

# COMMAND ----------

from pyspark.sql.functions import col, sum

df_kpi = df_gold.groupBy("product_name") \
    .agg(
        sum("revenue").alias("total_sales"),
        ( (sum("selling_price") - sum("cost_price")) / sum("selling_price") * 100 ).alias("margin_pct")
    )

display(df_kpi)

# COMMAND ----------

df_gold = df_line \
    .join(df_sales, "salesorderid", "left") \
    .join(df_product, df_line.productid == df_product.product_id, "left") \
    .select(
        df_line.salesorderid,
        df_sales.creationdate,   
        df_sales.customerid,
        df_line.productid,
        df_product.product_name,
        df_product.selling_price,
        df_product.cost_price,
        df_line.quantity,
        df_line.orderedretaillineprice
    ) \
    .withColumn(
        "revenue",
        col("quantity") * col("orderedretaillineprice")
    )

# COMMAND ----------

df_gold.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("retail.gold.sales_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC **_QOQ Growth_**

# COMMAND ----------

from pyspark.sql.functions import year, quarter, col, sum
from pyspark.sql.window import Window
from pyspark.sql.functions import lag

df_gold = spark.table("retail.gold.sales_summary")

df_q = df_gold \
    .withColumn("year", year(col("creationdate"))) \
    .withColumn("quarter", quarter(col("creationdate")))

df_quarter = df_q.groupBy("year", "quarter") \
    .agg(sum("revenue").alias("total_sales")) \
    .orderBy("year", "quarter")

window_spec = Window.orderBy("year", "quarter")

df_qoq = df_quarter \
    .withColumn("prev_sales", lag("total_sales").over(window_spec)) \
    .withColumn(
        "qoq_growth_pct",
        ((col("total_sales") - col("prev_sales")) / col("prev_sales")) * 100
    )

display(df_qoq)
