# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog retail;
# MAGIC use schema bronze;

# COMMAND ----------

base_path = "/Volumes/retail/source_data/raw/"

# COMMAND ----------

def clean_column_names(df):
    for col_name in df.columns:
        new_col = col_name.lower().replace(" ", "_")
        df = df.withColumnRenamed(col_name, new_col)
    return df

# COMMAND ----------

# DBTITLE 1,Cell 3
from pyspark.sql.functions import current_timestamp, col

def clean_column_names(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower().replace(" ", "_"))
    return df


def load_to_bronze(file_name, table_name):
    
    df = spark.read.format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .load(base_path + file_name)
    
    # Clean column names 
    df = clean_column_names(df)
    
    # Convert ALL columns to string 
    for c in df.columns:
        df = df.withColumn(c, col(c).cast("string"))
    
    # Add metadata
    df = df.withColumn("_ingest_time", current_timestamp()) \
           .withColumn("_source_file", col("_metadata.file_path"))
    
    # Write to Delta
    df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"retail.bronze.{table_name}")
    
    print(f"{file_name} → loaded")

# COMMAND ----------

load_to_bronze("Product.csv", "product")
load_to_bronze("Product_Incr.csv", "product")

load_to_bronze("customer.csv", "customer")

load_to_bronze("SalesOrder.csv", "sales_order")
load_to_bronze("SalesOrderline.csv", "sales_order_line")

load_to_bronze("CardPayment.csv", "payment")
load_to_bronze("CardRefund.csv", "refund")

# COMMAND ----------

display(spark.table("retail.bronze.product"))
display(spark.table("retail.bronze.customer"))
display(spark.table("retail.bronze.sales_order"))