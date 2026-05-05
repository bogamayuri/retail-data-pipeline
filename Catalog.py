# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists retail

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog retail
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists bronze;
# MAGIC create schema if not exists silver;
# MAGIC create schema if not exists gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases from retail