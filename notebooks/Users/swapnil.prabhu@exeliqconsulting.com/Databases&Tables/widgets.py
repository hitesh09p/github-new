# Databricks notebook source
# DBTITLE 1,Widgets
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("age", "4", "employee_age")

# COMMAND ----------

age = ""
age = dbutils.widgets.get("age")
print(age)



# COMMAND ----------

dbutils.widgets.dropdown("emp_name", "sam", ["hitesh", "amol","krishna","wang", "sam" ])

# COMMAND ----------

dbutils.widgets.getArgument("emp_name")

# COMMAND ----------

babynames = spark.read.format("csv")\
            .option("header", "true")\
            .option("inferSchema", "true")\
            .load("/FileStore/tables/BabyNames.csv")
babynames.createOrReplaceTempView("babyname_tbl")
display(babynames)

# COMMAND ----------


years = spark.sql("select distinct(Year) from babyname_tbl")
years.orderBy("Year", ascending=False)
display(years)

# COMMAND ----------

dbutils.widgets.dropdown("year", "2007", [str(x) for x in range (2007, 2020)])

# COMMAND ----------

display(babynames.filter(babynames.Year == getArgument("year")))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from babyname_tbl where Year = getArgument("year")