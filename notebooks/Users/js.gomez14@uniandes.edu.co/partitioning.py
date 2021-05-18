# Databricks notebook source
df = sc.range(0,20)
df.getNumPartitions()

# COMMAND ----------

rdd1 = sc.parallelize((0,20), 10)
rdd1.getNumPartitions()

# COMMAND ----------

path = "/FileStore/tables/"
rddDesdeArchivo = sc \
    .textFile(path+"deporte.csv", 10)


# COMMAND ----------

rddDesdeArchivo.getNumPartitions()

# COMMAND ----------

rddDesdeArchivo.saveAsTextFile(path+"out/out.csv")

# COMMAND ----------

rdd = sc.wholeTextFiles(path+"out/out.csv/*")

# COMMAND ----------

rdd.take(5)

# COMMAND ----------

lista = rdd.mapValues(lambda x : x.split()).collect()

# COMMAND ----------

lista = [l[0] for l in lista ]
lista.sort()

# COMMAND ----------

rdd2 = sc.textFile(','.join(lista),10).map(lambda l : l.split(","))

# COMMAND ----------

rdd.take(5)