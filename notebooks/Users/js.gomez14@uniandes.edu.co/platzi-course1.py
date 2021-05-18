# Databricks notebook source
# ex = spark.sql("select COUNT(*) from example_data")
# display(ex.select("*"))

# diamonds = spark.table("diamonds")
# display(diamonds.select("*"))

# COMMAND ----------

rdd1 = sc.parallelize([1,2,3])
type(rdd1)

# COMMAND ----------

rdd1.collect()

# COMMAND ----------

path = "/FileStore/tables/"

equiposOlimpicosRDD = sc.textFile(path+"paises.csv") \
    .map(lambda line : line.split(","))

# COMMAND ----------

equiposOlimpicosRDD.take(15)

# COMMAND ----------

resp = equiposOlimpicosRDD.map(lambda x : x[2]).distinct().filter(lambda y : y != 'sigla').count()
resp

# COMMAND ----------

equiposOlimpicosRDD.map(lambda x : (x[2],x[1])).groupByKey() \
    .mapValues(len).take(5)

# COMMAND ----------

equiposOlimpicosRDD.map(lambda x : (x[2],x[1])).groupByKey() \
    .mapValues(list).take(5)

# COMMAND ----------

equiposArg = equiposOlimpicosRDD.filter(lambda l : "ARG" in l)
equiposArg.collect()

# COMMAND ----------

equiposOlimpicosRDD.count()

# COMMAND ----------

equiposOlimpicosRDD.countApprox(20)

# COMMAND ----------

deportistaOlimpicoRDD = sc.textFile(path+"deportista.csv") \
    .map(lambda line : line.split(","))
deportistaOlimpicoRDD2 = sc.textFile(path+"deportista2.csv") \
    .map(lambda line : line.split(","))

# COMMAND ----------

deportistaOlimpicoRDD = deportistaOlimpicoRDD \
    .union(deportistaOlimpicoRDD2)

# COMMAND ----------

deportistaOlimpicoRDD.count()

# COMMAND ----------

deportistaOlimpicoRDD.top(2)

# COMMAND ----------

equiposOlimpicosRDD.top(2)

# COMMAND ----------

deportistas_equipos = deportistaOlimpicoRDD.map(lambda l: [l[-1],l[:-1]]) \
      .join(equiposOlimpicosRDD.map(lambda x : [x[0],x[2]]))
deportistas_equipos.take(6)

# COMMAND ----------

deportistas_equipos.takeSample(False,6,25)

# COMMAND ----------

resultadosOlimpicoRDD = sc.textFile(path+"resultados.csv") \
    .map(lambda line : line.split(","))

# COMMAND ----------

resultadosGanadoresRDD = resultadosOlimpicoRDD.filter(lambda l : "NA" not in l[1])

# COMMAND ----------

resultadosGanadoresRDD.take(2)

# COMMAND ----------

deportistas_equipos_resultados = deportistas_equipos \
      .join(resultadosGanadoresRDD)
deportistas_equipos_resultados.take(6)
      

# COMMAND ----------

valores_medallas = {'Gold': 7,
'Silver': 5,
'Bronze' : 4}

# COMMAND ----------

paises_medallas = deportistas_equipos_resultados \
    .map(lambda x: (x[1][0][-1], valores_medallas[x[1][1]]))

# COMMAND ----------

from operator import add
conclusion = paises_medallas.reduceByKey((add)) \
    .sortBy(lambda x : x[1], ascending = False)


# COMMAND ----------

conclusion.take(10)