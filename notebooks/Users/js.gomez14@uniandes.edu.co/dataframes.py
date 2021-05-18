# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType,StringType,FloatType
from pyspark.sql.types import Row

# COMMAND ----------

path = "/FileStore/tables/"

# COMMAND ----------

juegoSchema = StructType([
  StructField("juego_id", IntegerType(),False),
  StructField("annio", StringType(),False),
  StructField("temporada", StringType(),False),
  StructField("ciudad", StringType(),False)
])

juegoDF = sqlContext.read.schema(juegoSchema) \
    .option("header", "true").csv(path+"juegos.csv")

# COMMAND ----------

paisSchema = StructType([
  StructField("id", IntegerType(),False),
  StructField("equipo", StringType(),False),
  StructField("sigla", StringType(),False)
])
paisesDF = sqlContext.read.schema(paisSchema) \
    .option("header", "true").csv(path+"paises.csv")
paisesDF.show(5)

# COMMAND ----------

equipoSchema = StructType([
  StructField("id", IntegerType(),False),
  StructField("equipo", StringType(),False),
  StructField("sigla", StringType(),False)
])
equiposDF = sqlContext.read.schema(equipoSchema) \
    .option("header", "true").csv(path+"paises.csv")
equiposDF.show(5)

# COMMAND ----------

#['resultado_id', 'medalla', 'deportista_id', 'juego_id', 'evento_id']
resultadosSchema = StructType([
  StructField("resultado_id", IntegerType(),False),
  StructField("medalla", StringType(),False),
  StructField("deportista_id", IntegerType(),False),
  StructField("juego_id", IntegerType(),False),
  StructField("evento_id", IntegerType(),False),
])
resultadosDF = sqlContext.read.schema(resultadosSchema) \
    .option("header", "true").csv(path+"resultados.csv")
resultadosDF.show(5)

# COMMAND ----------

juegoDF.show(5)

# COMMAND ----------

deportistaOlimpicoRDD = sc.textFile(path+"deportista.csv") \
    .map(lambda line : line.split(","))
deportistaOlimpicoRDD2 = sc.textFile(path+"deportista2.csv") \
    .map(lambda line : line.split(","))

# COMMAND ----------

deportistaOlimpicoRDD = deportistaOlimpicoRDD \
    .union(deportistaOlimpicoRDD2)

# COMMAND ----------

def removeHeader(index, iterator):
  return iter(list(iterator)[1:])

# COMMAND ----------

deportistaOlimpicoRDD=deportistaOlimpicoRDD.mapPartitionsWithIndex(removeHeader)

# COMMAND ----------

deportistaOlimpicoRDD.take(5)

# COMMAND ----------

deportistaOlimpicoRDD = deportistaOlimpicoRDD.map(lambda l : (
int(l[0]),
l[1],
int(l[2]),
int(l[3]),
int(l[4]),
float(l[5]),
int(l[6])
))

# COMMAND ----------

schema = StructType([
  StructField("deportista_id", IntegerType(), False),
  StructField("nombre", StringType(), False),
  StructField("genero", IntegerType(), False),
  StructField("edad", IntegerType(), False),
  StructField("altura", IntegerType(), False),
  StructField("peso", FloatType(), False),
  StructField("equipo_id", IntegerType(), False),
])

# COMMAND ----------

deportistasDF = sqlContext.createDataFrame(deportistaOlimpicoRDD, schema)

# COMMAND ----------

deportistasDF.show(5)

# COMMAND ----------

deporteSchema = StructType([
  StructField("deporte_id", IntegerType(), False),
  StructField("deporte", StringType(), False)
])
deportesDF = sqlContext.read.option("header",True) \
    .schema(deporteSchema).csv(path+"deporte.csv")
deportesDF.show(5)

# COMMAND ----------

deportesDF.printSchema()

# COMMAND ----------

deporteSchema = StructType([
  StructField("deporte_id", IntegerType(), False),
  StructField("deporte", StringType(), False)
])
deportesDF = sqlContext.read.option("header",True) \
    .schema(deporteSchema).csv(path+"deporte.csv")
deportesDF.show(5)

# COMMAND ----------

eventoSchema = StructType([
  StructField("evento_id", IntegerType(), False),
  StructField("evento", StringType(), False),
  StructField("deporte_id", IntegerType(), False),
])
eventosDF = sqlContext.read.option("header",True) \
    .schema(eventoSchema).csv(path+"evento.csv")
eventosDF.show(5)

# COMMAND ----------

deportistasDF.printSchema()

# COMMAND ----------

deportistasDF=deportistasDF.withColumnRenamed("genero","sexo").drop("altura")
deportistasDF.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
deportistasDF=deportistasDF.select("deportista_id",
                      "nombre", 
                     col("edad").alias("edad_al_jugar"),
                     "equipo_id")


# COMMAND ----------

deportistasDF.show(5)

# COMMAND ----------

deportistasDF=deportistasDF.filter((deportistasDF.edad_al_jugar != 0)) \
    .sort("edad_al_jugar")

deportistasDF.printSchema()

# COMMAND ----------

resultadosDF.printSchema()

# COMMAND ----------

juegoDF.printSchema()

# COMMAND ----------

deportesDF.printSchema()

# COMMAND ----------

deportistasDF.join(
    resultadosDF, 
    deportistasDF.deportista_id == resultadosDF.deportista_id,
    "left"
  ) \
  .join(
    juegoDF,
    juegoDF.juego_id == resultadosDF.juego_id,
    "left"
  ) \
  .join(
    eventosDF,
    eventosDF.evento_id == resultadosDF.evento_id,
    "left"
  ) \
  .select(deportistasDF.nombre,
          "edad_al_jugar",
          "medalla",col("annio").alias("annio_de_juego"),
         eventosDF.evento.alias("nombre_disciplina")).show()

# COMMAND ----------

deportistasDF.join(
    resultadosDF, 
    deportistasDF.deportista_id == resultadosDF.deportista_id,
    "left"
  ) \
  .join(
    juegoDF,
    juegoDF.juego_id == resultadosDF.juego_id,
    "left"
  ) \
  .join(
    eventosDF,
    eventosDF.evento_id == resultadosDF.evento_id,
    "left"
  ) \
  .select(deportistasDF.nombre,
          "edad_al_jugar",
          "medalla",
          col("annio").alias("annio_de_juego"),
         eventosDF.evento.alias("nombre_disciplina")).show()

# COMMAND ----------

deportistasDF\
  .join(
      resultadosDF,
      deportistasDF.deportista_id == resultadosDF.deportista_id,
      "left"
    ) \
  .join(
    equiposDF,
    deportistasDF.equipo_id == equiposDF.id,
    "left"
    ) \
  .where(resultadosDF.medalla != 'NA') \
  .select(deportistasDF.nombre,
          "edad_al_jugar",
          "medalla",
          equiposDF.equipo,
          col("sigla").alias("pais_equipo")
         ) \
  .show()
  
  

# COMMAND ----------

medallistaXAnnio = deportistasDF\
  .join(
      resultadosDF,
      deportistasDF.deportista_id == resultadosDF.deportista_id,
      "left"
    ) \
  .join(
      juegoDF,
      juegoDF.juego_id == resultadosDF.juego_id,
      "left"
    ) \
  .join(
    paisesDF,
    deportistasDF.equipo_id == paisesDF.id,
    "left"
    ) \
  .join(
    eventosDF,
    eventosDF.evento_id == resultadosDF.evento_id,
    "left"
    ) \
  .join(
    deportesDF,
    deportesDF.deporte_id == eventosDF.deporte_id,
    "left"
    ) \
  .select("sigla",
          "annio",
          "medalla",
          eventosDF.evento.alias("nombre_subdisciplina"),
          deportesDF.deporte.alias("nombre_disciplina"),
          deportistasDF.nombre.alias("nombre_deportista"),
         )
  

# COMMAND ----------

medallistaXAnnio.show(5)

# COMMAND ----------

cuenta_medallas_paises_xannio=medallistaXAnnio.where(medallistaXAnnio.medalla != "NA") \
  .groupBy("sigla", "annio", "nombre_subdisciplina") \
  .count() \
  .sort("annio")
cuenta_medallas_paises_xannio.show(5)

# COMMAND ----------

cuenta_medallas_paises_xannio.printSchema()

# COMMAND ----------

cuenta_medallas_paises_xannio.groupBy("sigla","annio") \
    .agg(sum("count").alias("total_medallas"),
        avg("count").alias("promedio_medallas")).show()

# COMMAND ----------

resultadosDF.registerTempTable("Resultado")
deportistasDF.registerTempTable("Deportista")
paisesDF.registerTempTable("Pais")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM Resultado

# COMMAND ----------

sqlContext.sql("""
SELECT medalla, equipo, sigla
FROM Resultado AS R
INNER JOIN Deportista AS D ON D.deportista_id = R.deportista_id
INNER JOIN Pais AS P ON P.id = D.equipo_id
WHERE medalla != 'NA'
ORDER BY 3 DESC
""").show() 

# COMMAND ----------

deportistaError = sc.textFile(path+"deportistaError.csv") \
    .map(lambda l : l.split(","))

# COMMAND ----------

def elimina_encabezado(indice, iterador):
  return iter(list(iterador)[1:])

# COMMAND ----------

deportistaError=deportistaError.mapPartitionsWithIndex(elimina_encabezado)

# COMMAND ----------

deportistaError.take(2)

# COMMAND ----------

deportistaError=deportistaError.map(lambda l:(
                   l[0],
                   l[1],
                   l[2],
                   l[3],
                   l[4],
                   l[5],
                   l[6]))

schemaDepError = StructType([
  StructField("deportista_id", StringType(),False),
  StructField("nombre",StringType(),False),
  StructField("genero",StringType(), False),
  StructField("edad", StringType(), False),
  StructField("altura",StringType(),False),
  StructField("peso",StringType(),False),
  StructField("equipo_id",StringType(),True),
])

deportistaErrorDF = sqlContext.createDataFrame(deportistaError,schemaDepError)


# COMMAND ----------

deportistaErrorDF.show()

# COMMAND ----------

# UDF
def conversion_enteros(valor):
  return int(valor) if len(valor) > 0 else None

conversion_enteros_udf = udf(lambda z: conversion_enteros(z), IntegerType())
sqlContext.udf.register("conversion_enteros_udf", conversion_enteros_udf)



# COMMAND ----------

deportistaErrorDF.select(conversion_enteros_udf("altura")).show()



# COMMAND ----------

from pyspark.storagelevel import StorageLevel

medallistaXAnnio.is_cached

# COMMAND ----------

medallistaXAnnio.rdd.cache()

# COMMAND ----------

medallistaXAnnio.rdd.getStorageLevel()

# COMMAND ----------

medallistaXAnnio.rdd.unpersist()

# COMMAND ----------

medallistaXAnnio.rdd.persist(StorageLevel.MEMORY_AND_DISK_2)

# COMMAND ----------

StorageLevel.MEMORY_AND_DISK_3 = StorageLevel(True,True,False,False,3)
medallistaXAnnio.rdd.unpersist()

# COMMAND ----------

medallistaXAnnio.rdd.persist(StorageLevel.MEMORY_AND_DISK_3)