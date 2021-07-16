-- Databricks notebook source
-- MAGIC %py
-- MAGIC username = "Yanse"
-- MAGIC health_tracker = f"/dbacademy/{username}/DLRS/healthtracker/"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS dbacademy;
USE dbacademy

-- COMMAND ----------

SET spark.sql.shuffle.partitions=8

-- COMMAND ----------

SELECT * FROM health_tracker_data_2020_01

-- COMMAND ----------

SELECT * FROM health_tracker_data_2020_01

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC 
-- MAGIC rm -r /dbacademy/DLRS/healthtracker/silver

-- COMMAND ----------

DROP TABLE IF EXISTS health_tracker_silver;               -- ensures that if we run this again, it won't fail
                                                          
CREATE TABLE health_tracker_silver                        
USING PARQUET                                             
PARTITIONED BY (p_device_id)                              -- column used to partition the data
LOCATION "/dbacademy/DLRS/healthtracker/silver"           -- location where the parquet files will be saved
AS (                                                      
  SELECT name,                                            -- query used to transform the raw data
         heartrate,                                       
         CAST(FROM_UNIXTIME(time) AS TIMESTAMP) AS time,  
         CAST(FROM_UNIXTIME(time) AS DATE) AS dte,        
         device_id AS p_device_id                         
  FROM health_tracker_data_2020_01   
)

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver

-- COMMAND ----------

DESCRIBE DETAIL health_tracker_silver

-- COMMAND ----------

CONVERT TO DELTA 
  parquet.`/dbacademy/DLRS/healthtracker/silver` 
  PARTITIONED BY (p_device_id double)

-- COMMAND ----------

DROP TABLE IF EXISTS health_tracker_silver;

CREATE TABLE health_tracker_silver
USING DELTA
LOCATION "/dbacademy/DLRS/healthtracker/silver"

-- COMMAND ----------

DESCRIBE DETAIL health_tracker_silver

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC 
-- MAGIC rm -r /dbacademy/DLRS/healthtracker/gold/health_tracker_user_analytics

-- COMMAND ----------

DROP TABLE IF EXISTS health_tracker_user_analytics;

CREATE TABLE health_tracker_user_analytics
USING DELTA
LOCATION '/dbacademy/DLRS/healthtracker/gold/health_tracker_user_analytics'
AS (
  SELECT p_device_id, 
         AVG(heartrate) AS avg_heartrate,
         STD(heartrate) AS std_heartrate,
         MAX(heartrate) AS max_heartrate 
  FROM health_tracker_silver GROUP BY p_device_id
)

-- COMMAND ----------

SELECT * FROM health_tracker_user_analytics

-- COMMAND ----------

INSERT INTO health_tracker_silver
SELECT name,
       heartrate,
       CAST(FROM_UNIXTIME(time) AS TIMESTAMP) AS time,
       CAST(FROM_UNIXTIME(time) AS DATE) AS dte,
       device_id as p_device_id
FROM health_tracker_data_2020_02

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver VERSION AS OF 0

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver

-- COMMAND ----------

SELECT p_device_id, COUNT(*) FROM health_tracker_silver GROUP BY p_device_id

-- COMMAND ----------

SELECT * FROM health_tracker_silver WHERE p_device_id IN (3, 4)

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW broken_readings
AS (
  SELECT COUNT(*) as broken_readings_count, dte FROM health_tracker_silver
  WHERE heartrate < 0
  GROUP BY dte
  ORDER BY dte
)

-- COMMAND ----------

SELECT * FROM broken_readings

-- COMMAND ----------

SELECT SUM(broken_readings_count) FROM broken_readings

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW updates 
AS (
  SELECT name, (prev_amt+next_amt)/2 AS heartrate, time, dte, p_device_id
  FROM (
    SELECT *, 
    LAG(heartrate) OVER (PARTITION BY p_device_id, dte ORDER BY p_device_id, dte) AS prev_amt, 
    LEAD(heartrate) OVER (PARTITION BY p_device_id, dte ORDER BY p_device_id, dte) AS next_amt 
    FROM health_tracker_silver
  ) 
  WHERE heartrate < 0
)

-- COMMAND ----------

DESCRIBE health_tracker_silver

-- COMMAND ----------

DESCRIBE updates

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW inserts 
AS (
    SELECT name, 
    heartrate,
    CAST(FROM_UNIXTIME(time) AS timestamp) AS time,
    CAST(FROM_UNIXTIME(time) AS date) AS dte,
    device_id
    FROM health_tracker_data_2020_02_01
   )

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW upserts
AS (
    SELECT * FROM updates 
    UNION ALL 
    SELECT * FROM inserts
    )

-- COMMAND ----------


MERGE INTO health_tracker_silver                            -- the MERGE instruction is used to perform the upsert
USING upserts

ON health_tracker_silver.time = upserts.time AND        
   health_tracker_silver.p_device_id = upserts.p_device_id  -- ON is used to describe the MERGE condition
   
WHEN MATCHED THEN                                           -- WHEN MATCHED describes the update behavior
  UPDATE SET
  health_tracker_silver.heartrate = upserts.heartrate   
WHEN NOT MATCHED THEN                                       -- WHEN NOT MATCHED describes the insert behavior
  INSERT (name, heartrate, time, dte, p_device_id)              
  VALUES (name, heartrate, time, dte, p_device_id)

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver VERSION AS OF 1

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver

-- COMMAND ----------

DESCRIBE HISTORY health_tracker_silver

-- COMMAND ----------

SELECT SUM(broken_readings_count) FROM broken_readings

-- COMMAND ----------

SELECT sum(broken_readings_count) FROM broken_readings WHERE dte < '2020-02-25'

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW upserts
AS (
  SELECT * FROM updates
)

-- COMMAND ----------

SELECT COUNT(*) FROM upserts

-- COMMAND ----------

MERGE INTO health_tracker_silver                            -- the MERGE instruction is used to perform the upsert
USING upserts

ON health_tracker_silver.time = upserts.time AND        
   health_tracker_silver.p_device_id = upserts.p_device_id  -- ON is used to describe the MERGE condition
   
WHEN MATCHED THEN                                           -- WHEN MATCHED describes the update behavior
  UPDATE SET
  health_tracker_silver.heartrate = upserts.heartrate   
WHEN NOT MATCHED THEN                                       -- WHEN NOT MATCHED describes the insert behavior
  INSERT (name, heartrate, time, dte, p_device_id)              
  VALUES (name, heartrate, time, dte, p_device_id)

-- COMMAND ----------

SELECT SUM(broken_readings_count)FROM broken_readings

-- COMMAND ----------

DELETE FROM health_tracker_silver where p_device_id = 4

-- COMMAND ----------

DESCRIBE HISTORY health_tracker_silver

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver VERSION AS OF 0

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver VERSION AS OF 1

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver VERSION AS OF 2

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver VERSION AS OF 3

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver VERSION AS OF 4

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC 
-- MAGIC ls /dbacademy/DLRS/healthtracker/silver/_delta_log

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW upserts
AS (
  SELECT NULL AS name, heartrate, time, dte, p_device_id 
  FROM health_tracker_silver VERSION AS OF 3
  WHERE p_device_id = 4
)

-- COMMAND ----------

MERGE INTO health_tracker_silver                            -- the MERGE instruction is used to perform the upsert
USING upserts

ON health_tracker_silver.time = upserts.time AND        
   health_tracker_silver.p_device_id = upserts.p_device_id  -- ON is used to describe the MERGE condition
   
WHEN MATCHED THEN                                           -- WHEN MATCHED describes the update behavior
  UPDATE SET
  health_tracker_silver.heartrate = upserts.heartrate   
WHEN NOT MATCHED THEN                                       -- WHEN NOT MATCHED describes the insert behavior
  INSERT (name, heartrate, time, dte, p_device_id)              
  VALUES (name, heartrate, time, dte, p_device_id)

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver

-- COMMAND ----------

DESCRIBE HISTORY health_tracker_silver

-- COMMAND ----------

SELECT * FROM health_tracker_silver WHERE p_device_id = 4

-- COMMAND ----------

SELECT * FROM health_tracker_silver VERSION AS OF 2 WHERE p_device_id = 4

-- COMMAND ----------

VACUUM health_tracker_silver RETAIN 0 Hours

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

SELECT * FROM health_tracker_silver VERSION AS OF 3 WHERE p_device_id = 4
