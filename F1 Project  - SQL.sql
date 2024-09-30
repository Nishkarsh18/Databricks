-- Databricks notebook source
create database if not exists f1

-- COMMAND ----------

show databases

-- COMMAND ----------

describe database f1

-- COMMAND ----------

describe database extended f1

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use database f1

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Managed Tables

-- COMMAND ----------

show tables

-- COMMAND ----------

show tables in default

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df = spark.read.option('header',True).csv('dbfs:/FileStore/Formula1/races.csv').write.mode('overwrite').format('parquet').saveAsTable('RacesTable')

-- COMMAND ----------

select * from RacesTable

-- COMMAND ----------

describe extended RacesTable

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df = spark.read.option('header',True).csv('dbfs:/FileStore/Formula1/races.csv').write.mode('overwrite').saveAsTable('RacesTable2')

-- COMMAND ----------

describe extended RacesTable2

-- COMMAND ----------

create table newtable 
as 
select * from RacesTable

-- COMMAND ----------

describe extended newtable

-- COMMAND ----------

describe history newtable

-- COMMAND ----------

describe history RacesTable2

-- COMMAND ----------

drop table newtable

-- COMMAND ----------

drop table RacesTable2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df = spark.read.option('header',True).csv('dbfs:/FileStore/Formula1/races.csv').write.mode('overwrite').option('path','dbfs:/FileStore/ExternalTableCre').format('parquet').saveAsTable('RacesTable')

-- COMMAND ----------

describe extended RacesTable

-- COMMAND ----------

create table demo
(
  id string , 
  name string
)

-- COMMAND ----------

describe extended demo


-- COMMAND ----------

drop table demo

-- COMMAND ----------

create table demo
(
  id string , 
  name string
)
using parquet
location 'dbfs:/FileStore/demosql'

-- COMMAND ----------

insert into demo 
values ('id1','Nishkarsh')

-- COMMAND ----------

create table demo2
(
  id string , 
  name string
)
using parquet
location 'dbfs:/FileStore/demosql2'

-- COMMAND ----------

insert into demo2 
select * from demo

-- COMMAND ----------

create table demo3
(
  id string , 
  name string
)
using parquet
location 'dbfs:/FileStore/demosql'

-- COMMAND ----------

select * from demo3

-- COMMAND ----------

create temp view tempview
as 
select * from demo3

-- COMMAND ----------

create global temporary view globalview
as
select * from demo3

-- COMMAND ----------

create view perview
as 
select * from demo3

-- COMMAND ----------

select * from global_temp.globalview

-- COMMAND ----------

create database d1
location 'dbfs:/FileStore/db'

-- COMMAND ----------

use database d1

-- COMMAND ----------

create table demo4
(
  id string , 
  name string
)

-- COMMAND ----------

describe extended demo4

-- COMMAND ----------

desc database d1

-- COMMAND ----------


