# 2-3,2小时入门SparkSQL编程


<!-- #region -->
本节将介绍SparkSQL编程基本概念和基本用法。

不同于RDD编程的命令式编程范式，SparkSQL编程是一种声明式编程范式，我们可以通过SQL语句或者调用DataFrame的相关API描述我们想要实现的操作。

然后Spark会将我们的描述进行语法解析，找到相应的执行计划并对其进行流程优化，然后调用相应基础命令进行执行。

我们使用pyspark进行RDD编程时，在Excutor上跑的很多时候就是Python代码，当然，少数时候也会跑java字节码。

但我们使用pyspark进行SparkSQL编程时，在Excutor上跑的全部是java字节码，pyspark在Driver端就将相应的Python代码转换成了java任务然后放到Excutor上执行。


因此，使用SparkSQL的编程范式进行编程，我们能够取得几乎和直接使用scala/java进行编程相当的效率(忽略语法解析时间差异)。此外SparkSQL提供了非常方便的数据读写API，我们可以用它和Hive表，HDFS，mysql表，Cassandra，Hbase等各种存储媒介进行数据交换。

美中不足的是，SparkSQL的灵活性会稍差一些，其默认支持的数据类型通常只有 Int,Long,Float,Double,String,Boolean 等这些标准SQL数据类型, 类型扩展相对繁琐。对于一些较为SQL中不直接支持的功能，通常可以借助于用户自定义函数(UDF)来实现，如果功能更加复杂，则可以转成RDD来进行实现。

本节我们将主要介绍以下主要内容：

* RDD和DataFrame的对比

* 创建DataFrame

* DataFrame保存成文件

* DataFrame的API交互

* DataFrame的SQL交互

<!-- #endregion -->

```python
import findspark

#指定spark_home为刚才的解压路径,指定python路径
spark_home = "/Users/liangyun/ProgramFiles/spark-3.0.1-bin-hadoop3.2"
python_path = "/Users/liangyun/anaconda3/bin/python"
findspark.init(spark_home,python_path)

import pyspark 
from pyspark.sql import SparkSession

#SparkSQL的许多功能封装在SparkSession的方法接口中

spark = SparkSession.builder \
        .appName("test") \
        .config("master","local[4]") \
        .enableHiveSupport() \
        .getOrCreate()

sc = spark.sparkContext


```

### 一，RDD，DataFrame和DataSet对比


DataFrame参照了Pandas的思想，在RDD基础上增加了schma，能够获取列名信息。

DataSet在DataFrame基础上进一步增加了数据类型信息，可以在编译时发现类型错误。

DataFrame可以看成DataSet[Row]，两者的API接口完全相同。

DataFrame和DataSet都支持SQL交互式查询，可以和 Hive无缝衔接。

DataSet只有Scala语言和Java语言接口中才支持，在Python和R语言接口只支持DataFrame。

DataFrame数据结构本质上是通过RDD来实现的，但是RDD是一种行存储的数据结构，而DataFrame是一种列存储的数据结构。




### 二，创建DataFrame


**1，通过toDF方法转换成DataFrame**

可以将RDD用toDF方法转换成DataFrame


```python
#将RDD转换成DataFrame
rdd = sc.parallelize([("LiLei",15,88),("HanMeiMei",16,90),("DaChui",17,60)])
df = rdd.toDF(["name","age","score"])
df.show()
df.printSchema()
```

```
+---------+---+-----+
|     name|age|score|
+---------+---+-----+
|    LiLei| 15|   88|
|HanMeiMei| 16|   90|
|   DaChui| 17|   60|
+---------+---+-----+

root
 |-- name: string (nullable = true)
 |-- age: long (nullable = true)
 |-- score: long (nullable = true)
```

```python

```

**2, 通过createDataFrame方法将Pandas.DataFrame转换成pyspark中的DataFrame**

```python
import pandas as pd 

pdf = pd.DataFrame([("LiLei",18),("HanMeiMei",17)],columns = ["name","age"])
df = spark.createDataFrame(pdf)
df.show()
```

```
+---------+---+
|     name|age|
+---------+---+
|    LiLei| 18|
|HanMeiMei| 17|
+---------+---+
```

```python

```

```python
# 也可以对列表直接转换
values = [("LiLei",18),("HanMeiMei",17)]
df = spark.createDataFrame(values,["name","age"])
df.show()
```

```
+---------+---+
|     name|age|
+---------+---+
|    LiLei| 18|
|HanMeiMei| 17|
+---------+---+
```

```python

```

**4, 通过createDataFrame方法指定schema动态创建DataFrame**

可以通过createDataFrame的方法指定rdd和schema创建DataFrame。

这种方法比较繁琐，但是可以在预先不知道schema和数据类型的情况下在代码中动态创建DataFrame.


```python
from pyspark.sql.types import *
from pyspark.sql import Row
from datetime import datetime

schema = StructType([StructField("name", StringType(), nullable = False),
                     StructField("score", IntegerType(), nullable = True),
                     StructField("birthday", DateType(), nullable = True)])

rdd = sc.parallelize([Row("LiLei",87,datetime(2010,1,5)),
                      Row("HanMeiMei",90,datetime(2009,3,1)),
                      Row("DaChui",None,datetime(2008,7,2))])

dfstudent = spark.createDataFrame(rdd, schema)

dfstudent.show()
```

```
+---------+-----+----------+
|     name|score|  birthday|
+---------+-----+----------+
|    LiLei|   87|2010-01-05|
|HanMeiMei|   90|2009-03-01|
|   DaChui| null|2008-07-02|
+---------+-----+----------+

```

```python

```

**4，通过读取文件创建**

可以读取json文件，csv文件，hive数据表或者mysql数据表得到DataFrame。

```python
#读取json文件生成DataFrame
df = spark.read.json("data/people.json")
df.show()
```

```
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+
```

```python
#读取csv文件
df = spark.read.option("header","true") \
 .option("inferSchema","true") \
 .option("delimiter", ",") \
 .csv("data/iris.csv")
df.show(5)
df.printSchema()
```

```
+-----------+----------+-----------+----------+-----+
|sepallength|sepalwidth|petallength|petalwidth|label|
+-----------+----------+-----------+----------+-----+
|        5.1|       3.5|        1.4|       0.2|    0|
|        4.9|       3.0|        1.4|       0.2|    0|
|        4.7|       3.2|        1.3|       0.2|    0|
|        4.6|       3.1|        1.5|       0.2|    0|
|        5.0|       3.6|        1.4|       0.2|    0|
+-----------+----------+-----------+----------+-----+
only showing top 5 rows

root
 |-- sepallength: double (nullable = true)
 |-- sepalwidth: double (nullable = true)
 |-- petallength: double (nullable = true)
 |-- petalwidth: double (nullable = true)
 |-- label: integer (nullable = true)
```

```python
#读取csv文件
df = spark.read.format("com.databricks.spark.csv") \
 .option("header","true") \
 .option("inferSchema","true") \
 .option("delimiter", ",") \
 .load("data/iris.csv")
df.show(5)
df.printSchema()
```

```
+-----------+----------+-----------+----------+-----+
|sepallength|sepalwidth|petallength|petalwidth|label|
+-----------+----------+-----------+----------+-----+
|        5.1|       3.5|        1.4|       0.2|    0|
|        4.9|       3.0|        1.4|       0.2|    0|
|        4.7|       3.2|        1.3|       0.2|    0|
|        4.6|       3.1|        1.5|       0.2|    0|
|        5.0|       3.6|        1.4|       0.2|    0|
+-----------+----------+-----------+----------+-----+
only showing top 5 rows

root
 |-- sepallength: double (nullable = true)
 |-- sepalwidth: double (nullable = true)
 |-- petallength: double (nullable = true)
 |-- petalwidth: double (nullable = true)
 |-- label: integer (nullable = true)
```

```python

```

```python
#读取parquet文件
df = spark.read.parquet("data/users.parquet")
df.show()
```

```
+------+--------------+----------------+
|  name|favorite_color|favorite_numbers|
+------+--------------+----------------+
|Alyssa|          null|  [3, 9, 15, 20]|
|   Ben|           red|              []|
+------+--------------+----------------+

```

```python


```

```python
#读取hive数据表生成DataFrame

spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
spark.sql("LOAD DATA LOCAL INPATH 'data/kv1.txt' INTO TABLE src")
df = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")
df.show(5)

```

```
+---+-----+
|key|value|
+---+-----+
|  0|val_0|
|  0|val_0|
|  0|val_0|
|  0|val_0|
|  0|val_0|
+---+-----+
only showing top 5 rows
```

```python
#读取mysql数据表生成DataFrame
"""
url = "jdbc:mysql://localhost:3306/test"
df = spark.read.format("jdbc") \
 .option("url", url) \
 .option("dbtable", "runoob_tbl") \
 .option("user", "root") \
 .option("password", "0845") \
 .load()\
df.show()
"""

```

### 四，DataFrame保存成文件


可以保存成csv文件，json文件，parquet文件或者保存成hive数据表

```python
#保存成csv文件
df = spark.read.format("json").load("data/people.json")
df.write.format("csv").option("header","true").save("data/people_write.csv")
```

```python
#先转换成rdd再保存成txt文件
df.rdd.saveAsTextFile("data/people_rdd.txt")
```

```python
#保存成json文件
df.write.json("data/people_write.json")
```

```python
#保存成parquet文件, 压缩格式, 占用存储小, 且是spark内存中存储格式，加载最快
df.write.partitionBy("age").format("parquet").save("data/namesAndAges.parquet")
df.write.parquet("data/people_write.parquet")
```

```python
#保存成hive数据表
df.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")

```

```python

```

### 五，DataFrame的API交互

```python
from pyspark.sql import Row
from pyspark.sql.functions import * 

df = spark.createDataFrame(
    [("LiLei",15,"male"),
     ("HanMeiMei",16,"female"),
     ("DaChui",17,"male")]).toDF("name","age","gender")

df.show()
df.printSchema()

```

```
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|    LiLei| 15|  male|
|HanMeiMei| 16|female|
|   DaChui| 17|  male|
+---------+---+------+

root
 |-- name: string (nullable = true)
 |-- age: long (nullable = true)
 |-- gender: string (nullable = true)
```


**1，Action操作**


DataFrame的Action操作包括show,count,collect,,describe,take,head,first等操作。

```python
#show
df.show()
```

```
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|    LiLei| 15|  male|
|HanMeiMei| 16|female|
|   DaChui| 17|  male|
+---------+---+------+
```

```python
#show(numRows: Int, truncate: Boolean) 
#第二个参数设置是否当输出字段长度超过20时进行截取
df.show(2,False) 
```

```
+---------+---+------+
|name     |age|gender|
+---------+---+------+
|LiLei    |15 |male  |
|HanMeiMei|16 |female|
+---------+---+------+
only showing top 2 rows
```

```python
#count
df.count()
```

```
3
```

```python
#collect
df.collect()
```

```
[Row(name='LiLei', age=15, gender='male'),
 Row(name='HanMeiMei', age=16, gender='female'),
 Row(name='DaChui', age=17, gender='male')]
```

```python
#first
df.first()
```

```
Row(name='LiLei', age=15, gender='male')
```

```python
#take
df.take(2)
```

```
[Row(name='LiLei', age=15, gender='male'),
 Row(name='HanMeiMei', age=16, gender='female')]
```

```python
#head
df.head(2)
```

```
[Row(name='LiLei', age=15, gender='male'),
 Row(name='HanMeiMei', age=16, gender='female')]

```

```python

```

**2，类RDD操作** 


DataFrame支持RDD中一些诸如distinct,cache,sample,foreach,intersect,except等操作。

可以把DataFrame当做数据类型为Row的RDD来进行操作，必要时可以将其转换成RDD来操作。

```python
df = spark.createDataFrame([("Hello World",),("Hello China",),("Hello Spark",)]).toDF("value")
df.show()
```

```
+-----------+
|      value|
+-----------+
|Hello World|
|Hello China|
|Hello Spark|
+-----------+
```

```python
#map操作，需要先转换成rdd
rdd = df.rdd.map(lambda x:Row(x[0].upper()))
dfmap = rdd.toDF(["value"]).show()
```

```
+-----------+
|      value|
+-----------+
|HELLO WORLD|
|HELLO CHINA|
|HELLO SPARK|
+-----------+
```

```python
#flatMap，需要先转换成rdd
df_flat = df.rdd.flatMap(lambda x:x[0].split(" ")).map(lambda x:Row(x)).toDF(["value"])
df_flat.show()
```

```
+-----+
|value|
+-----+
|Hello|
|World|
|Hello|
|China|
|Hello|
|Spark|
+-----+
```

```python
#filter过滤
df_filter = df.rdd.filter(lambda s:s[0].endswith("Spark")).toDF(["value"])

df_filter.show()
```

```
+-----------+
|      value|
+-----------+
|Hello Spark|
+-----------+
```

```python
# filter和broadcast混合使用
broads = sc.broadcast(["Hello","World"])

df_filter_broad = df_flat.filter(~col("value").isin(broads.value))

df_filter_broad.show() 
```

```
+-----+
|value|
+-----+
|China|
|Spark|
+-----+
```

```python
#distinct
df_distinct = df_flat.distinct()
df_distinct.show() 

```

```
+-----+
|value|
+-----+
|World|
|China|
|Hello|
|Spark|
+-----+
```

```python
#cache缓存
df.cache()
df.unpersist()
```

```python
#sample抽样
dfsample = df.sample(False,0.6,0)

dfsample.show()  
```

```
+-----------+
|      value|
+-----------+
|Hello China|
|Hello Spark|
+-----------+
```

```python
df2 = spark.createDataFrame([["Hello World"],["Hello Scala"],["Hello Spark"]]).toDF("value")
df2.show()
```

```
+-----------+
|      value|
+-----------+
|Hello World|
|Hello Scala|
|Hello Spark|
+-----------+
```

```python
#intersect交集
dfintersect = df.intersect(df2)

dfintersect.show()
```

```
+-----------+
|      value|
+-----------+
|Hello Spark|
|Hello World|
+-----------+
```

```python
#exceptAll补集

dfexcept = df.exceptAll(df2)
dfexcept.show()

```

```
+-----------+
|      value|
+-----------+
|Hello China|
+-----------+
```

```python

```

**3，类Excel操作**


可以对DataFrame进行增加列，删除列，重命名列，排序等操作，去除重复行，去除空行，就跟操作Excel表格一样。

```python
df = spark.createDataFrame([
("LiLei",15,"male"),
("HanMeiMei",16,"female"),
("DaChui",17,"male"),
("RuHua",16,None)
]).toDF("name","age","gender")

df.show()
df.printSchema()
```

```
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|    LiLei| 15|  male|
|HanMeiMei| 16|female|
|   DaChui| 17|  male|
|    RuHua| 16|  null|
+---------+---+------+

root
 |-- name: string (nullable = true)
 |-- age: long (nullable = true)
 |-- gender: string (nullable = true)
```

```python
#增加列
dfnew = df.withColumn("birthyear",-df["age"]+2020)

dfnew.show() 
```

```
+---------+---+------+---------+
|     name|age|gender|birthyear|
+---------+---+------+---------+
|    LiLei| 15|  male|     2005|
|HanMeiMei| 16|female|     2004|
|   DaChui| 17|  male|     2003|
|    RuHua| 16|  null|     2004|
+---------+---+------+---------+
```

```python
#置换列的顺序
dfupdate = dfnew.select("name","age","birthyear","gender")
dfupdate.show()
```

```python
#删除列
dfdrop = df.drop("gender")
dfdrop.show() 
```

```
+---------+---+
|     name|age|
+---------+---+
|    LiLei| 15|
|HanMeiMei| 16|
|   DaChui| 17|
|    RuHua| 16|
+---------+---+
```

```python
#重命名列
dfrename = df.withColumnRenamed("gender","sex")
dfrename.show() 
```

```
+---------+---+------+
|     name|age|   sex|
+---------+---+------+
|    LiLei| 15|  male|
|HanMeiMei| 16|female|
|   DaChui| 17|  male|
|    RuHua| 16|  null|
+---------+---+------+

```

```python
#排序sort，可以指定升序降序
dfsorted = df.sort(df["age"].desc())
dfsorted.show()
```

```
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|   DaChui| 17|  male|
|    RuHua| 16|  null|
|HanMeiMei| 16|female|
|    LiLei| 15|  male|
+---------+---+------+
```

```python
#排序orderby,默认为升序,可以根据多个字段
dfordered = df.orderBy(df["age"].desc(),df["gender"].desc())

dfordered.show()
```

```
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|   DaChui| 17|  male|
|HanMeiMei| 16|female|
|    RuHua| 16|  null|
|    LiLei| 15|  male|
+---------+---+------+
```

```python
#去除nan值行
dfnotnan = df.na.drop()

dfnotnan.show()
```

```
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|    LiLei| 15|  male|
|HanMeiMei| 16|female|
|   DaChui| 17|  male|
+---------+---+------+
```

```python
#填充nan值
df_fill = df.na.fill("female")
df_fill.show()
```

```
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|    LiLei| 15|  male|
|HanMeiMei| 16|female|
|   DaChui| 17|  male|
|    RuHua| 16|female|
+---------+---+------+
```

```python
#替换某些值
df_replace = df.na.replace({"":"female","RuHua":"SiYu"})
df_replace.show()
```

```
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|    LiLei| 15|  male|
|HanMeiMei| 16|female|
|   DaChui| 17|  male|
|     SiYu| 16|  null|
+---------+---+------+
```


```python
#去重，默认根据全部字段
df2 = df.unionAll(df)
df2.show()
dfunique = df2.dropDuplicates()
dfunique.show()
```

```
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|    LiLei| 15|  male|
|HanMeiMei| 16|female|
|   DaChui| 17|  male|
|    RuHua| 16|  null|
|    LiLei| 15|  male|
|HanMeiMei| 16|female|
|   DaChui| 17|  male|
|    RuHua| 16|  null|
+---------+---+------+

+---------+---+------+
|     name|age|gender|
+---------+---+------+
|    RuHua| 16|  null|
|   DaChui| 17|  male|
|HanMeiMei| 16|female|
|    LiLei| 15|  male|
+---------+---+------+
```

```python
#去重,根据部分字段
dfunique_part = df.dropDuplicates(["age"])
dfunique_part.show()
```

```
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|   DaChui| 17|  male|
|    LiLei| 15|  male|
|HanMeiMei| 16|female|
+---------+---+------+

```

```python
#简单聚合操作
dfagg = df.agg({"name":"count","age":"max"})

dfagg.show()
```

```
+-----------+--------+
|count(name)|max(age)|
+-----------+--------+
|          4|      17|
+-----------+--------+

```

```python
#汇总信息
df_desc = df.describe()
df_desc.show()
```

```
+-------+------+-----------------+------+
|summary|  name|              age|gender|
+-------+------+-----------------+------+
|  count|     4|                4|     3|
|   mean|  null|             16.0|  null|
| stddev|  null|0.816496580927726|  null|
|    min|DaChui|               15|female|
|    max| RuHua|               17|  male|
+-------+------+-----------------+------+
```

```python
#频率超过0.5的年龄和性别
df_freq = df.stat.freqItems(("age","gender"),0.5)

df_freq.show()
```

```
+-------------+----------------+
|age_freqItems|gender_freqItems|
+-------------+----------------+
|         [16]|          [male]|
+-------------+----------------+
```

```python

```

**4，类SQL表操作**


类SQL表操作主要包括表查询(select,selectExpr,where),表连接(join,union,unionAll),表分组(groupby,agg,pivot)等操作。

```python
df = spark.createDataFrame([
("LiLei",15,"male"),
("HanMeiMei",16,"female"),
("DaChui",17,"male"),
("RuHua",16,None)]).toDF("name","age","gender")

df.show()
```

```
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|    LiLei| 15|  male|
|HanMeiMei| 16|female|
|   DaChui| 17|  male|
|    RuHua| 16|  null|
+---------+---+------+
```

```python
#表查询select
dftest = df.select("name").limit(2)
dftest.show()
```

```
+---------+
|     name|
+---------+
|    LiLei|
|HanMeiMei|
+---------+
```

```python
dftest = df.select("name",df["age"] + 1)
dftest.show()
```

```
+---------+---------+
|     name|(age + 1)|
+---------+---------+
|    LiLei|       16|
|HanMeiMei|       17|
|   DaChui|       18|
|    RuHua|       17|
+---------+---------+
```

```python
#表查询select
dftest = df.select("name",-df["age"]+2020).toDF("name","birth_year")
dftest.show()
```

```
+---------+----------+
|     name|birth_year|
+---------+----------+
|    LiLei|      2005|
|HanMeiMei|      2004|
|   DaChui|      2003|
|    RuHua|      2004|
+---------+----------+
```

```python
#表查询selectExpr,可以使用UDF函数，指定别名等
import datetime
spark.udf.register("getBirthYear",lambda age:datetime.datetime.now().year-age)
dftest = df.selectExpr("name", "getBirthYear(age) as birth_year" , "UPPER(gender) as gender" )
dftest.show()
```

```
+---------+----------+------+
|     name|birth_year|gender|
+---------+----------+------+
|    LiLei|      2005|  MALE|
|HanMeiMei|      2004|FEMALE|
|   DaChui|      2003|  MALE|
|    RuHua|      2004|  null|
+---------+----------+------+
```

```python
#表查询where, 指定SQL中的where字句表达式
dftest = df.where("gender='male' and age>15")
dftest.show()
```

```
+------+---+------+
|  name|age|gender|
+------+---+------+
|DaChui| 17|  male|
+------+---+------+
```

```python
#表查询filter
dftest = df.filter(df["age"]>16)
dftest.show()
```

```
+------+---+------+
|  name|age|gender|
+------+---+------+
|DaChui| 17|  male|
+------+---+------+
```

```python
#表查询filter
dftest = df.filter("gender ='male'")
dftest.show()
```

```
+------+---+------+
|  name|age|gender|
+------+---+------+
| LiLei| 15|  male|
|DaChui| 17|  male|
+------+---+------+
```

```python
#表连接join
dfscore = spark.createDataFrame([("LiLei","male",88),("HanMeiMei","female",90),("DaChui","male",50)]) \
          .toDF("name","gender","score") 

dfscore.show()
```

```
+---------+------+-----+
|     name|gender|score|
+---------+------+-----+
|    LiLei|  male|   88|
|HanMeiMei|female|   90|
|   DaChui|  male|   50|
+---------+------+-----+
```

```python
#表连接join,根据单个字段
dfjoin = df.join(dfscore.select("name","score"),"name")
dfjoin.show()
```

```
+---------+---+------+-----+
|     name|age|gender|score|
+---------+---+------+-----+
|    LiLei| 15|  male|   88|
|HanMeiMei| 16|female|   90|
|   DaChui| 17|  male|   50|
+---------+---+------+-----+
```

```python
#表连接join,根据多个字段
dfjoin = df.join(dfscore,["name","gender"])
dfjoin.show()
```

```
+---------+------+---+-----+
|     name|gender|age|score|
+---------+------+---+-----+
|HanMeiMei|female| 16|   90|
|   DaChui|  male| 17|   50|
|    LiLei|  male| 15|   88|
+---------+------+---+-----+
```

```python
#表连接join,根据多个字段
#可以指定连接方式为"inner","left","right","outer","semi","full","leftanti","anti"等多种方式
dfjoin = df.join(dfscore,["name","gender"],"right")
dfjoin.show()
```

```
+---------+------+---+-----+
|     name|gender|age|score|
+---------+------+---+-----+
|HanMeiMei|female| 16|   90|
|   DaChui|  male| 17|   50|
|    LiLei|  male| 15|   88|
+---------+------+---+-----+

```

```python
dfjoin = df.join(dfscore,["name","gender"],"outer")
dfjoin.show()
```

```
+---------+------+---+-----+
|     name|gender|age|score|
+---------+------+---+-----+
|HanMeiMei|female| 16|   90|
|   DaChui|  male| 17|   50|
|    LiLei|  male| 15|   88|
|    RuHua|  null| 16| null|
+---------+------+---+-----+
```

```python
#表连接，灵活指定连接关系
dfmark = dfscore.withColumnRenamed("gender","sex")
dfmark.show()
```

```
+---------+------+-----+
|     name|   sex|score|
+---------+------+-----+
|    LiLei|  male|   88|
|HanMeiMei|female|   90|
|   DaChui|  male|   50|
+---------+------+-----+

```

```python
dfjoin = df.join(dfmark,(df["name"] == dfmark["name"]) & (df["gender"]==dfmark["sex"]),
        "inner")
dfjoin.show()
```

```
+---------+---+------+---------+------+-----+
|     name|age|gender|     name|   sex|score|
+---------+---+------+---------+------+-----+
|HanMeiMei| 16|female|HanMeiMei|female|   90|
|   DaChui| 17|  male|   DaChui|  male|   50|
|    LiLei| 15|  male|    LiLei|  male|   88|
+---------+---+------+---------+------+-----+

```

```python
#表合并union
dfstudent = spark.createDataFrame([("Jim",18,"male"),("Lily",16,"female")]).toDF("name","age","gender")
dfstudent.show()
```

```
+----+---+------+
|name|age|gender|
+----+---+------+
| Jim| 18|  male|
|Lily| 16|female|
+----+---+------+
```

```python
dfunion = df.union(dfstudent)
dfunion.show()
```

```
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|    LiLei| 15|  male|
|HanMeiMei| 16|female|
|   DaChui| 17|  male|
|    RuHua| 16|  null|
|      Jim| 18|  male|
|     Lily| 16|female|
+---------+---+------+
```

```python
#表分组 groupBy
from pyspark.sql import functions as F 
dfgroup = df.groupBy("gender").max("age")
dfgroup.show()
```

```
+------+--------+
|gender|max(age)|
+------+--------+
|  null|      16|
|female|      16|
|  male|      17|
+------+--------+
```

```python
#表分组后聚合，groupBy,agg
dfagg = df.groupBy("gender").agg(F.mean("age").alias("mean_age"),
   F.collect_list("name").alias("names"))
dfagg.show()
```

```
+------+--------+---------------+
|gender|mean_age|          names|
+------+--------+---------------+
|  null|    16.0|        [RuHua]|
|female|    16.0|    [HanMeiMei]|
|  male|    16.0|[LiLei, DaChui]|
+------+--------+---------------+

```

```python
#表分组聚合，groupBy,agg
dfagg = df.groupBy("gender").agg(F.expr("avg(age)"),F.expr("collect_list(name)"))
dfagg.show()

```

```
+------+--------+------------------+
|gender|avg(age)|collect_list(name)|
+------+--------+------------------+
|  null|    16.0|           [RuHua]|
|female|    16.0|       [HanMeiMei]|
|  male|    16.0|   [LiLei, DaChui]|
+------+--------+------------------+

```

```python
#表分组聚合，groupBy,agg
df.groupBy("gender","age").agg(F.collect_list(col("name"))).show()
```

```
+------+---+------------------+
|gender|age|collect_list(name)|
+------+---+------------------+
|  male| 15|           [LiLei]|
|  male| 17|          [DaChui]|
|female| 16|       [HanMeiMei]|
|  null| 16|           [RuHua]|
+------+---+------------------+

```

```python
#表分组后透视，groupBy,pivot
dfstudent = spark.createDataFrame([("LiLei",18,"male",1),("HanMeiMei",16,"female",1),
                    ("Jim",17,"male",2),("DaChui",20,"male",2)]).toDF("name","age","gender","class")
dfstudent.show()
dfstudent.groupBy("class").pivot("gender").max("age").show()
```

```
+---------+---+------+-----+
|     name|age|gender|class|
+---------+---+------+-----+
|    LiLei| 18|  male|    1|
|HanMeiMei| 16|female|    1|
|      Jim| 17|  male|    2|
|   DaChui| 20|  male|    2|
+---------+---+------+-----+

+-----+------+----+
|class|female|male|
+-----+------+----+
|    1|    16|  18|
|    2|  null|  20|
+-----+------+----+
```

```python
#窗口函数

df = spark.createDataFrame([("LiLei",78,"class1"),("HanMeiMei",87,"class1"),
                           ("DaChui",65,"class2"),("RuHua",55,"class2")]) \
    .toDF("name","score","class")

df.show()
dforder = df.selectExpr("name","score","class",
         "row_number() over (partition by class order by score desc) as order")

dforder.show()
```

```
+---------+-----+------+
|     name|score| class|
+---------+-----+------+
|    LiLei|   78|class1|
|HanMeiMei|   87|class1|
|   DaChui|   65|class2|
|    RuHua|   55|class2|
+---------+-----+------+

+---------+-----+------+-----+
|     name|score| class|order|
+---------+-----+------+-----+
|   DaChui|   65|class2|    1|
|    RuHua|   55|class2|    2|
|HanMeiMei|   87|class1|    1|
|    LiLei|   78|class1|    2|
+---------+-----+------+-----+
```

```python

```

### 六，DataFrame的SQL交互


将DataFrame注册为临时表视图或者全局表视图后，可以使用sql语句对DataFrame进行交互。

不仅如此，还可以通过SparkSQL对Hive表直接进行增删改查等操作。



**1，注册视图后进行SQL交互** 

```python
#注册为临时表视图, 其生命周期和SparkSession相关联
df = spark.createDataFrame([("LiLei",18,"male"),("HanMeiMei",17,"female"),("Jim",16,"male")],
                              ("name","age","gender"))

df.show()
df.createOrReplaceTempView("student")
dfmale = spark.sql("select * from student where gender='male'")
dfmale.show()
```

```
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|    LiLei| 18|  male|
|HanMeiMei| 17|female|
|      Jim| 16|  male|
+---------+---+------+

+-----+---+------+
| name|age|gender|
+-----+---+------+
|LiLei| 18|  male|
|  Jim| 16|  male|
+-----+---+------+
```

```python
#注册为全局临时表视图,其生命周期和整个Spark应用程序关联

df.createOrReplaceGlobalTempView("student")
query = """
 select t.gender
 , collect_list(t.name) as names 
 from global_temp.student t 
 group by t.gender
""".strip("\n")

spark.sql(query).show()
#可以在新的Session中访问
spark.newSession().sql("select * from global_temp.student").show()

```

```
+------+------------+
|gender|       names|
+------+------------+
|female| [HanMeiMei]|
|  male|[LiLei, Jim]|
+------+------------+

+---------+---+------+
|     name|age|gender|
+---------+---+------+
|    LiLei| 18|  male|
|HanMeiMei| 17|female|
|      Jim| 16|  male|
+---------+---+------+
```


**2，对Hive表进行增删改查操作**

```python

```

```python
#删除hive表

query = "DROP TABLE IF EXISTS students" 
spark.sql(query) 

```

```python
#建立hive分区表
#(注：不可以使用中文字段作为分区字段)

query = """CREATE TABLE IF NOT EXISTS `students`
(`name` STRING COMMENT '姓名',
`age` INT COMMENT '年龄'
)
PARTITIONED BY ( `class` STRING  COMMENT '班级', `gender` STRING  COMMENT '性别')
""".replace("\n"," ")
spark.sql(query) 
```

```python
##动态写入数据到hive分区表
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict") #注意此处有一个设置操作
dfstudents = spark.createDataFrame([("LiLei",18,"class1","male"),
                                    ("HanMeimei",17,"class2","female"),
                                    ("DaChui",19,"class2","male"),
                                    ("Lily",17,"class1","female")]).toDF("name","age","class","gender")
dfstudents.show()

#动态写入分区
dfstudents.write.mode("overwrite").format("hive")\
.partitionBy("class","gender").saveAsTable("students")
```

```python
#写入到静态分区
dfstudents = spark.createDataFrame([("Jim",18,"class3","male"),
                                    ("Tom",19,"class3","male")]).toDF("name","age","class","gender")
dfstudents.createOrReplaceTempView("dfclass3")

#INSERT INTO 尾部追加, INSERT OVERWRITE TABLE 覆盖分区
query = """
INSERT OVERWRITE TABLE `students`
PARTITION(class='class3',gender='male') 
SELECT name,age from dfclass3
""".replace("\n"," ")
spark.sql(query)
```

```python
#写入到混合分区
dfstudents = spark.createDataFrame([("David",18,"class4","male"),
                                    ("Amy",17,"class4","female"),
                                    ("Jerry",19,"class4","male"),
                                    ("Ann",17,"class4","female")]).toDF("name","age","class","gender")
dfstudents.createOrReplaceTempView("dfclass4")

query = """
INSERT OVERWRITE TABLE `students`
PARTITION(class='class4',gender) 
SELECT name,age,gender from dfclass4
""".replace("\n"," ")
spark.sql(query)
```

```python
#读取全部数据

dfdata = spark.sql("select * from students")
dfdata.show()
```

```
+---------+---+------+------+
|     name|age| class|gender|
+---------+---+------+------+
|      Ann| 17|class4|female|
|      Amy| 17|class4|female|
|HanMeimei| 17|class2|female|
|   DaChui| 19|class2|  male|
|    LiLei| 18|class1|  male|
|     Lily| 17|class1|female|
|    Jerry| 19|class4|  male|
|    David| 18|class4|  male|
|      Jim| 18|class3|  male|
|      Tom| 19|class3|  male|
+---------+---+------+------+
```

```python

```

**如果本书对你有所帮助，想鼓励一下作者，记得给本项目加一颗星星star⭐️，并分享给你的朋友们喔😊!** 

如果对本书内容理解上有需要进一步和作者交流的地方，欢迎在公众号"算法美食屋"下留言。作者时间和精力有限，会酌情予以回复。

也可以在公众号后台回复关键字：**spark加群**，加入spark和大数据读者交流群和大家讨论。

![image.png](./data/算法美食屋二维码.jpg)

```python

```
