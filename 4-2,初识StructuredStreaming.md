# 4-2,初识StructuredStreaming



设想我们要设计一个交易数据展示系统，实时呈现比特币最近1s钟的成交均价。

我们可以通过交易数据接口以非常低的延迟获得全球各个比特币交易市场的每一笔比特币的成交价，成交额，交易时间。

由于比特币交易事件一直在发生，所以交易事件触发的交易数据会像流水一样源源不断地通过交易接口传给我们。

如何对这种流式数据进行实时的计算呢？我们需要使用流计算工具，在数据到达的时候就立即对其进行计算。

市面上主流的开源流计算工具主要有 Storm, Flink 和 Spark。

其中Storm的延迟最低，一般为几毫秒到几十毫秒，但数据吞吐量较低，每秒能够处理的事件在几十万左右，建设成本高。

Flink是目前国内互联网厂商主要使用的流计算工具，延迟一般在几十到几百毫秒，数据吞吐量非常高，每秒能处理的事件可以达到几百上千万，建设成本低。

Spark通过Spark Streaming或Spark Structured Streaming支持流计算。但Spark的流计算是将流数据按照时间分割成一个一个的小批次(mini-batch)进行处理的，其延迟一般在1秒左右。吞吐量和Flink相当。值得注意的是Spark Structured Streaming 现在也支持了Continous Streaming 模式，即在数据到达时就进行计算，不过目前还处于测试阶段，不是特别成熟。

虽然从目前来看，在流计算方面，Flink比Spark更具性能优势，是当之无愧的王者。但由于Spark拥有比Flink更加活跃的社区，其流计算功能也在不断地完善和发展，未来在流计算领域或许足以挑战Flink的王者地位。


```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F 
import time,os,random

#本文主要用小数据测试，设置较小的分区数可以获得更高性能
spark = SparkSession.builder \
        .appName("structured streaming") \
        .config("spark.sql.shuffle.partitions","8") \
        .config("spark.default.parallelism","8") \
        .config("master","local[4]") \
        .enableHiveSupport() \
        .getOrCreate()

sc = spark.sparkContext

```

### 一，Structured Streaming 基本概念


**流计算(Streaming)和批计算(Batch)**:

批计算或批处理是处理离线数据。单个处理数据量大，处理速度比较慢。

流计算是处理在线实时产生的数据。单次处理的数据量小，但处理速度更快。

<br/>


**Spark Streaming 和 Spark Structured Streaming**:

Spark在2.0之前，主要使用的Spark Streaming来支持流计算，其数据结构模型为DStream，其实就是一个个小批次数据构成的RDD队列。

目前，Spark主要推荐的流计算模块是Structured Streaming，其数据结构模型是Unbounded DataFrame，即没有边界的数据表。

相比于 Spark Streaming 建立在 RDD数据结构上面，Structured Streaming 是建立在 SparkSQL基础上，DataFrame的绝大部分API也能够用在流计算上，实现了流计算和批处理的一体化，并且由于SparkSQL的优化，具有更好的性能，容错性也更好。

<br/>


**source 和 sink**:

source即流数据从何而来。在Spark Structured Streaming 中，主要可以从以下方式接入流数据。

1, Kafka Source。当消息生产者发送的消息到达某个topic的消息队列时，将触发计算。这是structured Streaming 最常用的流数据来源。

2, File Source。当路径下有文件被更新时，将触发计算。这种方式通常要求文件到达路径是原子性(瞬间到达，不是慢慢写入)的，以确保读取到数据的完整性。在大部分文件系统中，可以通过move操作实现这个特性。

3, Socket Source。需要制定host地址和port端口号。这种方式一般只用来测试代码。linux环境下可以用nc命令来开启网络通信端口发送消息测试。

sink即流数据被处理后从何而去。在Spark Structured Streaming 中，主要可以用以下方式输出流数据计算结果。

1, Kafka Sink。将处理后的流数据输出到kafka某个或某些topic中。

2, File Sink。 将处理后的流数据写入到文件系统中。

3, ForeachBatch Sink。 对于每一个micro-batch的流数据处理后的结果，用户可以编写函数实现自定义处理逻辑。例如写入到多个文件中，或者写入到文件并打印。

4， Foreach Sink。一般在Continuous触发模式下使用，用户编写函数实现每一行的处理处理。

5，Console Sink。打印到Driver端控制台，如果日志量大，谨慎使用。一般供调试使用。

6，Memory Sink。输出到内存中，供调试使用。


**append mode, complete mode 和 update mode**:

这些是流数据输出到sink中的方式，叫做 output mode。

append mode 是默认方式，将新流过来的数据的计算结果添加到sink中。

complete mode 一般适用于有aggregation查询的情况。流计算启动开始到目前为止接收到的全部数据的计算结果添加到sink中。

update mode 只有本次结果中和之前结果不一样的记录才会添加到sink中。

<br/>



**operation 和 query**:

在SparkSQL批处理中，算子被分为Transformation算子和Action算子。Spark Structured Streaming 有所不同，所有针对流数据的算子都是懒惰执行的，叫做operation。

DataFrame的Action算子(例如show,count,reduce)都不可以在Spark Structured Streaming中使用，而大部分Transformation算子都可以在Structured Streaming中使用(例如select,where,groupBy,agg)。

但也有些操作不可以(例如sort, distinct,某些类型的join操作，以及连续的agg操作等)。

如果要触发执行，需要通过writeStream启动一个query，指定sink，output mode，以及触发器trigger类型。

从一定意义上，可以将writeStream理解成Structured Streaming 唯一的 Action 算子。

Spark Structured Streaming支持的触发器trigger类型主要有以下一些。

1，unspecified。不指定trigger类型，以micro-batch方式触发，当上一个micro-batch执行完成后，将中间收到的数据作为下一个micro-batch的数据。

2，fixed interval micro-batches。指定时间间隔的micro-batch。如果上一个micro-batch在间隔时间内完成，需要等待指定间隔时间。如果上一个micro-batch在间隔时间后才完成，那么会在上一个micro-batch执行完成后立即执行。

3，one-time micro-batch。只触发一次,以micro-batch方式触发。一种在流计算模式下执行批处理的方法。

4，continuous with fixed checkpoint interval。每个事件触发一次，真正的流计算，这种模式目前还处于实验阶段。

<br/>


**event time， processing time 和 watermarking**:

event time 是流数据的发生时间，一般嵌入到流数据中作为一个字段。 

processing time 是指数据被处理的时间。

Spark Structured Streaming 一般 使用 event time作为 Windows切分的依据，例如每秒钟的成交均价，是取event time中每秒钟的数据进行处理。

考虑到数据存在延迟，如果一个数据到达时，其对应的时间批次已经被计算过了，那么会重新计算这个时间批次的数据并更新之前的计算结果。但是如果这个数据延迟太久，那么可以设置watermarking(水位线)来允许丢弃 processing time和event time相差太久的数据，即延迟过久的数据。**注意这种丢弃是或许会发生的，不是一定会丢弃**。

<br/>


**at-most once，at-least once 和 exactly once**:

这是分布式流计算系统在某些机器发生发生故障时，对结果一致性(无论机器是否发生故障，结果都一样)的保证水平。反应了分布式流计算系统的容错能力。

at-most once，最多一次。每个数据或事件最多被程序中的所有算子处理一次。这本质上是一种尽力而为的方法，只要机器发生故障，就会丢弃一些数据。这是比较低水平的一致性保证。

at-least once，至少一次。每个数据或事件至少被程序中的所有算子处理一次。这意味着当机器发生故障时，数据会从某个位置开始重传。但有些数据可能在发生故障前被所有算子处理了一次，在发生故障后重传时又被所有算子处理了一次，甚至重传时又有机器发生了故障，然后再次重传，然后又被所有算子处理了一次。因此是至少被处理一次。这是一种中间水平的一致性保证。

exactly once，恰好一次。从计算结果看，每个数据或事件都恰好被程序中的所有算子处理一次。这是一种最高水平的一致性保证。

spark structured streaming 在micro-batch触发器类型下，sink是File情况下，可以保证为exactly once的一致性水平。

但是在continuou触发器类型下，只能保证是at-least once的一致性水平。

详情参考如下文章：《谈谈流计算中的『Exactly Once』特性》

https://segmentfault.com/a/1190000019353382


```python

```

### 二，word count 基本范例


下面范例中，我们将用Python代码在一个目录下不断生成一些简单句子组成的文件。

然后用pyspark读取文件流，并进行词频统计，并将结果打印。



下面是生成文件流的代码。并通过subprocess.Popen调用它异步执行。

```python
%%writefile make_streamming_data.py
import random 
import os 
import time 
import shutil
sentences = ["eat tensorflow2 in 30 days","eat pytorch in 20 days","eat pyspark in 10 days"]
data_path = "./data/streamming_data"

if os.path.exists(data_path):
    shutil.rmtree(data_path)
    
os.makedirs(data_path)

for i in range(20):
    line = random.choice(sentences)
    tmp_file = str(i)+".txt"
    with open(tmp_file,"w") as f:
        f.write(line)
        f.flush()
    shutil.move(tmp_file,os.path.join(data_path,tmp_file))
    time.sleep(1)
    
```

```python
# 在后台异步生成文件流
import subprocess
cmd = ["python", "make_streamming_data.py"]
process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

#process.wait() #等待结束

```

```python
#通过 readStream 创建streaming dataframe
schema = T.StructType().add("value", "string")
data_path = "./data/streamming_data"

dflines = spark \
    .readStream \
    .option("sep", ".") \
    .schema(schema) \
    .csv(data_path)

dflines.printSchema() 
print(dflines.isStreaming) 
```

```
root
 |-- value: string (nullable = true)

True
```

```python
#实施operator转换
dfwords = dflines.select(F.explode(F.split(dflines.value, " ")).alias("word"))
dfwordCounts = dfwords.groupBy("word").count()

```

```python
#执行query, 注意是异步方式执行, 相当于是开启了后台进程

def foreach_batch_function(df, epoch_id):
    print("Batch: ",epoch_id)
    df.show()

query = dfwordCounts \
    .writeStream \
    .outputMode("complete")\
    .foreachBatch(foreach_batch_function) \
    .start()

#query.awaitTermination() #阻塞当前进程直到query发生异常或者被stop

print(query.isActive)

#60s后主动停止query
time.sleep(30)
query.stop()

print(query.isActive)


```

```
True
Batch:  0
+-----------+-----+
|       word|count|
+-----------+-----+
|        eat|   10|
|       days|   10|
|         20|    4|
|tensorflow2|    3|
|         30|    3|
|         10|    3|
|    pyspark|    3|
|         in|   10|
|    pytorch|    4|
+-----------+-----+

Batch:  1
+-----------+-----+
|       word|count|
+-----------+-----+
|        eat|   13|
|       days|   13|
|         20|    4|
|tensorflow2|    5|
|         30|    5|
|         10|    4|
|    pyspark|    4|
|         in|   13|
|    pytorch|    4|
+-----------+-----+

Batch:  2
+-----------+-----+
|       word|count|
+-----------+-----+
|        eat|   15|
|       days|   15|
|         20|    5|
|tensorflow2|    5|
|         30|    5|
|         10|    5|
|    pyspark|    5|
|         in|   15|
|    pytorch|    5|
+-----------+-----+

Batch:  3
+-----------+-----+
|       word|count|
+-----------+-----+
|        eat|   18|
|       days|   18|
|         20|    6|
|tensorflow2|    5|
|         30|    5|
|         10|    7|
|    pyspark|    7|
|         in|   18|
|    pytorch|    6|
+-----------+-----+

Batch:  4
+-----------+-----+
|       word|count|
+-----------+-----+
|        eat|   20|
|       days|   20|
|         20|    7|
|tensorflow2|    5|
|         30|    5|
|         10|    8|
|    pyspark|    8|
|         in|   20|
|    pytorch|    7|
+-----------+-----+

False
```

```python

```

```python

```

### 三，创建Streaming DataFrame



可以从Kafka Source，File Source 以及 Socket Source 中创建 Streaming DataFrame。



**1，从Kafka Source 创建**


需要安装kafka，并加载其jar包到依赖中。

详细参考：http://spark.apache.org/docs/2.2.0/structured-streaming-kafka-integration.html

以下代码仅供示范，运行需要配置相关kafka环境。

```python
df = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("subscribe", "topic1") \
  .load()

```

```python

```

**2，从File Source 创建**


支持读取parquet文件，csv文件，json文件，txt文件目录。需要指定schema。



```python
schema = T.StructType().add("name","string").add("age","integer").add("score","double")
dfstudents = spark.readStream.schema(schema).json("./data/students_json")
dfstudents.printSchema() 
```

```python
query = dfstudents.writeStream \
    .outputMode("append")\
    .format("parquet") \
    .option("checkpointLocation", "./data/checkpoint/") \
    .option("path", "./data/students_parquet/") \
    .start()

#query.awaitTermination()
```

```python

```

```python

```

**3,从Socket Source创建**

在bash中输入nc -lk 9999 开启socket网络通信端口，然后在其中输入一些句子，如：

```
hello world
hello China
hello Beijing
```

```python
dflines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()
```

```python

```

```python

```

### 三，使用operator转换


可以在Streaming DataFrame上使用Static DataFrame大部分常规Transformation算子。

还可以针对event time进行滑动窗口(window)操作，可以通过设置水位线(watermarking)来丢弃延迟过久的数据。

不仅如此，可以对Streaming DataFrame和 Static DataFrame 进行表连接 join操作。

甚至两个Streaming DataFrame之前也是可以join的。 



**1，Basic Operators** 


一些常用的Transformation算子都可以在Unbounded DataFrame上使用，例如select,selectExpr, where, groupBy, agg等等。

也可以像批处理中的静态的DataFrame那样，注册临时视图，然后在视图上使用SQL语法。


```python
schema = T.StructType().add("name","string").add("age","integer").add("score","double")
dfstudents = spark.readStream.schema(schema).json("./data/students_json")
dfstudents.printSchema() 

dfstudents.createOrReplaceTempView("students")

dfstudents_old = spark.sql("select * from students where age >25")
print(dfstudents_old.isStreaming)
```

```python

```

```python

```

**2, Window Operations on Event Time**


基于事件时间滑动窗上的聚合操作和其它列的goupBy操作非常相似，落在同一个时间窗的记录就好像具有相同的key，它们将进行聚合。

下面我们通过一个虚拟的比特币交易价格的例子来展示基于事件时间滑动窗上的聚合操作。



```python
%%writefile make_trading_data.py

import random 
import os 
import time 
import datetime 
import json 
import shutil

data_path = "./data/trading_data"

if os.path.exists(data_path):
    shutil.rmtree(data_path)
    
os.makedirs(data_path)

for i in range(20):
    now =  datetime.datetime.now()
    now_str =  now.strftime("%Y-%m-%d %H:%M:%S.%f")
    
    #构造延迟数据, 延迟20min左右
    right_now = now - datetime.timedelta(minutes = 20)
    right_now_str = right_now.strftime("%Y-%m-%d %H:%M:%S.%f")
    
    if i%2==0:
        dic = {"dt": now_str, "amount": 100, "price": 10000.0+random.choice(range(5))}
    else:
        dic = {"dt": right_now_str, "amount": 100 ,"price": 100.0-random.choice(range(5))}
        
    tmp_file = str(i)+".json"
    with open(tmp_file,"w") as f:
        json.dump(dic,f)
    shutil.move(tmp_file,os.path.join(data_path,tmp_file))
    time.sleep(10)
```

```python
# 在后台异步生成文件流
import subprocess
cmd = ["python", "make_trading_data.py"]
process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

#process.wait() #等待结束
```

```python
data_path = "./data/trading_data"
schema = T.StructType().add("dt","string").add("amount","integer").add("price","double")
dfprice_raw = spark.readStream.schema(schema).json("./data/trading_data")
dfprice_raw.printSchema() 
```

```python
dfprice = dfprice_raw.selectExpr("cast(dt as timestamp) as dt","amount","price", "amount*price as volume")
dfprice.printSchema() 
```

```python
# 控制台方式输出，可能需要在jupyter 的log界面查看输出日志

query = dfprice.writeStream \
    .outputMode("append")\
    .format("console") \
    .start()

time.sleep(20)
query.stop()

#query.awaitTermination()

```

```
-------------------------------------------
Batch: 0
-------------------------------------------
+--------------------+------+-------+---------+
|                  dt|amount|  price|   volume|
+--------------------+------+-------+---------+
|2020-12-21 08:11:...|   100|10004.0|1000400.0|
+--------------------+------+-------+---------+

-------------------------------------------
Batch: 1
-------------------------------------------
+--------------------+------+-----+------+
|                  dt|amount|price|volume|
+--------------------+------+-----+------+
|2020-12-21 07:51:...|   100| 99.0|9900.0|
+--------------------+------+-----+------+

-------------------------------------------
Batch: 2
-------------------------------------------
+--------------------+------+-------+---------+
|                  dt|amount|  price|   volume|
+--------------------+------+-------+---------+
|2020-12-21 08:12:...|   100|10003.0|1000300.0|
+--------------------+------+-------+---------+

```

```python
#下面我们将dfprice按照时间分窗，窗口范围为10min，滑动周期为5min，并统计滑动窗口内的平均交易价格

dfprice_avg = dfprice.groupBy(F.window(dfprice.dt, "10 minutes", "5 minutes")) \
   .agg(F.sum("amount").alias("amount"), F.sum("volume").alias("volume")) \
   .selectExpr("window","window.start","window.end","volume/amount as avg_price")

dfprice_avg.printSchema() 
```

```python
query = dfprice_avg.writeStream \
    .outputMode("complete")\
    .format("console") \
    .start()

time.sleep(60)
query.stop()

#query.awaitTermination()

```

```
-------------------------------------------
Batch: 0
-------------------------------------------
+--------------------+-------------------+-------------------+---------+
|              window|              start|                end|avg_price|
+--------------------+-------------------+-------------------+---------+
|[2020-12-21 08:25...|2020-12-21 08:25:00|2020-12-21 08:35:00|  10004.0|
|[2020-12-21 08:00...|2020-12-21 08:00:00|2020-12-21 08:10:00|     97.0|
|[2020-12-21 08:05...|2020-12-21 08:05:00|2020-12-21 08:15:00|     97.0|
|[2020-12-21 08:20...|2020-12-21 08:20:00|2020-12-21 08:30:00|  10004.0|
+--------------------+-------------------+-------------------+---------+

-------------------------------------------
Batch: 1
-------------------------------------------
+--------------------+-------------------+-------------------+---------+
|              window|              start|                end|avg_price|
+--------------------+-------------------+-------------------+---------+
|[2020-12-21 08:25...|2020-12-21 08:25:00|2020-12-21 08:35:00|  10002.0|
|[2020-12-21 08:00...|2020-12-21 08:00:00|2020-12-21 08:10:00|     97.0|
|[2020-12-21 08:05...|2020-12-21 08:05:00|2020-12-21 08:15:00|     97.0|
|[2020-12-21 08:20...|2020-12-21 08:20:00|2020-12-21 08:30:00|  10002.0|
+--------------------+-------------------+-------------------+---------+

-------------------------------------------
Batch: 2
-------------------------------------------
+--------------------+-------------------+-------------------+---------+
|              window|              start|                end|avg_price|
+--------------------+-------------------+-------------------+---------+
|[2020-12-21 08:25...|2020-12-21 08:25:00|2020-12-21 08:35:00|  10002.0|
|[2020-12-21 08:00...|2020-12-21 08:00:00|2020-12-21 08:10:00|     98.5|
|[2020-12-21 08:05...|2020-12-21 08:05:00|2020-12-21 08:15:00|     98.5|
|[2020-12-21 08:20...|2020-12-21 08:20:00|2020-12-21 08:30:00|  10002.0|
+--------------------+-------------------+-------------------+---------+
```

```python
#进一步地，我们设置watermarking(水位线)为20分钟, 则超出水位线的数据将允许被丢弃(但不一定被丢弃)

dfprice_avg = dfprice.withWatermark("dt", "20 minutes") \
   .groupBy(F.window(dfprice.dt, "10 minutes", "5 minutes")) \
   .agg(F.sum("amount").alias("amount"), F.sum("volume").alias("volume")) \
   .selectExpr("window","window.start","window.end","volume/amount as avg_price")

dfprice_avg.printSchema() 


```

```python
#设置水位线后， outputMode必须是append或者update
query = dfprice_avg.writeStream \
    .outputMode("update")\
    .format("console") \
    .start()

time.sleep(60)
query.stop()

#query.awaitTermination()
```

```
-------------------------------------------
Batch: 0
-------------------------------------------
+--------------------+-------------------+-------------------+---------+
|              window|              start|                end|avg_price|
+--------------------+-------------------+-------------------+---------+
|[2020-12-21 08:00...|2020-12-21 08:00:00|2020-12-21 08:10:00|     96.0|
|[2020-12-21 08:15...|2020-12-21 08:15:00|2020-12-21 08:25:00|  10001.0|
|[2020-12-21 08:20...|2020-12-21 08:20:00|2020-12-21 08:30:00|  10001.0|
|[2020-12-21 07:55...|2020-12-21 07:55:00|2020-12-21 08:05:00|     96.0|
+--------------------+-------------------+-------------------+---------+

-------------------------------------------
Batch: 1
-------------------------------------------
+------+-----+---+---------+
|window|start|end|avg_price|
+------+-----+---+---------+
+------+-----+---+---------+

-------------------------------------------
Batch: 2
-------------------------------------------
+--------------------+-------------------+-------------------+---------+
|              window|              start|                end|avg_price|
+--------------------+-------------------+-------------------+---------+
|[2020-12-21 08:00...|2020-12-21 08:00:00|2020-12-21 08:10:00|     97.5|
|[2020-12-21 07:55...|2020-12-21 07:55:00|2020-12-21 08:05:00|     97.5|
+--------------------+-------------------+-------------------+---------+

-------------------------------------------
Batch: 3
-------------------------------------------
+--------------------+-------------------+-------------------+------------------+
|              window|              start|                end|         avg_price|
+--------------------+-------------------+-------------------+------------------+
|[2020-12-21 08:15...|2020-12-21 08:15:00|2020-12-21 08:25:00|10000.666666666666|
|[2020-12-21 08:20...|2020-12-21 08:20:00|2020-12-21 08:30:00|10000.666666666666|
+--------------------+-------------------+-------------------+------------------+

-------------------------------------------
Batch: 4
-------------------------------------------
+------+-----+---+---------+
|window|start|end|avg_price|
+------+-----+---+---------+
+------+-----+---+---------+

-------------------------------------------
Batch: 5
-------------------------------------------
+--------------------+-------------------+-------------------+---------+
|              window|              start|                end|avg_price|
+--------------------+-------------------+-------------------+---------+
|[2020-12-21 08:00...|2020-12-21 08:00:00|2020-12-21 08:10:00|     98.0|
|[2020-12-21 07:55...|2020-12-21 07:55:00|2020-12-21 08:05:00|     98.0|
+--------------------+-------------------+-------------------+---------+
```

```python

```

**3, Join Operations**


Streaming DataFrame 可以和 Static DataFrame 进行 Inner 或者 Left Outer 连接操作。join后的结果依然是一个 Streaming DataFrame。

此外 Streaming  DataFrame 也可以和  Streaming  DataFrame 进行 Inner join. 

这种join机制是通过追溯被join的 Streaming DataFrame 已经接收到的流数据和主动 join的 Streaming DataFrame的当前批次进行key的配对，为了避免追溯过去太久的数据造成性能瓶颈，可以通过设置 watermark 来清空过去太久的历史数据的State，数据被清空State后将允许不被配对查询。



```python
schema = T.StructType().add("name","string").add("age","integer").add("score","double")
dfstudents = spark.readStream.schema(schema).json("./data/students_json")
dfstudents.printSchema() 
```

下面是Streaming DataFrame 和 Static DataFrame 进行 join的示范。

```python
dfclasses = spark.createDataFrame([("LiLei","class1"),("Hanmeimei","class2"),("Lily","class3")]).toDF("name","class")
dfclasses.printSchema() 
```

```python
# 示范 Streaming DataFrame  inner join Static DataFrame
dfjoin_inner = dfstudents.join(dfclasses, "name", "inner")
dfjoin_inner.printSchema()
print(dfjoin_inner.isStreaming)
```

```
root
 |-- name: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- score: double (nullable = true)
 |-- class: string (nullable = true)

True
```

```python
query = dfjoin_inner.writeStream \
    .outputMode("append")\
    .format("console") \
    .start()

time.sleep(10)
query.stop()

#query.awaitTermination()
```

```
-------------------------------------------
Batch: 0
-------------------------------------------
+---------+---+-----+------+
|     name|age|score| class|
+---------+---+-----+------+
|    LiLei| 12| 75.5|class1|
|Hanmeimei| 16| 90.0|class2|
|     Lily| 15| 68.0|class3|
+---------+---+-----+------+
```

```python
# 示范 Streaming DataFrame  left join Static DataFrame
dfjoin_left = dfstudents.join(dfclasses, "name", "left")
dfjoin_left.printSchema()
print(dfjoin_left.isStreaming)

```

```python
query = dfjoin_left.writeStream \
    .outputMode("append")\
    .format("console") \
    .start()

time.sleep(10)
query.stop()

#query.awaitTermination()
```

```
-------------------------------------------
Batch: 0
-------------------------------------------
+---------+---+-----+------+
|     name|age|score| class|
+---------+---+-----+------+
|    LiLei| 12| 75.5|class1|
|   Justin| 19| 87.0|  null|
|     Lily| 15| 68.0|class3|
|     Andy| 17| 80.0|  null|
|Hanmeimei| 16| 90.0|class2|
|  Michael| 20| 70.5|  null|
+---------+---+-----+------+
```


下面是一个简单的Streaming DataFrame inner join Streaming DataFrame示范。

```python

dfhometown = dfstudents.selectExpr("name","if(rand()>0.5,'China','USA') as hometown")
dfhometown.printSchema()
print(dfhometown.isStreaming)

```

```
root
 |-- name: string (nullable = true)
 |-- hometown: string (nullable = false)

True
```

```python
dfjoin_streaming = dfstudents.join(dfhometown,"name","inner")
dfjoin_streaming.printSchema()
print(dfjoin_streaming.isStreaming)
```

```python
query = dfjoin_streaming.writeStream \
    .outputMode("append")\
    .format("console") \
    .start()

time.sleep(10)
query.stop()

#query.awaitTermination()
```

```
-------------------------------------------
Batch: 0
-------------------------------------------
+---------+---+-----+--------+
|     name|age|score|hometown|
+---------+---+-----+--------+
|    LiLei| 12| 75.5|     USA|
|   Justin| 19| 87.0|   China|
|     Lily| 15| 68.0|   China|
|Hanmeimei| 16| 90.0|   China|
|     Andy| 17| 80.0|     USA|
|  Michael| 20| 70.5|   China|
+---------+---+-----+--------+
```

```python

```

```python

```

### 四，输出 Structured Streaming 的结果


Streaming DataFrame 支持以下类型的结果输出：

* Kafka Sink。将处理后的流数据输出到kafka某个或某些topic中。

* File Sink。 将处理后的流数据写入到文件系统中。

* ForeachBatch Sink。 对于每一个micro-batch的流数据处理后的结果，用户可以编写函数实现自定义处理逻辑。例如写入到多个文件中，或者写入到文件并打印。

* Foreach Sink。一般在Continuous触发模式下使用，用户编写函数实现每一行的处理。

* Console Sink。打印到Driver端控制台，如果日志量大，谨慎使用。一般供调试使用。

* Memory Sink。输出到内存中，供调试使用。


```python

```

**1，输出到Kafka Sink**

<!-- #region -->

示范代码如下，注意，df 应当具备以下列：topic, key 和 value.

```python
query = df.selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)") \
  .writeStream()\
  .format("kafka")\
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")\
  .start()
```
<!-- #endregion -->

```python

```

**2，输出到File Sink**


```python
schema = T.StructType().add("name","string").add("age","integer").add("score","double")
dfstudents = spark.readStream.schema(schema).json("./data/students_json")

query = dfstudents \
    .writeStream\
    .format("csv") \
    .option("checkpointLocation", "./data/checkpoint") \
    .option("path", "./data/students_csv") \
    .start()

time.sleep(5)
query.stop()

```

```python

```

**3, 输出到ForeachBatch Sink**


对于每一个Batch,可以当做一个Static DataFrame 进行处理。

```python
schema = T.StructType().add("name","string").add("age","integer").add("score","double")
dfstudents = spark.readStream.schema(schema).json("./data/students_json")

def foreach_batch_function(df, epoch_id):
    print("epoch_id = ",epoch_id)
    df.show()
    print("rows = ",df.count())
    
query = dfstudents.writeStream.foreachBatch(foreach_batch_function).start()  

time.sleep(3)
query.stop()
```

```
epoch_id =  0
+---------+---+-----+
|     name|age|score|
+---------+---+-----+
|    LiLei| 12| 75.5|
|Hanmeimei| 16| 90.0|
|     Lily| 15| 68.0|
|  Michael| 20| 70.5|
|     Andy| 17| 80.0|
|   Justin| 19| 87.0|
+---------+---+-----+

rows =  6
```

```python

```

**4, 输出到Console Sink**


将结果输出到终端，对于jupyter 环境调试，可能需要在jupyter 的 log 日志中去查看。

```python
schema = T.StructType().add("name","string").add("age","integer").add("score","double")
dfstudents = spark.readStream.schema(schema).json("./data/students_json")

dfstudents.writeStream \
  .format("console") \
  .trigger(processingTime='2 seconds') \
  .start()
```

```
-------------------------------------------
Batch: 0
-------------------------------------------
+---------+---+-----+
|     name|age|score|
+---------+---+-----+
|    LiLei| 12| 75.5|
|Hanmeimei| 16| 90.0|
|     Lily| 15| 68.0|
|  Michael| 20| 70.5|
|     Andy| 17| 80.0|
|   Justin| 19| 87.0|
+---------+---+-----+
```

```python

```

```python

```

```python

```

**5, 输出到Memory Sink**

```python
schema = T.StructType().add("name","string").add("age","integer").add("score","double")
dfstudents = spark.readStream.schema(schema).json("./data/students_json")


#设置的queryName 将成为需要查询的表的名称
query = dfstudents \
    .writeStream \
    .queryName("dfstudents") \
    .outputMode("append") \
    .format("memory") \
    .start()

time.sleep(3)
query.stop()

dfstudents_static = spark.sql("select * from dfstudents")
dfstudents_static.show() 


```

```
+---------+---+-----+
|     name|age|score|
+---------+---+-----+
|    LiLei| 12| 75.5|
|Hanmeimei| 16| 90.0|
|     Lily| 15| 68.0|
|  Michael| 20| 70.5|
|     Andy| 17| 80.0|
|   Justin| 19| 87.0|
+---------+---+-----+
```

```python

```

**如果本书对你有所帮助，想鼓励一下作者，记得给本项目加一颗星星star⭐️，并分享给你的朋友们喔😊!** 

如果对本书内容理解上有需要进一步和作者交流的地方，欢迎在公众号"算法美食屋"下留言。作者时间和精力有限，会酌情予以回复。

也可以在公众号后台回复关键字：**spark加群**，加入spark和大数据读者交流群和大家讨论。

![image.png](./data/算法美食屋二维码.jpg)
