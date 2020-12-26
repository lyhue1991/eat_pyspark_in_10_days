# 如何用10天吃掉pyspark？🔥🔥


《10天吃掉那只pyspark》

🚀 github项目地址: https://github.com/lyhue1991/eat_pyspark_in_10_days


### 一，pyspark 🍎  or spark-scala 🔥 ?

<!-- #region -->
pyspark强于分析，spark-scala强于工程。

如果应用场景有非常高的性能需求，应该选择spark-scala. 

如果应用场景有非常多的可视化和机器学习算法需求，推荐使用pyspark，可以更好地和python中的相关库配合使用。

此外spark-scala支持spark graphx图计算模块，而pyspark是不支持的。



<br/>


pyspark学习曲线平缓，spark-scala学习曲线陡峭。

从学习成本来说，spark-scala学习曲线陡峭，不仅因为scala是一门困难的语言，更加因为在前方的道路上会有无尽的环境配置痛苦等待着读者。

而pyspark学习成本相对较低，环境配置相对容易。从学习成本来说，如果说pyspark的学习成本是3，那么spark-scala的学习成本大概是9。


如果读者有较强的学习能力和充分的学习时间，建议选择spark-scala，能够解锁spark的全部技能，并获得最优性能，这也是工业界最普遍使用spark的方式。

如果读者学习时间有限，并对Python情有独钟，建议选择pyspark。pyspark在工业界的使用目前也越来越普遍。

<!-- #endregion -->

```python

```

### 二，本书📚 面向读者🤗


本书假定读者具有基础的的Python编码能力，熟悉Python中numpy, pandas库的基本用法。 

并且假定读者具有一定的SQL使用经验，熟悉select,join,group by等sql语法。

对于Python基础不是非常扎实的读者，可以参考《3小时Python入门》文章。

[《3小时Python入门》](https://github.com/lyhue1991/PythonAiRoad/blob/master/3%E5%B0%8F%E6%97%B6Python%E5%85%A5%E9%97%A8.ipynb)

对于numpy和Pandas不甚了解的读者，可以参考 《3小时入门numpy,pandas,matplotlib》文章。

[《3小时入门numpy,pandas,matplotlib》](https://github.com/lyhue1991/PythonAiRoad/blob/master/3%E5%B0%8F%E6%97%B6%E5%85%A5%E9%97%A8numpy%2Cpandas%2Cmatplotlib.ipynb)



```python

```

### 三，本书写作风格🍉


本书是一本对人类用户极其友善的pyspark入门工具书，Don't let me think是本书的最高追求。

本书主要是在参考spark官方文档，并结合作者学习使用经验基础上整理总结写成的。

不同于Spark官方文档的繁冗断码，本书在篇章结构和范例选取上做了大量的优化，在用户友好度方面更胜一筹。

本书按照内容难易程度、读者检索习惯和spark自身的层次结构设计内容，循序渐进，层次清晰，方便按照功能查找相应范例。

本书在范例设计上尽可能简约化和结构化，增强范例易读性和通用性，大部分代码片段在实践中可即取即用。

如果说通过学习spark官方文档掌握pyspark的难度大概是5，那么通过本书学习掌握pyspark的难度应该大概是2.

仅以下图对比spark官方文档与本书《10天吃掉那只pyspark》的差异。

![](./data/eat_pyspark_in_10_days.png)

```python

```



<!-- #region -->
### 四，本书学习方案 ⏰

**1，学习计划**

本书是作者利用工作之余大概1个月写成的，大部分读者应该在10天可以完全学会。

预计每天花费的学习时间在30分钟到2个小时之间。

当然，本书也非常适合作为pyspark的工具手册在工程落地时作为范例库参考。

**点击学习内容蓝色标题即可进入该章节。**


|日期 | 学习内容                                                       | 内容难度   | 预计学习时间 | 更新状态|
|----:|:--------------------------------------------------------------|-----------:|----------:|-----:|
|&nbsp;|**一、基础篇**   |&nbsp;  |  &nbsp;   |&nbsp;   |
|day1 | [1-1,快速搭建你的Spark开发环境](./1-1,快速搭建你的Spark开发环境.md)    | ⭐️⭐️|   1hour    |✅    |
|day2 | [1-2,1小时看懂Spark的基本原理](./1-2,1小时看懂Spark的基本原理.md)    | ⭐️⭐️⭐️ |   1hour    | ✅   |
|&nbsp; |**二、核心篇** | &nbsp; |  &nbsp; | &nbsp; |
|day3 |  [2-1,2小时入门Spark之RDD编程](./2-1,2小时入门Spark之RDD编程.md)  | ⭐️⭐️⭐️ |   2hour    |   |
|day4 |  [2-2,7道RDD编程练习题](./2-2,7道RDD编程练习题.md)  | ⭐️⭐️⭐️  |   1hour    | |
|day5 |  [2-3,2小时入门SparkSQL编程](./2-3,2小时入门SparkSQL编程.md)  | ⭐️⭐️⭐️  |   2hour    |   |
|day6 |  [2-4,7道SparkSQL编程练习题](./2-4,7道SparkSQL编程练习题.md)  | ⭐️⭐️⭐️   |   1hour    |   |
|&nbsp; |**三、进阶篇** |   &nbsp; |  &nbsp;   |&nbsp; |
|day7 |  [3-1,Spark性能调优方法](./3-1,Spark性能调优方法.md)   | ⭐️⭐️⭐️⭐️⭐️ |   2hour    |  |
|day8 |  [3-2,RDD和SparkSQL综合应用](./3-2,RDD和SparkSQL综合应用.md)   | ⭐️⭐️⭐️⭐️⭐️  |  2hour    | |
|&nbsp;| **四、拓展篇**| &nbsp; |  &nbsp;| &nbsp;|
|day9| [4-1,探索MLlib机器学习](./4-1,探索MLlib机器学习.md)  |⭐️⭐️⭐️⭐️   | 2hour|  |
|day10|  [4-2,初识StructuredStreaming](./4-2,初识StructuredStreaming.md)   | ⭐️⭐️⭐️   |   1hour    ||
<!-- #endregion -->

<!-- #region -->
**2，学习环境**

本书全部源码在jupyter中编写测试通过，建议通过git克隆到本地，并在jupyter中交互式运行学习。

为了直接能够在jupyter中打开markdown文件，建议安装jupytext，将markdown转换成ipynb文件。


为简单起见，本书按照如下2个步骤配置单机版spark3.0.1环境进行练习。


step1: 安装java8

jdk下载地址：https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html

java安装教程：https://www.runoob.com/java/java-environment-setup.html


step2: 安装pyspark,findspark

pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pyspark

pip install findspark

此外，也可以在kesci云端notebook中直接运行pyspark

https://www.kesci.com/home/project

<!-- #endregion -->

```python
import findspark

#指定spark_home,指定python路径
spark_home = "/Users/liangyun/anaconda3/lib/python3.7/site-packages/pyspark"
python_path = "/Users/liangyun/anaconda3/bin/python"
findspark.init(spark_home,python_path)

import pyspark 
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("test").setMaster("local[4]")
sc = SparkContext(conf=conf)

print("spark version:",pyspark.__version__)
rdd = sc.parallelize(["hello","spark"])
print(rdd.reduce(lambda x,y:x+' '+y))

```

```
spark version: 3.0.1
hello spark
```

<!-- #region -->
除了以上方法外，也可以参考1-1节中介绍的其它方法。


[1-1,快速搭建你的Spark开发环境](./1-1,快速搭建你的Spark开发环境.md) 


<!-- #endregion -->

```python

```

### 五，鼓励和联系作者


**如果本书对你有所帮助，想鼓励一下作者，记得给本项目加一颗星星star⭐️，并分享给你的朋友们喔😊!** 

如果对本书内容理解上有需要进一步和作者交流的地方，欢迎在公众号"算法美食屋"下留言。作者时间和精力有限，会酌情予以回复。

也可以在公众号后台回复关键字：**spark加群**，加入spark和大数据读者交流群和大家讨论。

![image.png](./data/Python与算法之美.png)

```python

```
