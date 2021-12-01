# 14848-homework5
code and screenshot for homework5 of 14848

```
from pyspark.sql import SparkSession
import pyspark
import sys
from pyspark import SparkContext, SparkConf
spark = SparkSession.builder.appName("DataFrame").getOrCreate()
#sc = pyspark.SparkContext(appName="Databricks Shell")
words = sc.textFile("/FileStore/shared_uploads/xupengs@andrew.cmu.edu/*").flatMap(lambda line: line.split(" "))
require_list = ['they', 'she', 'he', 'it', 'the', 'as', 'is', 'and']
wordCounts = words.map(lambda word: (word, 1) if word in require_list else (word, 0)).reduceByKey(lambda a,b:a +b).sortBy(lambda x: -x[1])
print(wordCounts.take(8))
```

