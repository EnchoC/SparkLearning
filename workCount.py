#coding=utf-8
from tql.spark import SparkInit

sc, spark = SparkInit()()
tf = (sc.textFile('./workCountData.txt')
 .flatMap(lambda line: line.split(" "))
 .map(lambda word: (word, 1))
 .reduceByKey(lambda a, b: a + b)
 .collect())

print(sorted(tf, key=lambda x: - x[1]))
