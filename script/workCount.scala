val textData = sc.textFile('./workCountData.txt')
val stringRdd = textData.flatMap(t => t.split(' '))
val countsRdd = stringRdd.map(word => (word, 1)).reduceByKey(_ + _)
countsRdd.saveAsTextFile('./output')
