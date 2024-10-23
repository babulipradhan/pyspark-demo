from pyspark import SparkContext

sc = SparkContext("local", "RDD Save to File")
data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
collected_data = distData.collect()
print(collected_data)
distData.saveAsTextFile("data")