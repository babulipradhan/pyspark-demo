from pyspark import SparkContext

sc = SparkContext("local", "RDD Read from File.")
distFile = sc.textFile("data")
collected_data = distFile.collect()
print(collected_data)