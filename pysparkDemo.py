import sys
from pyspark.sql import SparkSession

def loadFile(filePath):
    spark = SparkSession.builder.master("local").appName("PySpark Read CSV file").getOrCreate()
    df = spark.read.csv(filePath)
    #df = spark.createDataFrame([(1, "Hello"), (2, "World")], ["id", "message"])
    df.printSchema()
    df.show()

if __name__ == '__main__':
    if len(sys.argv) > 1:
        filePath = sys.argv[1]
        loadFile(filePath)
    else:
        print('Please enter the file path.')