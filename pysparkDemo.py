import sys
from pyspark.sql import SparkSession


def show(df, records):
    df.show(records)


def loadfile(filePath):
    spark = SparkSession.builder.master("local").appName("PIN code analysis").getOrCreate()
    df = spark.read.csv(filePath, header=True, inferSchema=True)
    # dfcount = df.groupBy("CircleName").count().sort("count")
    # odisha = df.filter(df.CircleName=="Odisha Circle").groupBy("DivisionName").count()
    # berhampur_po = df.filter((df.RegionName == "Berhampur Region") & (df.District == "GANJAM") & (df.OfficeType == "PO"))

    # ganjam_so = df.filter((df.District == "GANJAM") & (df.OfficeType == "SO"))

    # odisha_ho = df.filter((df.StateName == "ODISHA") & (df.OfficeType == "HO"))
    #ganjam_ho = df.filter((df.District == "GANJAM") & (df.OfficeType == "HO"))

    #aska_division = df.filter(df.DivisionName == "Aska Division")
    berhampur_division = df.filter(df.DivisionName == "Berhampur Division")

    berhampur_division_po = df.filter((df.DivisionName == "Berhampur Division") & (df.OfficeType=="PO"))
    #non_delivery = df.filter((df.District == "GANJAM") & (df.Delivery == "Non Delivery"))
    # polasara = df.filter(df.Pincode == "761105")
    # polasara.printSchema()
    show(berhampur_division_po, 100)
    #print(berhampur_division_po.count())


if __name__ == '__main__':
    if len(sys.argv) > 1:
        filePath = sys.argv[1]
        loadfile(filePath)
    else:
        print('Please enter the file path.')
