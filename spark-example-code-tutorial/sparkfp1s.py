from pyspark.sql.types import *

struct1 = StructType().add("placeid",IntegerType(), True).add("placename",StringType(), True)
fpc = spark.read.schema(struct1).csv('/user/cs595/foodplaces.txt')
fpc.show()
fpc.printSchema()
