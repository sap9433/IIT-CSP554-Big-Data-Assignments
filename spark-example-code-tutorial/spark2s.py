from pyspark.sql.types import *

struct1 = StructType().add("name", StringType(), True).add("age",IntegerType(), True)
peoplet = spark.read.schema(struct1).text('/user/cs595/people.txt')
peoplet.show()
peoplet.printSchema()
