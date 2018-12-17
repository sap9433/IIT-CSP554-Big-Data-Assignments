from pyspark.sql.types import *

struct1 = StructType(
	[
		StructField("name", StringType(), True),
		StructField("f1",IntegerType(), True),
		StructField("f2",IntegerType(), True),
		StructField("f3",IntegerType(), True),
		StructField("f4",IntegerType(), True),
		StructField("placeid",IntegerType(), True)
	]
)

frc = spark.read.schema(struct1).csv('/user/cs595/foodratings.txt')
frc.show()
frc.printSchema()
