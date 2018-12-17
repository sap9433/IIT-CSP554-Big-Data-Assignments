from pyspark.sql import Row
from pyspark.sql.types import *

struct1 = StructType().add("schema_placeid",IntegerType(), True).add("schema_placename",StringType(), True)

lines_rdd = sc.textFile('/user/cs595/foodplaces.txt')
parts_rdd = lines_rdd.map(lambda line : line.split(','))

# convert each row to a tuple
# note, we still need to convert the first field to an integer
places_rdd = parts_rdd.map(lambda part: (int(part[0]), part[1]))

places_df = spark.createDataFrame(places_rdd, struct1)
places_df.show()
places_df.printSchema()
