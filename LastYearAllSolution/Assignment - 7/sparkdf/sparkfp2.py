from pyspark.sql import Row

lines_rdd = sc.textFile('/user/cs595/foodplaces.txt')
parts_rdd = lines_rdd.map(lambda line : line.split(','))
places_rdd = parts_rdd.map(lambda part: Row(id=int(part[0]), placename=part[1]))

places_df = spark.createDataFrame(places_rdd)
places_df.show()
places_df.printSchema()
