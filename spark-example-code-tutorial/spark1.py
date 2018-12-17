peoplej = spark.read.json('/user/cs595/people.json')
peoplej.show()
peoplej.printSchema()

