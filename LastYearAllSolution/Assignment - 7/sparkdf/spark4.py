peoplech = spark.read.csv('/user/cs595/peopleh.csv', header=True)
peoplech.show()
peoplech.printSchema()

