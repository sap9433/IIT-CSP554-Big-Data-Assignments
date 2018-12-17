peoplet = spark.read.text('/user/cs595/people.txt')
peoplet.show()
peoplet.printSchema()

