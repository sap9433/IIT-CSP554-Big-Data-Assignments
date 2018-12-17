lines = spark.read.text('/user/cs595/cs595doc2.txt')
lineLengths = lines.map(lambda s: len(s))
totalLength = lineLengths.reduce(lambda a, b: a + b)
