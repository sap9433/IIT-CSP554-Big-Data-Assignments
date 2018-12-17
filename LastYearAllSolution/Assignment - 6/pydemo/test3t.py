lines = sc.textFile('/user/cs595/twinkle.txt')
lineLengths = lines.map(lambda s: len(s))
totalLength = lineLengths.reduce(lambda a, b: a + b)
