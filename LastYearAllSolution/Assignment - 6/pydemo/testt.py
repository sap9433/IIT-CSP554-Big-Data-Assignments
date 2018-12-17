lines=sc.textFile('/user/cs595/twinkle.txt')
upper=lines.map(lambda line: line.upper())
words= lines.flatMap(lambda line: line.split(" "))

