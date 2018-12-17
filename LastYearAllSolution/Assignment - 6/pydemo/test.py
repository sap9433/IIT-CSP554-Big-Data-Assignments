lines=sc.textFile('/user/cs595/cs595doc2.txt')
print 'lines.take(10):'
print lines.take(10)
upper=lines.map(lambda line: line.upper())
print 'upper.take(10):'
print upper.take(10);
words= lines.flatMap(lambda line: line.split(" "))
print 'words.take(10):'
print words.take(10)
