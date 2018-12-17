from pyspark import SparkContext
logFile = '../hw3/cs595words.txt'
sc = SparkContext("local", "Simple App")

logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()

numBs = logData.filter(lambda s: 'b' in s).count()
print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

