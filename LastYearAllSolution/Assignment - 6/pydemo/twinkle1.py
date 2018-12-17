a = sc.textFile('/user/cs595/twinkle.txt')
# a: org.apache.spark.rdd.RDD[String]
a.count()
# 5
a_twinkle = a.filter(lambda line: line.find("twinkle")!= -1)
# a_twinkle: org.apache.spark.rdd.RDD[String]

a_twinkle.count()
# 2
print a_twinkle.collect()
# twinkle twinkle little star
# twinkle twinkle little star
