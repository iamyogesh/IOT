import findspark
import pyspark
from pyspark import SparkContext

findspark.init('/home/yogesh/spark/spark-2.0.0-bin-hadoop2.7')
sc = SparkContext("local","simple app")
a=[1,4,3,5]
a = sc.parallelize(a)
print a.take(2)

