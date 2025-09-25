from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AdvancedTransformation").master("local[*]").getOrCreate()
print("Spark running:")
print(spark.sparkContext.appName)

#Map / FlatMap / Mappartitions / Filter / Distinct / UnionByName / Repartition
# map() → one-to-one transformation
# flatMap() → one-to-many transformation (returns a flattened list)
# mapPartitions() → transforms data partition-wise (better for expensive operations like DB calls)
# filter() → filters data based on a condition
# distinct() → removes duplicate records
# unionByName() → combines two DataFrames by matching column names
# repartition() → changes the number of partitions in a DataFrame
data1 = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
data2 = [(40,"David"), (38,"Eva"), (50,"Frank")]
columns1= ["Name", "Age"]
columns2=["Age","Name"]
df1 = spark.createDataFrame(data1, columns1)
df2 = spark.createDataFrame(data2, columns2)
mapDf=df1.rdd.map(lambda x: (x[0], x[1]+10)).toDF(["Name","Age"]) #Map Transformation
mapDf.show()
rdd = spark.sparkContext.parallelize(["hello world", "spark rdd"])

result = rdd.flatMap(lambda x: x.split(" "))

print(result.collect())

df3=df1.unionByName(df2,allowMissingColumns=True) #When Number columns are different we can use allowMissingColumns=True
df3.show()
df4=df3.repartition(3)
print("Number of Partitions: ",df4.rdd.getNumPartitions())

#mapPartitions Example
def process_partition(iterator):
    return (x * 2 for x in iterator)

rdd = spark.sparkContext.parallelize([1, 2, 3, 4], numSlices=3)

result = rdd.mapPartitions(process_partition)

print(result.collect())








spark.stop()