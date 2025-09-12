from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col


spark = SparkSession.builder.appName("Stages").master("local[*]").getOrCreate()
print("Spark running:")
print(spark.sparkContext.appName)

##Narrow Transformation does not create any stages and jobs
##Wide Transformation creates stages and jobs
#Create a Structered Dataframe
data1 = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
data2 = [(40,"David"), (38,"Eva"), (50,"Frank")]
columns1= ["Name", "Age"]
columns2=["Age","Name"]
df1 = spark.createDataFrame(data1, columns1)
df2 = spark.createDataFrame(data2, columns2)
df3=df1.withColumn("Address",
                   when(col("Age")<40,"Address1").otherwise("Address2"))

dfDist=df3.distinct()
dfDist.repartition(2)
print("Number of Stages First: ",spark.sparkContext.statusTracker().getActiveStageIds())
print("Number of Jobs First: ",spark.sparkContext.statusTracker().getActiveJobsIds())

df3.show()
df4=df1.unionByName(df3,allowMissingColumns=True) #When Number columns are different we can use allowMissingColumns=True
df4.show()
spark.stop()