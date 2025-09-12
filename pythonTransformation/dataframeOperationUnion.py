from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("UnionOperation").master("local[*]").getOrCreate()
print("Spark running:")
print(spark.sparkContext.appName)

#Create a Structered Dataframe
data1 = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
data2 = [(40,"David"), (38,"Eva"), (50,"Frank")]
columns1= ["Name", "Age"]
columns2=["Age","Name"]
df1 = spark.createDataFrame(data1, columns1)
df2 = spark.createDataFrame(data2, columns2)
df3=df1.unionByName(df2)#We cant do union because the schema is different thats why we use unionByName
df3.show()

print("Number of Stages First: ",spark.sparkContext.statusTracker().getActiveStageIds())
print("Number of Jobs First: ",spark.sparkContext.statusTracker().getActiveJobsIds())

data3 = [("Alice", 34,"Address1"), ("Bob", 45,"Address2"), ("Cathy", 29,"Address3")]
data4 = [(40,"David"), (38,"Eva"), (50,"Frank")]
columns3= ["Name", "Age","Address"]
columns4=["Age","Name"]
df4 = spark.createDataFrame(data3, columns3)
df5 = spark.createDataFrame(data4, columns4)
df6=df4.unionByName(df5,allowMissingColumns=True) #When Number columns are different we can use allowMissingColumns=True
df6.show()

print("Number of Stages Second: ",spark.sparkContext.statusTracker().getActiveStageIds())
print("Number of Jobs Second: ",spark.sparkContext.statusTracker().getActiveJobsIds())

#Consider we have a scenario of different columns but we want to union based on specific columns
data5 = [("Alice", 34,"Address1"), ("Bob", 45,"Address2"), ("Cathy", 29,"Address3")]
data6 = [(40,"David"), (38,"Eva"), (50,"Frank")]
columns5= ["Name", "Age","Address"]
columns6=["Age","Name"]
df7 = spark.createDataFrame(data5, columns5)
df8 = spark.createDataFrame(data6, columns6)
df9=df7.select("Name","Age").unionByName(df8) #Select the specific columns to match the schema
df9.show()
#Identify the Number of stages and jobs
print("Number of Stages Final: ",spark.sparkContext.statusTracker().getActiveStageIds())
print("Number of Jobs Final: ",spark.sparkContext.statusTracker().getActiveJobsIds())

#Note -> Union Doesnt create any shuffle operation so no stages and jobs are created
spark.stop()