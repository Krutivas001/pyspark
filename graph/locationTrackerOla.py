from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("locationTracker").master("local[*]").getOrCreate()
print("Spark running:")
print(spark.sparkContext.appName)
#Plot graph for location tracker
df = spark.read.option("header", True).csv("flights.csv", inferSchema=True).limit(1000)
df.show(5)
pdf = df.toPandas()
top_dest = pdf.groupby("DESTINATION_AIRPORT")["AIRLINE"].sort_values(ascending=False).head(10)

plt.figure(figsize=(10,5))
top_dest.plot(kind="bar")
plt.title("Top 10 Destination Countries")
plt.ylabel("Number of Flights")
plt.show()