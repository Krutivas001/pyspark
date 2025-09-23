#Read JSN file which has notification Data to read and store in a dataframe
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
import os
import json
from datetime import datetime
import pytz
spark = SparkSession.builder.appName("LiquidityTest").master("local[*]").getOrCreate()
print("Spark running:")
print(spark.sparkContext.appName)
#Read Json file
input_path = os.path.join(os.path.dirname(__file__), 'liquidityJson.json')
with open(input_path, 'r') as file:
    json_data = json.load(file)
#Define Schema
print(json_data)
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip", StringType(), True)
    ])),
    StructField("orders", ArrayType(
        StructType([
            StructField("orderId", StringType(), True),
            StructField("date", StringType(), True),
            StructField("items", ArrayType(
                StructType([
                    StructField("productId", StringType(), True),
                    StructField("productName", StringType(), True),
                    StructField("quantity", IntegerType(), True),
                    StructField("price", DoubleType(), True)
                ])
            ))
        ])
    ))
])
#Create Dataframe
print("Creating Dataframe")
df = spark.read.json(input_path, schema=schema, multiLine=True)
df.printSchema()
df.show(truncate=False)
#Extracting nested fields
df_extracted = df.select(
    col("id"),
    col("name"),
    col("email"),
    col("address.street").alias("street"),
    col("address.city").alias("city"),
    col("address.state").alias("state"),
    col("address.zip").alias("zip"),
    col("orders")
)
df_extracted.show(truncate=False)
#Explode orders array to get individual orders
from pyspark.sql.functions import explode, expr
df_orders = df_extracted.withColumn("order", explode(col("orders"))).drop("orders")
df_orders = df_orders.select(
    col("id"),
    col("name"),
    col("email"),
    col("street"),
    col("city"),
    col("state"),
    col("zip"),
    col("order.orderId").alias("orderId"),
    col("order.date").alias("orderDate"),
    col("order.items").alias("items")
)
df_orders.show(truncate=False)
#Explode items array toget individual items
df_items = df_orders.withColumn("item", explode(col("items"))).drop("items")
df_items = df_items.select(
    col("id"),
    col("name"),
    col("email"),
    col("street"),
    col("city"),
    col("state"),
    col("zip"),
    col("orderId"),
    col("orderDate"),
    col("item.productId").alias("productId"),
    col("item.productName").alias("productName"),
    col("item.quantity").alias("quantity"),
    col("item.price").alias("price")
)
df_items.show(truncate=False)


#Calculate total price per item
df_final = df_items.withColumn("totalPrice", col("quantity") * col("price"))
df_final.show(truncate=False)
#Add a processing timestamp column
def current_timestamp():
    return datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")

#Write the final dataframe to a CSV file
#Store into Hive with Parquet Format



