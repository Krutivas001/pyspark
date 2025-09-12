from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").master("local[*]").getOrCreate()
print("Spark running:")
print(spark.sparkContext.appName)
text_file=spark.sparkContext.textFile("sampleSpecialChars.txt")

specialCharacters=";:#%^*(){}[]<>?/|`~!.,\"\'\\"
special_set = set(specialCharacters)
counts=text_file.flatMap(lambda line: [char for char in line if char in special_set]) \
    .map(lambda char: (char, 1)) \
    .reduceByKey(lambda a, b: a + b)
output=counts.collect()
for (word, count) in output:
    print(f"{word}: {count}")
spark.stop()