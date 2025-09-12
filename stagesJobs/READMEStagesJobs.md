# pyspark
Stages only creates for Wide Transformation jobs not for Narrow Transformation
See the reference below
df1.unionByName(df2)#Creates one statge
df1.distinct()#also creates one stage
df1.groupBy("column1").count()#also creates one stage

df1.show()# Creates Job, It only creates on action