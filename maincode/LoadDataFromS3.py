import os
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']


# This is how to get a Spark Session
# spark is the spark session context
from pyspark.sql import SparkSession
spark = SparkSession\
.builder\
.appName("FEMTO_Prep")\
.getOrCreate()

sc = spark.sparkContext

# Load data from S3 in Spark DF 
# Set Credentials 
sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", AWS_ACCESS_KEY_ID)
sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY)

# Load Data
s3path = "/cputnam-bdr/Bearing_FEMTO-ST/Learning_set/Bearing1_1/acc*"
s3patht = "/cputnam-bdr/Bearing_FEMTO-ST/Learning_set/Bearing1_1/temp*"
data = sc.textFile("s3a:/"+s3path)
tempdata = sc.textFile("s3a:/"+s3patht)
# Reduce Partion Count
combined = data.coalesce(3)
combine_temp = tempdata.coalesce(3)

# Save to HDFS
combined.saveAsTextFile('/user/cputnam/femto/testset.csv')
combine_temp.saveAsTextFile('/user/cputnam/femto/temp')

