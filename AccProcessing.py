from pyspark.sql import SparkSession
spark = SparkSession\
.builder\
.appName("FEMTO_Prep")\
.getOrCreate()

sc = spark.sparkContext

#___________________________________________________
# Load from HDFS
#___________________________________________________

from pyspark.sql import Row
from pyspark.sql.types import *
import datetime
from pyspark.sql.window import Window
import pyspark.sql.functions as F

raw = sc.textFile('/user/cputnam/femto/testset.csv')


#___________________________________________________
# Split Strings and Create Dataframe
#___________________________________________________

splits = raw.map(lambda line: line.split(','))


# Schema
# [0] Hour
# [1] Minute
# [2] Second
# [3] Microsecond
# [4] HorizAccel
# [5] VertAccel

# Note the type conversion on column [3] string => float => int => string
# This is to handle the scientific notation values which are the data set
rdd_acc = splits.map(lambda line: ("1970-01-01 "+line[0]+":"+line[1]+":"+line[2]+"." + str(int(float(line[3]))), float(line[4]), float(line[5])))

# Use python to convert to time (This seems to work)
def tsconvert(x):
  date = datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f")
  return date
  
acc_t = rdd_acc.map(lambda x: (tsconvert(x[0]),x[1],x[2]))
acc_df = spark.createDataFrame(acc_t).toDF("date", "Horiz","Vert")

#___________________________________________________
# Window Processing
# Collect Acc data in 10 second windows
# Acc is recorded for 1 second every ten seconds
#___________________________________________________
w = acc_df.groupBy(F.window("date", "10 seconds")).agg(F.collect_list("Horiz").alias("Horiz"), F.collect_list("Vert").alias("Vert"))
foo = w.select(w.window.start.cast("string").alias("start"), w.window.end.cast("string").alias("end"), "Horiz", "Vert")
bar = foo.sort(foo.start.asc())
bar.show()





