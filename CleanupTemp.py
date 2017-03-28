from pyspark.sql import SparkSession
spark = SparkSession\
.builder\
.appName("FEMTO_Prep")\
.getOrCreate()

sc = spark.sparkContext

# This code loads the temperature data from HDFS and processes it

raw_temp = sc.textFile('/user/cputnam/femto/temp')

#Temperature Schema
# Schema
# [0] Hour       => Int
# [1] Minute     => Int
# [2] Second     => Int
# [3] 0.x second => Int [0-9] 0 tenths - 9 tenths of second
# [4] RTDSensor  => Float

split_temp = raw_temp.map(lambda line: line.split(','))

#Type Conversion
step1_temp = split_temp.map(lambda line: (int(line[0]),int(line[1]),int(line[2]),int(line[3]),float(line[4])))

#-----------------------------------------------------------
# Traditional RDD Approach
#___________________________________________________________
# Convert to relative time in seconds
def tempconvertToSec(x):
    Hsec = x[0]*3600
    Msec = x[1]*60
    Ssec = x[2]
    tensec = x[3]*0.1
    return Hsec+Msec+Ssec+tensec
  
step2_temp = step1_temp.map(lambda x: (tempconvertToSec(x),x[0],x[1],x[2],x[3],x[4]))

# Cache step2_temp as it gets used alot in the coming steps
step2_temp.persist()

# Schema in Step_2 is now
# [0] absolute time in seconds
# [1] Hour
# [2] Minute
# [3] Second
# [4] Tenth of a second
# [5] Sensor Reading

temp_data = step2_temp.map(lambda x: (x[0],x[5])).collect()

tx,ty = zip(*temp_data)

# Temperature over life of bearing
plt.plot(tx,ty)

# Find the Max Minutes and Min Minutes for the data set
hour_max = step2_temp.map(lambda x: x[1]).max()
# The longest hourly duration in this data set
print(hour_max)
# The lowest hourly duration in this data set
hour_min = step2_temp.map(lambda x: x[1]).min()
print(hour_min)

#Minute Max is the final minute in the final hour
minute_max = step2_temp.filter(lambda x: x[1] == hour_max).map(lambda x: x[2]).max()
#minute_min is the first minute in the first hour
minute_min = step2_temp.filter(lambda x: x[1] == hour_min).map(lambda x: x[2]).min()

# Calculate the duration of this test and the bearings useful life.
"This ranges from {} hours and {} minutes to {} hours and {} minutes".format(hour_min, minute_min, hour_max, minute_max)
elaped_time_hours = (((hour_max*60) + minute_max) - ((hour_min*60)+minute_min))/60
elapsed_time_minutes = (((hour_max*60) + minute_max) - ((hour_min*60)+minute_min))%60

"The total useful life of this bearing was {} hours and {} minutes".format(elaped_time_hours, elapsed_time_minutes)

# Now we can aggregate on the minute.  These loops need to be smarter to handle non zero start and end times.
# Also this process brings data back to the driver. Is there a way to do this conversion and leave data in cluster ?

x = 0
y = 0

run = []
maxt = []
for x in range(10, 17):
  for y in range(0,60):
    # print (x, y)
    u = step2_temp.filter(lambda d: d[1] == x)
    uu = u.filter(lambda d: d[2]== y)
    umax = uu.map(lambda z: z[5]).max()
    umin = uu.map(lambda z: z[5]).min()
    # print umax-umin
    maxt.append(umax)
    run.append(umax-umin)

plt.plot(run)
plt.plot(maxt)

#_________________________________________________________________________
# Spark Dataframe / SQL Approach
#_________________________________________________________________________

#Consider using a DataFrame with Spark SQL window functions to calculate the temp_change in the window See 
# See https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html
# Start from split_temp as data is still in strings

from pyspark.sql import Row
from pyspark.sql.types import *
import datetime
from pyspark.sql.window import Window
import pyspark.sql.functions as F


rdd_temp = split_temp.map(lambda line: ("1970-01-01 "+line[0]+":"+line[1]+":"+line[2]+"."+line[3], float(line[4])))

# Use python function to convert to timestamp from string
def tsconvert(x):
  date = datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f")
  return date
  
rdd_t = rdd_temp.map(lambda x: (tsconvert(x[0]),x[1]))
temp_df = spark.createDataFrame(rdd_t).toDF("date", "val")

w = temp_df.groupBy(F.window("date", "10 seconds")).agg(F.max("val").alias("max"), F.min("val").alias("min"))
foo = w.select(w.window.start.cast("string").alias("start"), w.window.end.cast("string").alias("end"), "max", "min")
# Check Sorting ?
bar = foo.sort(foo.start.asc())
bar.show()

# Really need this in the ACC data set
barbar = temp_df.groupBy(F.window("date", "60 seconds")).agg(F.collect_list("val").alias("tlist"))

