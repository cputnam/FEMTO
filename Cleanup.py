from pyspark.sql import SparkSession
spark = SparkSession\
.builder\
.appName("FEMTO_Prep")\
.getOrCreate()

sc = spark.sparkContext


# Load from HDFS

raw = sc.textFile('/user/cputnam/femto/testset.csv')
splits = raw.map(lambda line: line.split(','))


# Schema
# [0] Hour
# [1] Minute
# [2] Second
# [3] Microsecond
# [4] HorizAccel
# [5] VertAccel

step1 = splits.map(lambda line: (int(line[0]), int(line[1]),int(line[2]),float(line[3]),float(line[4]),float(line[5])))

def convertToSec(x):
    Hsec = x[0]*3600
    Msec = x[1]*60
    Ssec = x[2]
    Usec = x[3]/1000000.0
    return Hsec+Msec+Ssec+Usec
    
step2 = step1.map(lambda line: (convertToSec(line),line[4],line[5]))

# Converted Schema
# time in seconds
# Horiz Accel
# Vert Accel

# Graph Vert Accel
import matplotlib.pyplot as plt

xs = step2.map(lambda x: (x[0]))
xv = xs.collect()
ys = step2.map(lambda y: y[1])
yd = step2.map(lambda y: y[2])
yv = ys.collect()
yh = ys.collect()

# Horizontal Acceleration over life of bearing
plt.plot(xv,yv)
# Vertical Acceleration over life of bearing
plt.plot(xv,yh)

# Temperature Readings

