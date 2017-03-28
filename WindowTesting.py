


from pyspark.sql import SparkSession
spark = SparkSession\
.builder\
.appName("FEMTO_Prep")\
.getOrCreate()

sc = spark.sparkContext

stocksDF = spark.read.option("header","true").option("inferSchema","true").csv("/user/cputnam/stockdata/applestock.csv")
stocks2016 = stocksDF.filter("year(Date)==2016")

# Scala Example
# val tumblingWindowDS = stocks2016.groupBy(Window(stocks2016.col("Date"),"1 week")).agg(avg("Close"))

window = F.window("Date","1 week")
tumblingWindowDS = stocks2016.groupBy(window).agg(F.avg("Close"))

# _______________________________________
# Import the Window function
# See http://spark.apache.org/docs/2.0.0/api/python/pyspark.sql.html#module-pyspark.sql.functions

from pyspark.sql.window import Window

# Important the sql functions you will need to reference here for the function to use in the window
# as well as to define the window

from pyspark.sql import functions as F

#define the window to group by
window = F.window("date","5 seconds")

df = spark.createDataFrame([("2016-03-11 09:00:07", 1)]).toDF("date", "val")
w = df.groupBy(window).agg(F.sum("val").alias("sum"))
w.select(w.window.start.cast("string").alias("start"), w.window.end.cast("string").alias("end"), "sum").collect()



#______________ Print Window Function Port from Scala ____________

.sort("window.start").select("window.start","window.end","Close").show()

