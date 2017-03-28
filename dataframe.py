from pyspark.sql import Row
from pyspark.sql.types import *
readings = combined.map(lambda p: Row(hour=int(p[0]), minute=int(p[1]),\
                                   second=int(p[2]),\
                                   microsecond=int(p[3]),\
                                   HorizAccel=float(p[4]),\
                                   VertAccel=float(p[5])))
schemaReadings = spark.createDataFrame(readings)
schemaReadings.createOrReplaceTempView("table")

foo = schemaReadings.select("hour")
df = spark.sql("SELECT * FROM table")

schemaReadings.describe().show()
foo.describe().show()
foo.show()
bar = foo.take(1000)


