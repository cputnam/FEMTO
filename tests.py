# Move data from Workbench to HDFS
!hdfs dfs -mkdir /user/cputnam/femto
!hdfs dfs -mkdir /user/cputnam/femto/sample
!hdfs dfs -put acc_sample.csv /user/cputnam/femto/sample/
!hdfs dfs -ls /user/cputnam/femto/sample/
/user/cputnam/femto/sample/
foo = sc.textFile("/user/cputnam/femto/sample/acc_sample.csv")


one = sc.parallelize([1,'a','b'])
two = one.map(lambda x: int(x))
two = one.map(lambda x: x.find('a'))

testline = "My sentence"

rawcheck = raw.filter(lambda line: line.find('e') <> -1)


rdd.filter(lambda x: x % 2 == 0).collect()


raw_temp = sc.textFile('/user/cputnam/femto/temp')
raw_temp.take(2)

# Convert this with STRP time '1970-01-01 9:39:39.65664.0'

def tsconvert(x):
  date = datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f")
  return date

g = '1970-01-01 9:39:39.65664.0'

p = tsconvert(g)

splitsck = rawcheck.map(lambda line: line.split(','))
fee = splitsck.map(lambda line: ("1970-01-01 "+line[0]+":"+line[1]+":"+line[2]+".", str(int(float(line[3]))), float(line[4]), float(line[5])))

#converting string to int.. no joy
#converstion to float works.
