from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import types as T
import pyspark.sql.functions as F
import time
start_time = time.time()

spark = SparkSession.builder.master("local[*]").appName("testing").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


data = "C:\\Users\\Sushil\\OneDrive\\Desktop\\row\\Dataset\\world_bank.json"
df = spark.read.format("json").option("header","true").option("inferSchema","true").load(data)

df.show()
print ("time elapsed: {:.2f}s".format(time.time() - start_time))
