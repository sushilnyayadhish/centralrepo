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


def flatten(df):
    complex_fields = dict([
        (field.name, field.dataType)
        for field in df.schema.fields
        if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
    ])

    qualify = list(complex_fields.keys())[0] + "_"

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]

        if isinstance(complex_fields[col_name], T.StructType):
            expanded = [F.col(col_name + '.' + k).alias(col_name + '_' + k)
                        for k in [n.name for n in complex_fields[col_name]]
                        ]

            df = df.select("*", *expanded).drop(col_name)

        elif isinstance(complex_fields[col_name], T.ArrayType):
            df = df.withColumn(col_name, F.explode(col_name))

        complex_fields = dict([
            (field.name, field.dataType)
            for field in df.schema.fields
            if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
        ])

    for i in df.columns:
        df = df.withColumnRenamed(i, i.replace(qualify, ""))
        return df

res1 = flatten(df)
res1.show(300)
res1.printSchema()

print ("time elapsed: {:.2f}s".format(time.time() - start_time))