from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
import sys
import csv
# import excel
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql.functions import col, split 
import pyspark.sql.functions as f

from pyspark.sql.functions import lower, col

spark = SparkSession\
    .builder\
    .master("spark://192.168.29.230:7077")\
    .appName("jobCourseRecommendationEngine")\
    .getOrCreate()

course_data = spark.read.option('header', 'true').csv(f"/opt/coursea_data-1.csv")
course_data = course_data.select(col("course_title"))

slen = udf(lambda s: len(s), IntegerType())

def unique(colA, colB):
    unique1 = list(set(colA) & set(colB))
    return set(unique1)

unique_udf=udf(unique,ArrayType(StringType()))

def unique1(colA, colB):
    unique1 = list(set(colA) & set(colB))
    str1 = "|" 
    return (str1.join(unique1)) 

unique_udf1=udf(unique1,StringType())

############## Define Difference UDF ##############
def substract(colA, colB):
    sub = list(set(colA) - set(colB))
    return sub

sub_udf=udf(substract,ArrayType(StringType()))

############## Define Difference UDF ##############
def split_rows(colA):
    print (colA)
    sub =colA.split()
    print (' '.join(sub))
    return (' '.join(sub))

split_rows_udf=udf(split_rows,ArrayType(StringType()))

course_data = course_data.withColumn("course_title", f.regexp_replace(course_data["course_title"], "[^A-Za-z]", " "))
course_data = course_data.select(trim(lower(course_data['course_title'])).alias('course_title'))

course_data.show()

job_data = spark.read.option('header', 'true').csv(f"/opt/data_job_posts.csv")
job_data.take(2)

job_data.count()

job_data.registerTempTable("job_data")

# job_data = spark.sql("SELECT Title, JobDescription,  JobRequirment FROM job_data")
job_data = job_data.na.drop(how="any")
job_data.registerTempTable("job_data")
job_data.show()

job_data.count()

job_data = spark.sql("SELECT CONCAT(Title, JobDescription,  JobRequirment) as job_details FROM job_data")
job_data = job_data.withColumn("job_details", f.regexp_replace(job_data["job_details"], "[^A-Za-z]", " "))
job_data = job_data.select(trim(lower(job_data['job_details'])).alias('job_details'))

joined_data = job_data.crossJoin(course_data).repartition(10)
joined_data.show()	

joined_data = joined_data.withColumn("levenshtein_score", f.levenshtein(f.col("job_details"), f.col("course_title")))
joined_data = joined_data.withColumn("category_rank",row_number().over(Window.partitionBy("job_details").orderBy(("levenshtein_score")))).filter(col('category_rank') <= 50)

# write the output as csv to GCS

joined_data.coalesce(1).write.option("header",'true').mode("overwrite").csv(f"/opt/result.csv")

