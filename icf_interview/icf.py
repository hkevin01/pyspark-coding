# Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField

spark = SparkSession.builder.appName("ICFApp").getOrCreate()
#df = spark.read.json("/home/kevin/Downloads/yelp_academic_dataset_business.json");
#spark.read.option("multiLine","true")
multiline_df = spark.read.option("multiLine", "true").json("/home/kevin/Downloads/yelp_academic_dataset_business.json")
#df.select("business_id").show
#multiline_df.show()
#multiline_df.select("business_id","name","full_address","hours").show()
#multiline_df.select("business_id","").show()
column_name_list = multiline_df.columns #multiline_df.columns
print(column_name_list)

