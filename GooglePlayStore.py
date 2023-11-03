from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.types import IntegerType

# create Spark Session
spark_conf = SparkConf()
spark_conf.set("spark.app.name", "Google Play Store")
spark_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

# create dataframe and read file
first_df = spark.read.format("csv") \
    .option("inferschema", True) \
    .option("header", True) \
    .option("path",
            "D:\\PySpark Projects\\GooglePlayStoreAppAnalysis\\DataSet\\googleplaystore.csv").load()

# first_df.show(truncate=False)

# Data Cleaning
second_df = first_df.drop("Size", "Content Rating", "Last Updated", "Android Ver", "Current Ver")
#second_df.printSchema()

third_df = second_df.withColumn("Reviews", (col("Reviews").cast(IntegerType()))) \
    .withColumn("Installs", regexp_replace(col("Installs"), "[^0-9]", "")) \
    .withColumn("Installs", col("Installs").cast(IntegerType())) \
    .withColumn("Price", regexp_replace(col("Price"), "[$]", "")) \
    .withColumn("Price", col("Price").cast(IntegerType())) \
    .withColumn("Rating", col("Rating").cast(IntegerType()))

third_df.printSchema()
# third_df.show(100,truncate=False)
third_df.createOrReplaceTempView("GoogleApps")

# 1.Find top 10 reviews given to the Apps
spark.sql(
    "Select App,SUM(Reviews) as TotalReviews From GoogleApps Group By App Order by TotalReviews Desc Limit 10").show(
    truncate=False)

# 2.Find top 10 Install Apps
spark.sql(
    "Select App,SUM(Installs) as TotalInstalls From GoogleApps Group By App Order by TotalInstalls Desc Limit 10").show(
    truncate=False)

# 3. Category wise Distribution of installed apps
spark.sql(
    "Select Category,SUM(Installs) as TotalInstalls From GoogleApps Group By Category Order by TotalInstalls Desc").show(
    truncate=False)

# 4. Top Paid Apps
spark.sql(
    "Select App,SUM(Price) as TotalPrice From GoogleApps Where Type='Paid' Group By App Order by TotalPrice Desc Limit 10").show(
    truncate=False)

# 5. Top Rated Paid Apps
spark.sql(
    "Select App,AVG(Rating) as Average_rating From GoogleApps Where Type='Paid' Group By App Order by Average_rating Desc ").show(
    truncate=False)


spark.stop()
