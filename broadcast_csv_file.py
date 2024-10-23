from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from  pyspark.sql.functions import count, sum, expr

# Create a Spark session
spark = SparkSession.builder \
    .appName("SalesDataProcessing") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

sale_df = spark.read \
    .format("csv") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load("/FileStore/tables/Sales.csv")
sale_df.show(5)

product_df = spark.read \
    .format("csv") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load("/FileStore/tables/products.csv")
product_df.show(5)

join_broadcast_df = sale_df.join(broadcast(product_df), sale_df.product_id == product_df.product_id, how = "left_anti")

# Perform some basic data processing (example: show the first 5 rows)
# join_broadcast_df.show()

most_likes_product = join_broadcast_df.groupBy("product_id", "city_Name").agg(count("product_id").alias("total_product_sale")).orderBy("total_product_sale", ascending=False)


all_sold_product_df = join_broadcast_df.filter(sale_df.procured_quantity > 0)
all_sold_product_df.show()

total_profit_each_city_df = join_broadcast_df.groupBy("city_name").agg(
    sum(expr("procured_quantity * (unit_selling_price - total_weighted_landing_price)")).alias("total_profit"))

# Show the results
total_profit_each_city_df.show()


negative_profit_df = join_broadcast_df.withColumn(
    "profit", expr("procured_quantity * (unit_selling_price - total_weighted_landing_price)")
).filter("profit < 0")

negative_profit_df.show()

# most_likes_product.show()