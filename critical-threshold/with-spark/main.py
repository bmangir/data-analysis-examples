from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *


def read_json(path, schema=None):
    """
    Read data json files
    :param schema: schema of dataframe
    :param path: path of json file
    :return:
    """
    return spark.read.json(path, schema=schema)


# Create spark Session
spark = SparkSession.builder \
    .master("local") \
    .appName('seller_success') \
    .getOrCreate()

orders_schema = StructType(
    [
        StructField(dataType=DateType(), name="orderDate", nullable=True),
        StructField(dataType=LongType(), name="uuid", nullable=False),
        StructField(dataType=LongType(), name="productId", nullable=False),
        StructField(dataType=LongType(), name="sellerId", nullable=False),
        StructField(dataType=DoubleType(), name="price", nullable=True)
    ]
)

products_scheme = StructType(
    [
        StructField(dataType=StringType(), name="uuid", nullable=False),
        StructField(dataType=LongType(), name="id", nullable=False),
        StructField(dataType=StringType(), name="name", nullable=True),
        StructField(dataType=StringType(), name="color", nullable=True),
        StructField(dataType=StringType(), name="brand", nullable=True),
        StructField(dataType=StringType(), name="category", nullable=True),
        StructField(dataType=StringType(), name="productSize", nullable=True),
        StructField(dataType=IntegerType(), name="stock", nullable=True)
    ]
)

orders_df = read_json("/Users/berkantmangir/Desktop/PROJECTS/data-analysis-examples/critical-threshold/data/orders_nsdo.json", orders_schema)
products_df = read_json("/Users/berkantmangir/Desktop/PROJECTS/data-analysis-examples/critical-threshold/data/products_nsdo.json", products_scheme)

daily_sales_info_df = orders_df \
    .where(col("orderDate").between('2021-06-07 00:00:00', '2021-07-08 00:00:00')) \
    .groupBy("sellerId", "productId") \
    .agg(round(count("productId") / 30, 3).alias("daily_average_sale_quantity"),
         round(sum("price") / 30, 3).alias("daily_average_sale_price"))

#window = Window.partitionBy().orderBy("critical_threshold").withColumn("dense_rank", dense_rank().over(window)) \
result_df = daily_sales_info_df \
    .join(products_df, products_df["id"] == daily_sales_info_df["productId"], "left") \
    .withColumn("stock", coalesce(products_df["stock"], lit(0))) \
    .withColumn("critical_threshold", round(col("stock") / col("daily_average_sale_quantity"), 3)) \
    .drop("uuid", "id", "stock") \
    .orderBy(col("critical_threshold"))

result_df.show()

