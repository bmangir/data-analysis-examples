from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from spark_utility import read, create_spark_session


def find_daily_sales_information(df):
    """
    find the daily sales information in 30 days which means 1 month for each seller with each product
    :param df: dataframe that orders
    :return: dataframe that calculated daily average sale quantity and daily average sale price
    """
    return df \
        .where(col("orderDate").between('2021-06-07 00:00:00', '2021-07-08 00:00:00')) \
        .groupBy("sellerId", "productId") \
        .agg(round(count("productId") / 30, 3).alias("daily_average_sale_quantity"),
             round(sum("price") / 30, 3).alias("daily_average_sale_price"))


def find_and_rank(daily_sales_information, products):
    """
    find the critical threshold and after that ranked them for each seller by critical threshold in descending order
    :param daily_sales_information: dataframe of daily sales information
    :param products: data frame of products file
    :return: found critical threshold and ranked table
    """
    window = Window.partitionBy("sellerId").orderBy(col("critical_threshold").desc())
    return daily_sales_information \
        .join(products, products["id"] == daily_sales_information["productId"], "left") \
        .withColumn("stock", coalesce(products["stock"], lit(0))) \
        .withColumn("critical_threshold", round(col("stock") / col("daily_average_sale_quantity"), 3)) \
        .withColumn("rank", dense_rank().over(window)) \
        .drop("uuid", "id", "stock")


spark = create_spark_session("critical threshold")

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

# Read files
orders_df = read(spark, "/Users/berkantmangir/Desktop/PROJECTS/data-analysis-examples/critical-threshold/data/orders_nsdo.json", orders_schema)
products_df = read(spark, "/Users/berkantmangir/Desktop/PROJECTS/data-analysis-examples/critical-threshold/data/products_nsdo.json", products_scheme)

daily_sales_info_df = find_daily_sales_information(orders_df)

result_df = find_and_rank(daily_sales_info_df, products_df)

result_df.show()
