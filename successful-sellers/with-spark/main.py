from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time


def current_millis_time():
    """
    Get current millis time
    :return:
    """
    return int(time.time() * 1000)


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

order_scheme = StructType(
    [
        StructField(dataType=DateType(), name="orderDate", nullable=True),
        StructField(dataType=LongType(), name="categoryId", nullable=False),
        StructField(dataType=LongType(), name="itemId", nullable=False),
        StructField(dataType=LongType(), name="orderId", nullable=False),
        StructField(dataType=DoubleType(), name="price", nullable=True),
        StructField(dataType=LongType(), name="sellerId", nullable=False)
    ]
)

prod_review_scheme = StructType(
    [
        StructField(dataType=DoubleType(), name="rate", nullable=True),
        StructField(dataType=StringType(), name="review", nullable=True),
        StructField(dataType=LongType(), name="created_date", nullable=False),
        StructField(dataType=LongType(), name="last_modified_date", nullable=False),
        StructField(dataType=StringType(), name="status", nullable=True),
        StructField(dataType=LongType(), name="seller_id", nullable=False)
    ]
)

seller_details_scheme = StructType(
    [
        StructField(dataType=StringType(), name="email", nullable=True),
        StructField(dataType=StringType(), name="name", nullable=True),
        StructField(dataType=LongType(), name="createdDate", nullable=False),
        StructField(dataType=StringType(), name="status", nullable=True),
        StructField(dataType=LongType(), name="id", nullable=False)
    ]
)

seller_scores_scheme = StructType(
    [
        StructField(dataType=LongType(), name="calculated_at", nullable=False),
        StructField(dataType=DoubleType(), name="score", nullable=True),
        StructField(dataType=LongType(), name="seller_id", nullable=False)
    ]
)

visits_scheme = StructType(
    [
        StructField(dataType=LongType(), name="product_id", nullable=False),
        StructField(dataType=StringType(), name="url", nullable=True),
        StructField(dataType=LongType(), name="visited_at", nullable=False),
        StructField(dataType=LongType(), name="customer_id", nullable=False),
        StructField(dataType=LongType(), name="seller_id", nullable=False)
    ]
)

# Read json files
orders_df = read_json("/Users/berkantmangir/Desktop/PROJECTS/data-analysis-examples/successful-sellers/data/orders.json", order_scheme).withColumnRenamed("sellerId", "seller_id")
prod_review_df = read_json("/Users/berkantmangir/Desktop/PROJECTS/data-analysis-examples/successful-sellers/data/productreviews.json", prod_review_scheme)
seller_details_df = read_json("/Users/berkantmangir/Desktop/PROJECTS/data-analysis-examples/successful-sellers/data/sellerdetails.json", seller_details_scheme).withColumn('createdDate', to_date(
    to_timestamp((col("createdDate") / 1000).cast(dataType=TimestampType())))) \
    .withColumnRenamed("id", "seller_id"). \
    withColumnRenamed("createdDate", "created_date")
seller_scores_df = read_json("/Users/berkantmangir/Desktop/PROJECTS/data-analysis-examples/successful-sellers/data/sellerscores.json", seller_scores_scheme)
visits_df = read_json("/Users/berkantmangir/Desktop/PROJECTS/data-analysis-examples/successful-sellers/data/visits.json", visits_scheme)

# Find total item that sold
grouped_orders = orders_df \
    .groupBy("seller_id") \
    .agg(count("itemId").alias("total_sold_prod"))

# Find total visit of all customers
grouped_visits = visits_df \
    .groupBy("seller_id") \
    .agg(count("*").alias("total_visits"))

# Find average production rate when their status is APPROVED
grouped_prod_reviews = prod_review_df \
    .where(col("status") == "APPROVED") \
    .groupBy("seller_id") \
    .agg(round(avg("rate"), 2).alias("avg_rate"))

# Find average score of sellers
grouped_seller_scores = seller_scores_df \
    .groupBy("seller_id") \
    .agg(round(avg("score"), 2).alias("avg_score"))

# noinspection PyTypeChecker
seller_info = seller_details_df \
    .join(
        other=grouped_orders,
        on=seller_details_df["seller_id"] == grouped_orders["seller_id"],
        how="left") \
    .join(other=grouped_visits,
          on=seller_details_df["seller_id"] == grouped_visits["seller_id"],
          how="left") \
    .join(other=grouped_prod_reviews,
          on=seller_details_df["seller_id"] == grouped_prod_reviews["seller_id"],
          how="left") \
    .join(other=grouped_seller_scores,
          on=seller_details_df["seller_id"] == grouped_seller_scores["seller_id"],
          how="left") \
    .withColumn("total_visits", coalesce(grouped_visits["total_visits"], lit(0.00))) \
    .withColumn("avg_score", coalesce(grouped_seller_scores["avg_score"], lit(0.00))) \
    .withColumn("avg_rate", coalesce(grouped_prod_reviews["avg_rate"], lit(0.00))) \
    .drop("email", "name", "status", grouped_orders["seller_id"], grouped_visits["seller_id"],
          grouped_prod_reviews["seller_id"], grouped_seller_scores["seller_id"])

is_successful_condition = (col("total_sold_prod") >= 4) & \
                          (col("total_visits") >= 3) & \
                          (col("avg_rate") >= 2.5) & \
                          (col("avg_score") >= 5) & \
                          (datediff(current_date(), col("created_date")) <= 365)

# noinspection PyTypeChecker
seller_info \
    .withColumn("is_successful", when(is_successful_condition, lit(True)).otherwise(lit(False))) \
    .withColumn("calculated_at", lit(current_millis_time())) \
    .show()
