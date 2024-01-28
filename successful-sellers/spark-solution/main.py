from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
from spark_utility import read, create_spark_session


def current_millis_time():
    """
    Get current millis time
    :return:
    """
    return int(time.time() * 1000)


def read_seller_details(path):
    """
    read the seller details json file
    :param path: path of json file
    :return: dataframe of seller details
    """
    return read(spark, path, seller_details_scheme).withColumn('createdDate', to_date(
        to_timestamp((col("createdDate") / 1000).cast(dataType=TimestampType())))) \
        .withColumnRenamed("id", "seller_id") \
        .withColumnRenamed("createdDate", "created_date")


def orders_grouped_by(df):
    """
    :param df: dataframe of orders file
    :return: version of grouped by seller id with counted total sold products
    """
    return df\
        .groupBy("seller_id") \
        .agg(count("itemId").alias("total_sold_prod"))


def visits_grouped_by(df):
    """
    :param df: dataframe of visits file
    :return: version of grouped by seller id with counted total visits
    """
    return df\
        .groupBy("seller_id") \
        .agg(count("*").alias("total_visits"))


def product_review_grouped_by(df):
    """
    :param df: dataframe of product reviews file
    :return: version of grouped by seller id where status is 'APPROVED' with calculated average rate of each seller
    """
    return df \
        .where(col("status") == "APPROVED") \
        .groupBy("seller_id") \
        .agg(round(avg("rate"), 2).alias("avg_rate"))


def seller_scores_grouped_by(df):
    """
    :param df: dataframe of seller scores file
    :return: version of grouped by seller id with calculated average score of each seller
    """
    return df \
        .groupBy("seller_id") \
        .agg(round(avg("score"), 2).alias("avg_score"))


def information_of_seller():
    """
    Find the results of joined table
    :return:
    """
    return seller_details_df \
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


def is_seller_successful(df):
    """
    create a column whose name is is_successful
    check the successful conditions on table, df.
    :param df: dataframe that joined table, seller information
    :return:
    """
    is_successful_condition = (col("total_sold_prod") >= 4) & \
                              (col("total_visits") >= 3) & \
                              (col("avg_rate") >= 2.5) & \
                              (col("avg_score") >= 5) & \
                              (datediff(current_date(), col("created_date")) <= 365)

    return df\
        .withColumn("is_successful", when(is_successful_condition, lit(True)).otherwise(lit(False))) \
        .withColumn("calculated_at", lit(current_millis_time())) \



# Create spark Session
spark = create_spark_session(app_name="successful sellers")

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
orders_df = read(spark, "/Users/berkantmangir/Desktop/PROJECTS/data-analysis-examples/successful-sellers/data/orders.json", order_scheme).withColumnRenamed("sellerId", "seller_id")
prod_review_df = read(spark, "/Users/berkantmangir/Desktop/PROJECTS/data-analysis-examples/successful-sellers/data/productreviews.json", prod_review_scheme)
seller_details_df = read_seller_details(path="/Users/berkantmangir/Desktop/PROJECTS/data-analysis-examples/successful-sellers/data/sellerdetails.json")
seller_scores_df = read(spark, "/Users/berkantmangir/Desktop/PROJECTS/data-analysis-examples/successful-sellers/data/sellerscores.json", seller_scores_scheme)
visits_df = read(spark, "/Users/berkantmangir/Desktop/PROJECTS/data-analysis-examples/successful-sellers/data/visits.json", visits_scheme)

# Make group by
grouped_orders = orders_grouped_by(orders_df)
grouped_visits = visits_grouped_by(visits_df)
grouped_prod_reviews = product_review_grouped_by(prod_review_df)
grouped_seller_scores = seller_scores_grouped_by(seller_scores_df)

seller_info = information_of_seller()

checked_seller_info = is_seller_successful(seller_info)
checked_seller_info.show()
