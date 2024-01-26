from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .master("local") \
    .appName('word_count') \
    .getOrCreate()

# spark.sparkContext.setLogLevel("INFO")

schema = StructType(
    [
        StructField(dataType=LongType(), name="categoryId", nullable=False),
        StructField(dataType=LongType(), name="itemId", nullable=False),
        StructField(dataType=StringType(), name="orderDate", nullable=True),
        StructField(dataType=LongType(), name="orderId", nullable=False),
        StructField(dataType=DoubleType(), name="price", nullable=True),
        StructField(dataType=LongType(), name="sellerId", nullable=False),
        StructField(dataType=StringType(), name="status", nullable=True)
    ]
)

df = spark.read.json("/Users/berkantmangir/Desktop/PROJECTS/data-analysis-examples/sales-distribution/data/orders_input.json", schema=schema)

output = df.withColumn("orderDate", to_date("orderDate")) \
    .groupBy("orderDate", "sellerId") \
    .agg(
    countDistinct("orderId").alias("distinct_order_count"),
    count("itemID").alias("item_count"),
    sum("price").alias("total_price")
) \
    .withColumn("distribution",
                when(col("total_price").between(0.00, 99.99), 1)
                .when(col("total_price").between(100.00, 499.99), 2)
                .when(col("total_price").between(500.00, 1999.99), 3)
                .otherwise(4)
                )

output.show()
