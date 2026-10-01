from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    sum,
    when
)

@dp.table(
    name="product_rating_distribution",
    comment="Gold product-level distribution of 1 to 5 star ratings"
)
def product_rating_distribution():

    df = (
        spark.read.table(
            "workspace.amazon_reviews.reviews_quality"
        )
        .filter(
            col("overall").isNotNull()
        )
    )

    return (
        df
        .groupBy("asin")
        .agg(

            # 1-star reviews
            sum(
                when(col("overall") == 1, 1)
                .otherwise(0)
            ).alias("rating_1_count"),

            # 2-star reviews
            sum(
                when(col("overall") == 2, 1)
                .otherwise(0)
            ).alias("rating_2_count"),

            # 3-star reviews
            sum(
                when(col("overall") == 3, 1)
                .otherwise(0)
            ).alias("rating_3_count"),

            # 4-star reviews
            sum(
                when(col("overall") == 4, 1)
                .otherwise(0)
            ).alias("rating_4_count"),

            # 5-star reviews
            sum(
                when(col("overall") == 5, 1)
                .otherwise(0)
            ).alias("rating_5_count")
        )
    )