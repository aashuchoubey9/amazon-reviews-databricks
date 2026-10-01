from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    round,
    when
)


@dp.table(
    name="product_performance_metrics",
    comment="Gold product performance KPIs combining review, rating, verification, and helpfulness metrics"
)
def product_performance_metrics():

    product_summary = spark.read.table(
        "workspace.amazon_reviews.product_review_summary"
    )

    return (
        product_summary
        .select(
            "asin",
            "review_count",
            "average_rating",
            "rating_1_count",
            "rating_2_count",
            "rating_3_count",
            "rating_4_count",
            "rating_5_count",
            "verified_review_count",
            "unique_reviewers",
            "helpful_vote_count"
        )
        .withColumn(
            "positive_rating_percentage",
            round(
                when(
                    col("review_count") > 0,
                    (
                        (
                            col("rating_4_count")
                            + col("rating_5_count")
                        )
                        / col("review_count")
                    ) * 100
                ).otherwise(0),
                2
            )
        )
        .withColumn(
            "negative_rating_percentage",
            round(
                when(
                    col("review_count") > 0,
                    (
                        (
                            col("rating_1_count")
                            + col("rating_2_count")
                        )
                        / col("review_count")
                    ) * 100
                ).otherwise(0),
                2
            )
        )
    )