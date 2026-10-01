from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    avg,
    sum,
    when,
    round
)

@dp.table(
    name="brand_review_metrics",
    comment="Gold brand-level review metrics for Amazon Magazine Subscription products"
)
def brand_review_metrics():

    df = spark.read.table(
        "workspace.amazon_reviews.reviews_quality"
    )

    return (
        df
        .groupBy("brand")
        .agg(
            count("*").alias(
                "review_count"
            ),

            countDistinct("asin").alias(
                "product_count"
            ),

            countDistinct("reviewerID").alias(
                "unique_reviewers"
            ),

            round(
                avg("overall"),
                2
            ).alias(
                "average_rating"
            ),

            sum(
                when(
                    col("verified") == True,
                    1
                ).otherwise(0)
            ).alias(
                "verified_review_count"
            ),

            sum(
                when(
                    col("reviewText").isNotNull()
                    & (col("reviewText") != ""),
                    1
                ).otherwise(0)
            ).alias(
                "text_review_count"
            ),

            sum(
                when(
                    col("vote_count").isNotNull(),
                    col("vote_count")
                ).otherwise(0)
            ).alias(
                "helpful_vote_count"
            )
        )
    )