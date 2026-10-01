from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    from_unixtime,
    to_date,
    date_format,
    count,
    countDistinct,
    avg,
    sum,
    when,
    round
)


@dp.table(
    name="review_trends",
    comment="Gold time-based review trend metrics for Amazon Magazine Subscription reviews"
)
def review_trends():

    df = (
        spark.read.table(
            "workspace.amazon_reviews.reviews_quality"
        )
        .filter(
            col("unixReviewTime").isNotNull()
        )
        .withColumn(
            "review_timestamp",
            from_unixtime(col("unixReviewTime"))
        )
        .withColumn(
            "review_date_from_unix",
            to_date(col("review_timestamp"))
        )
        .withColumn(
            "review_month",
            date_format(
                col("review_timestamp"),
                "yyyy-MM"
            )
        )
    )

    return (
        df
        .groupBy("review_month")
        .agg(
            count("*").alias(
                "review_count"
            ),

            countDistinct("asin").alias(
                "unique_products"
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
        .orderBy("review_month")
    )