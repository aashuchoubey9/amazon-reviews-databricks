from pyspark import pipelines as dp
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    max,
    min,
    sum,
    to_date,
    when
)

@dp.table(
    name="reviewer_activity_metrics",
    comment="Gold reviewer-level metrics for activity, ratings, verification, and helpfulness"
)
def reviewer_activity_metrics():

    df = (
        spark.read.table(
            "workspace.amazon_reviews.reviews_quality"
        )
        .filter(
            col("reviewerID").isNotNull()
        )
    )

    return (
        df
        .groupBy(
            "reviewerID",
            "reviewerName"
        )
        .agg(

            # Total reviews written
            count("*").alias("review_count"),

            # Number of different products reviewed
            countDistinct("asin").alias("unique_products_reviewed"),

            # Rating statistics
            avg("overall").alias("average_rating"),
            min("overall").alias("min_rating"),
            max("overall").alias("max_rating"),

            # Verified reviews
            sum(
                when(col("verified") == True, 1)
                .otherwise(0)
            ).alias("verified_review_count"),

            # Reviews containing text
            sum(
                when(
                    col("reviewText").isNotNull()
                    & (col("reviewText") != ""),
                    1
                )
                .otherwise(0)
            ).alias("text_review_count"),

            # Helpful votes
            sum(
                when(
                    col("vote").isNotNull(),
                    col("vote")
                )
                .otherwise(0)
            ).alias("helpful_vote_count"),

            # Reviewer activity period
            min(
                to_date(
                    col("review_date")
                )
            ).alias("first_review_date"),

            max(
                to_date(
                    col("review_date")
                )
            ).alias("last_review_date")
        )
    )