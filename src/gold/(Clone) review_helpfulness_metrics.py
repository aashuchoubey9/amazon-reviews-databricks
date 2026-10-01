from pyspark import pipelines as dp
from pyspark.sql.functions import (
    avg,
    col,
    count,
    max,
    sum,
    when
)


@dp.table(
    name="review_helpfulness_metrics",
    comment="Gold product-level metrics for review helpfulness and engagement"
)
def review_helpfulness_metrics():

    df = (
        spark.read.table(
            "workspace.amazon_reviews.reviews_quality"
        )
        .filter(
            col("asin").isNotNull()
        )
    )

    return (
        df
        .groupBy("asin")
        .agg(

            # Total reviews
            count("*").alias("review_count"),

            # Reviews that contain a helpful vote
            sum(
                when(
                    col("vote").isNotNull(),
                    1
                )
                .otherwise(0)
            ).alias("reviews_with_helpful_votes"),

            # Total helpful votes
            sum(
                when(
                    col("vote").isNotNull(),
                    col("vote")
                )
                .otherwise(0)
            ).alias("total_helpful_votes"),

            # Average helpful votes per review
            avg(
                when(
                    col("vote").isNotNull(),
                    col("vote")
                )
            ).alias("average_helpful_votes"),

            # Maximum helpful votes received by a review
            max(
                col("vote")
            ).alias("max_helpful_votes"),

            # Verified reviews
            sum(
                when(
                    col("verified") == True,
                    1
                )
                .otherwise(0)
            ).alias("verified_review_count"),

            # Average product rating
            avg(
                col("overall")
            ).alias("average_rating")
        )
    )