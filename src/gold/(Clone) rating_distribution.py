from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    round,
    when
)


@dp.table(
    name="rating_distribution",
    comment="Gold rating distribution metrics for Amazon Magazine Subscription reviews"
)
def rating_distribution():

    df = (
        spark.read.table(
            "workspace.amazon_reviews.reviews_quality"
        )
        .filter(
            col("overall").isNotNull()
        )
    )

    total_rated_reviews = df.count()

    return (
        df
        .groupBy("overall")
        .agg(
            count("*").alias(
                "review_count"
            ),

            round(
                count("*") * 100.0 / total_rated_reviews,
                2
            ).alias(
                "review_percentage"
            ),

            sum(
                when(
                    col("verified") == True,
                    1
                ).otherwise(0)
            ).alias(
                "verified_review_count"
            ),

            round(
                avg(
                    when(
                        col("vote_count").isNotNull(),
                        col("vote_count")
                    )
                ),
                2
            ).alias(
                "average_helpful_votes"
            )
        )
        .orderBy("overall")
    )