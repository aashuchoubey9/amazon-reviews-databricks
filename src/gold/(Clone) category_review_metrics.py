from pyspark import pipelines as dp
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    sum,
    trim,
    when
)


@dp.table(
    name="category_review_metrics",
    comment="Gold category-level metrics for reviews, ratings, products, reviewers, verification, and helpfulness"
)
def category_review_metrics():

    df = (
        spark.read.table(
            "workspace.amazon_reviews.reviews_quality"
        )
        .withColumn(
            "category_name",
            trim(col("category"))
        )
        .filter(
            col("category_name").isNotNull()
            & (col("category_name") != "")
        )
    )

    return (
        df
        .groupBy("category_name")
        .agg(

            # Total reviews
            count("*").alias("review_count"),

            # Products in the category
            countDistinct("asin").alias("unique_products"),

            # Reviewers in the category
            countDistinct("reviewerID").alias("unique_reviewers"),

            # Average rating
            avg("overall").alias("average_rating"),

            # Verified reviews
            sum(
                when(
                    col("verified") == True,
                    1
                )
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
            ).alias("helpful_vote_count")
        )
    )