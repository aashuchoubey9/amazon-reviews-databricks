from pyspark import pipelines as dp


@dp.table(
    name="product_review_summary",
    comment="Gold dashboard-ready summary of product review and rating metrics"
)
def product_review_summary():

    product_metrics = spark.read.table(
        "workspace.amazon_reviews.product_review_metrics"
    )

    rating_distribution = spark.read.table(
        "workspace.amazon_reviews.product_rating_distribution"
    )

    return (
        product_metrics.alias("pm")
        .join(
            rating_distribution.alias("rd"),
            on="asin",
            how="left"
        )
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
    )