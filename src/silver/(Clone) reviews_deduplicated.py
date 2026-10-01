from pyspark import pipelines as dp


@dp.table(
    name="reviews_deduplicated",
    comment="Silver layer containing deduplicated Amazon Magazine Subscription reviews"
)
def reviews_deduplicated():

    df = spark.readStream.table(
        "workspace.amazon_reviews.reviews_cleaned"
    )

    return (
        df
        .dropDuplicates(
            [
                "asin",
                "reviewerID",
                "unixReviewTime",
                "reviewText"
            ]
        )
    )