from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum, when


@dp.table(
    name="brand_rating_distribution",
    comment="Gold brand-level distribution of 1 to 5 star ratings using product metadata"
)
def brand_rating_distribution():

    reviews = (
        spark.read.table(
            "workspace.amazon_reviews.reviews_quality"
        )
        .filter(
            col("overall").isNotNull()
        )
        .select(
            "asin",
            "overall"
        )
    )

    product_metadata = (
        spark.read.table(
            "workspace.amazon_reviews.product_metadata_bronze"
        )
        .select(
            "asin",
            "brand"
        )
        .dropDuplicates(["asin"])
    )

    enriched_reviews = (
        reviews
        .join(
            product_metadata,
            on="asin",
            how="left"
        )
    )

    return (
        enriched_reviews
        .groupBy("brand")
        .agg(

            sum(
                when(col("overall") == 1, 1)
                .otherwise(0)
            ).alias("rating_1_count"),

            sum(
                when(col("overall") == 2, 1)
                .otherwise(0)
            ).alias("rating_2_count"),

            sum(
                when(col("overall") == 3, 1)
                .otherwise(0)
            ).alias("rating_3_count"),

            sum(
                when(col("overall") == 4, 1)
                .otherwise(0)
            ).alias("rating_4_count"),

            sum(
                when(col("overall") == 5, 1)
                .otherwise(0)
            ).alias("rating_5_count")
        )
    )