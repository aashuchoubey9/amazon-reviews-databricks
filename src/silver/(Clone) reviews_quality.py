from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    when,
    current_timestamp
)


@dp.table(
    name="reviews_quality",
    comment="Silver layer containing data quality flags for Amazon Magazine Subscription reviews"
)
def reviews_quality():

    df = spark.readStream.table(
        "workspace.amazon_reviews.reviews_deduplicated"
    )

    return (
        df

        # -----------------------------
        # ASIN quality
        # -----------------------------
        .withColumn(
            "dq_missing_asin",
            when(
                col("asin").isNull() | (col("asin") == ""),
                True
            ).otherwise(False)
        )

        # -----------------------------
        # Reviewer quality
        # -----------------------------
        .withColumn(
            "dq_missing_reviewer",
            when(
                col("reviewerID").isNull() | (col("reviewerID") == ""),
                True
            ).otherwise(False)
        )

        # -----------------------------
        # Rating quality
        # -----------------------------
        .withColumn(
            "dq_missing_rating",
            when(
                col("overall").isNull(),
                True
            ).otherwise(False)
        )

        .withColumn(
            "dq_invalid_rating",
            when(
                col("overall").isNotNull()
                & (
                    (col("overall") < 1)
                    | (col("overall") > 5)
                ),
                True
            ).otherwise(False)
        )

        # -----------------------------
        # Review text quality
        # -----------------------------
        .withColumn(
            "dq_missing_review_text",
            when(
                col("reviewText").isNull() | (col("reviewText") == ""),
                True
            ).otherwise(False)
        )

        # -----------------------------
        # Review date quality
        # -----------------------------
        .withColumn(
            "dq_missing_review_date",
            when(
                col("review_date").isNull()
                & col("reviewTime").isNotNull(),
                True
            ).otherwise(False)
        )

        # -----------------------------
        # Unix timestamp quality
        # -----------------------------
        .withColumn(
            "dq_missing_unix_time",
            when(
                col("unixReviewTime").isNull(),
                True
            ).otherwise(False)
        )

        # -----------------------------
        # Overall quality status
        # -----------------------------
        .withColumn(
    "dq_has_issue",
    (
        col("dq_missing_asin")
        | col("dq_missing_reviewer")
        | col("dq_missing_rating")
        | col("dq_invalid_rating")
        | col("dq_missing_review_text")
        | col("dq_missing_unix_time")
    )
)

        # -----------------------------
        # Processing timestamp
        # -----------------------------
        .withColumn(
            "_quality_checked_at",
            current_timestamp()
        )
    )