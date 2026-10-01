from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    trim,
    regexp_extract,
    regexp_replace,
    when,
    current_timestamp,
    make_date
)


@dp.table(
    name="reviews_cleaned",
    comment="Silver layer containing cleaned and standardized Amazon Magazine Subscription reviews"
)
def reviews_cleaned():

    df = spark.readStream.table(
        "workspace.amazon_reviews.reviews_bronze"
    )

    return (
        df

        # -----------------------------
        # Clean string columns
        # -----------------------------
        .withColumn("asin", trim(col("asin")))
        .withColumn("reviewerID", trim(col("reviewerID")))
        .withColumn("reviewerName", trim(col("reviewerName")))
        .withColumn("brand", trim(col("brand")))
        .withColumn("title", trim(col("title")))
        .withColumn("main_cat", trim(col("main_cat")))
        .withColumn("summary", trim(col("summary")))
        .withColumn("reviewText", trim(col("reviewText")))

        # -----------------------------
        # Standardize review date
        # Source examples:
        # 09 3, 2017
        # 5 5, 2017
        # 02 10, 2017
        # 8 18, 2015
        # -----------------------------
        .withColumn(
            "_review_month",
            regexp_extract(
                trim(col("reviewTime")),
                r"^(\d{1,2})\s+(\d{1,2}),\s*(\d{4})$",
                1
            ).cast("int")
        )
        .withColumn(
            "_review_day",
            regexp_extract(
                trim(col("reviewTime")),
                r"^(\d{1,2})\s+(\d{1,2}),\s*(\d{4})$",
                2
            ).cast("int")
        )
        .withColumn(
            "_review_year",
            regexp_extract(
                trim(col("reviewTime")),
                r"^(\d{1,2})\s+(\d{1,2}),\s*(\d{4})$",
                3
            ).cast("int")
        )
        .withColumn(
            "review_date",
            make_date(
                col("_review_year"),
                col("_review_month"),
                col("_review_day")
            )
        )

        # -----------------------------
        # Convert vote to integer
        # Examples:
        # "2" -> 2
        # "6" -> 6
        # "-" -> NULL
        # -----------------------------
        .withColumn(
            "vote_count",
            when(
                regexp_replace(col("vote"), "[^0-9]", "") != "",
                regexp_replace(
                    col("vote"),
                    "[^0-9]",
                    ""
                ).cast("int")
            )
        )

        # -----------------------------
        # Silver processing timestamp
        # -----------------------------
        .withColumn(
            "_silver_processed_at",
            current_timestamp()
        )

        # -----------------------------
        # Remove temporary date columns
        # -----------------------------
        .drop(
            "_review_month",
            "_review_day",
            "_review_year"
        )
    )