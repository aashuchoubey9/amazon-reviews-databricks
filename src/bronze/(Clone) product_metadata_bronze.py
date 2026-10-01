from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col


@dp.table(
    name="product_metadata_bronze",
    comment="Bronze layer containing raw Amazon Magazine Subscription product metadata"
)
def product_metadata_bronze():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/Volumes/workspace/amazon_reviews/raw_reviews/")
        .filter(
            col("_metadata.file_path").endswith(
                "meta_Magazine_Subscriptions.json"
            )
        )
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )