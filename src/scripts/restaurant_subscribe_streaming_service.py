import time

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import from_json, to_json, col, lit, struct, unix_timestamp, current_timestamp

SPARK_JARS_PACKAGES = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

KAFKA_SECURITY_OPTIONS = {
    'kafka.bootstrap.servers': 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}

POSTGRES_SECURITY_OPTIONS = {
    'url': 'jdbc:postgresql://localhost:5432/postgres',
    'driver': 'org.postgresql.Driver',
    'user': 'jovyan',
    'password': 'jovyan',
}

def spark_init() -> SparkSession:

    spark = (SparkSession.builder
        .appName("RestaurantSubscribeStreamingService")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", SPARK_JARS_PACKAGES)
        .getOrCreate()
    )

    return spark

def read_kafka_stream(spark: SparkSession) -> DataFrame:

    restaurant_read_stream_df = (spark.readStream
        .format('kafka')
        .options(**KAFKA_SECURITY_OPTIONS)
        .option('subscribe', 'ivivchick_in')
        .load()
    )

    return restaurant_read_stream_df

def transform(df: DataFrame) -> DataFrame:

    incomming_message_schema = StructType(
        [StructField("restaurant_id", StringType(), True),
        StructField("adv_campaign_id", StringType(), True),
        StructField("adv_campaign_content", StringType(), True),
        StructField("adv_campaign_owner", StringType(), True),
        StructField("adv_campaign_owner_contact", StringType(), True),
        StructField("adv_campaign_datetime_start", LongType(), True),
        StructField("adv_campaign_datetime_end", LongType(), True),
        StructField("datetime_created", LongType(), True)])

    filtered_read_stream_df = (df
        .withColumn('value', from_json(col('value').cast(StringType()), incomming_message_schema))
        .withColumn('key', col('key').cast(StringType()))
        .selectExpr('value.*', '*')
        .dropDuplicates(['restaurant_id', 'adv_campaign_id'])
        .withWatermark('timestamp', '5 minute')
        .filter(col('adv_campaign_datetime_end') > unix_timestamp(current_timestamp()))
    )

    return filtered_read_stream_df

def read_from_postgres(spark: SparkSession) -> DataFrame:
    subscribers_restaurant_df = (spark.read
        .format('jdbc')
        .options(**POSTGRES_SECURITY_OPTIONS)
        .option('dbtable', 'subscribers_restaurants')
        .load()
    )

    return subscribers_restaurant_df

def foreach_batch_function(df, epoch_id) -> None:
    perst_df = (df.select(
                    col('restaurant_id'),
                    col('adv_campaign_id'),
                    col('adv_campaign_content'),
                    col('adv_campaign_owner'),
                    col('adv_campaign_owner_contact'),
                    col('adv_campaign_datetime_start'),
                    col('adv_campaign_datetime_end'),
                    col('datetime_created'),
                    col('client_id'),
                    unix_timestamp(current_timestamp()).alias('trigger_datetime_created')
                 )
               .persist()
    )

    (perst_df
        .withColumn('feedback', lit(""))
        .write
        .format('jdbc')
        .options(**POSTGRES_SECURITY_OPTIONS)
        .option('dbtable', 'subscribers_feedback')
        .mode("append")
        .save()
    )

    (perst_df
         .select(to_json(
                    struct(
                        col('restaurant_id'),
                        col('adv_campaign_id'),
                        col('adv_campaign_content'),
                        col('adv_campaign_owner'),
                        col('adv_campaign_owner_contact'),
                        col('adv_campaign_datetime_start'),
                        col('adv_campaign_datetime_end'),
                        col('datetime_created'),
                        col('client_id'),
                        col('trigger_datetime_created'),
                    )).cast(StringType()).alias('value')
         )
         .write
         .format("kafka")
         .options(**KAFKA_SECURITY_OPTIONS)
         .option('topic', 'ivivchick_out')
         .save()

    )
    perst_df.unpersist()

def main():

    spark = spark_init()
    stream_df = read_kafka_stream(spark)
    postgres_df = read_from_postgres(spark)

    filtered_stream_df = transform(stream_df)

    result_df = filtered_stream_df.join(postgres_df, ['restaurant_id'])

    query = (result_df.writeStream
        .trigger(processingTime='5 seconds')
        .foreachBatch(foreach_batch_function)
        .start())

    while query.isActive:
        print(f"query information: runId={query.runId}, "
              f"status is {query.status}, "
              f"recent progress={query.recentProgress}")
        time.sleep(15)

    try:
        query.awaitTermination()
    finally:
        query.stop()

if __name__ == "__main__":
    main()