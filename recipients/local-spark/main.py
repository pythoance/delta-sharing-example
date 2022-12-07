# Before you can run the project you must install pyspark and delta-sharing (see pyproject.toml)
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import rank, col, when
import delta_sharing

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("LOCAL SPARK TEST APP") \
    .enableHiveSupport() \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.1,io.delta:delta-sharing-spark_2.12:0.6.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# The path to the profile file that you got from the share granter.
profile_file = "config.share"
client = delta_sharing.SharingClient(profile_file)


def show_tables():
    """
    Display all tables that are available with the given profile file.
    """
    print("Tables:")
    for share in client.list_shares():
        for schema in client.list_schemas(share):
            for table in client.list_tables(schema):
                print(f"  - {share.name}.{schema.name}.{table.name}")


def create_local_table(table_name):
    """
    Create a table with the columns: origin(STRING), amount(BIGINT), latest_version(BIGINT).
    :param table_name: The name that the table will have.
    """
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"CREATE TABLE {table_name} (origin STRING, amount BIGINT, latest_version BIGINT) USING DELTA")


def get_max(table_name, column_name, default_value=0):
    """
    Get the max value from a column from a table. If no value is found, the default_value is used.
    :param table_name: The name of the table in which to look.
    :param column_name: The name of the column in which to look.
    :param default_value: The default value in case no value is found.
    :return: The highest version from a column from a table.
    """
    return spark.sql(f"""
        SELECT coalesce(max({column_name}), {default_value})
        FROM {table_name}
        LIMIT 1
    """).collect()[0][0]


def read_changes(source_table_name, table_url, starting_version=0):
    """
    Read the changes that have been made to a remote shared table and put them in a local delta table but exclude:
     - versions older than the latest recorded version in our local table. the newest recorded version will be reprocessed for continuity
     - update_preimage changes because they just show the previous value in case of a row update
     - old changes of a row if there are multiple because only the latest change matters
    :param source_table_name: The name of the table where the changes will be placed.
    :param table_url: The url of the remote table for which to read the changes.
    :param starting_version: The version from where to start reading changes. Defaults to 0.
    """

    window_spec = Window.partitionBy("origin").orderBy("commit_version")

    delta_sharing.load_table_changes_as_spark(url=table_url, starting_version=starting_version) \
        .withColumnRenamed("_commit_version", "commit_version") \
        .withColumnRenamed("_commit_timestamp", "commit_timestamp") \
        .withColumnRenamed("_change_type", "change_type") \
        .where("change_type <> 'update_preimage'") \
        .withColumn("change_type",
                    when(col("change_type") == "update_postimage", "update").otherwise(col("change_type"))) \
        .withColumn("rank", rank().over(window_spec)) \
        .where("rank = 1") \
        .drop("rank") \
        .write \
        .mode("overwrite") \
        .saveAsTable(source_table_name)


def apply_changes(source_table_name, target_table_name):
    """
    Apply the changes of a table into a local delta table.
    :param source_table_name: The name of the delta table that contains the changes.
    :param target_table_name: The name of the delta table where the changes will be merged.
    """
    # TO NOTE: The merge operation only works on delta tables!
    spark.sql(f"""
        MERGE INTO {target_table_name} as TARGET
        USING {source_table_name} as SOURCE
        ON TARGET.origin = SOURCE.origin
        WHEN MATCHED AND SOURCE.change_type='update' AND TARGET.latest_version < SOURCE.commit_version THEN UPDATE SET TARGET.amount = SOURCE.amount, TARGET.latest_version = SOURCE.commit_version
        WHEN MATCHED AND SOURCE.change_type='delete' AND TARGET.latest_version < SOURCE.commit_version THEN DELETE
        WHEN NOT MATCHED AND SOURCE.change_type <> 'delete' THEN INSERT (TARGET.origin, TARGET.amount, TARGET.latest_version) VALUES (SOURCE.origin, SOURCE.amount, SOURCE.commit_version)
    """)


def start():
    table_url = profile_file + "#example_share.default.example_table"
    local_table_name = "recipient_example"
    local_changes_table_name = "recipient_example_changes"
    create_local_table(local_table_name)
    read_changes(local_changes_table_name, table_url, get_max(local_table_name, "latest_version", 0))
    apply_changes(local_changes_table_name, local_table_name)
    spark.sql(f"SELECT * FROM {local_table_name}").show(truncate=False)


if __name__ == '__main__':
    start()
