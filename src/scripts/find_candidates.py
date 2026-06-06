from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.config import CANDIDATE_DISTANCE_NM, GRID_SIZE_DEGREES, TIME_BUCKET_SECONDS
from src.scripts.calculate_distance import haversine_nm
from src.scripts.manage_stages import finish_stage, prepare_stage


def find_collision_candidates(spark: SparkSession, input_path: Path, output_path: Path) -> None:
    if not prepare_stage(output_path):
        return

    # Puts each point into a time bucket and grid cell
    data = (
        spark.read.parquet(str(input_path))
        .withColumn("time_bucket", F.floor(F.unix_timestamp("timestamp") / TIME_BUCKET_SECONDS))
        .withColumn("lat_cell", F.floor(F.col("latitude") / GRID_SIZE_DEGREES))
        .withColumn("lon_cell", F.floor(F.col("longitude") / GRID_SIZE_DEGREES))
        .select(
            "mmsi",
            "timestamp",
            "latitude",
            "longitude",
            "name",
            "sog",
            "time_bucket",
            "lat_cell",
            "lon_cell",
        )
    )

    # Expands the left side to nearby buckets and cells
    left = (
        data
        .withColumn("join_time_bucket", F.explode(F.array(
            F.col("time_bucket") - 1,
            F.col("time_bucket"),
            F.col("time_bucket") + 1,
        )))
        .withColumn("join_lat_cell", F.explode(F.array(
            F.col("lat_cell") - 1,
            F.col("lat_cell"),
            F.col("lat_cell") + 1,
        )))
        .withColumn("join_lon_cell", F.explode(F.array(
            F.col("lon_cell") - 1,
            F.col("lon_cell"),
            F.col("lon_cell") + 1,
        )))
        .alias("left")
    )

    # Prepares the right side for the bucket join
    right = (
        data
        .withColumnRenamed("time_bucket", "join_time_bucket")
        .withColumnRenamed("lat_cell", "join_lat_cell")
        .withColumnRenamed("lon_cell", "join_lon_cell")
        .alias("right")
    )

    # Joins the data
    joined = (
        left
        .join(right, ["join_time_bucket", "join_lat_cell", "join_lon_cell"])
        # Keeps each pair once
        .filter(F.col("left.mmsi") < F.col("right.mmsi"))
        # Keeps points that are close in time.
        .filter(F.abs(F.unix_timestamp(F.col("left.timestamp")) - F.unix_timestamp(F.col("right.timestamp"))) <= TIME_BUCKET_SECONDS)
        # Calculates exact distance
        .withColumn(
            "distance_nm",
            haversine_nm(
                F.col("left.latitude"),
                F.col("left.longitude"),
                F.col("right.latitude"),
                F.col("right.longitude"),
            ),
        )
        # Keeps very close vessel pairs
        .filter(F.col("distance_nm") <= CANDIDATE_DISTANCE_NM)
        .select(
            F.col("left.mmsi").alias("mmsi_1"),
            F.col("right.mmsi").alias("mmsi_2"),
            F.col("left.name").alias("name_1"),
            F.col("right.name").alias("name_2"),
            F.col("left.timestamp").alias("timestamp_1"),
            F.col("right.timestamp").alias("timestamp_2"),
            ((F.unix_timestamp(F.col("left.timestamp")) + F.unix_timestamp(F.col("right.timestamp"))) / 2).alias("collision_seconds"),
            ((F.col("left.latitude") + F.col("right.latitude")) / 2).alias("collision_latitude"),
            ((F.col("left.longitude") + F.col("right.longitude")) / 2).alias("collision_longitude"),
            "distance_nm",
        )
        .dropDuplicates(["mmsi_1", "mmsi_2", "timestamp_1", "timestamp_2"])
    )

    # Keeps the closest point for each vessel pair
    pair_window = Window.partitionBy("mmsi_1", "mmsi_2").orderBy("distance_nm")

    candidates = (
        joined
        .withColumn("rank", F.row_number().over(pair_window))
        .filter(F.col("rank") == 1)
        .drop("rank")
        .withColumn("collision_time", F.from_unixtime(F.col("collision_seconds")).cast("timestamp"))
        .withColumn(
            "candidate_id",
            F.sha2(F.concat_ws("|", "mmsi_1", "mmsi_2", F.col("collision_time").cast("string")), 256),
        )
        .select(
            "candidate_id",
            "mmsi_1",
            "mmsi_2",
            "name_1",
            "name_2",
            "collision_time",
            "collision_latitude",
            "collision_longitude",
            F.col("distance_nm").alias("minimum_distance_nm"),
        )
    )

    candidates.write.mode("overwrite").parquet(str(output_path))
    finish_stage(output_path)
    print(f"Collision candidates output: {output_path}")
