from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.config import GPS_EDGE_SPEED_LIMIT_KNOTS, GPS_SPEED_LIMIT_KNOTS
from src.scripts.calculate_distance import haversine_nm
from src.scripts.manage_stages import finish_stage, prepare_stage


def remove_gps_jumps(spark: SparkSession, input_path: Path, output_path: Path) -> None:
    if not prepare_stage(output_path):
        return

    # Reads the cleaned data
    data = spark.read.parquet(str(input_path))

    # Find records at the same time with conflicting positions
    bad_duplicate_times = (
        data
        .groupBy("mmsi", "timestamp")
        .agg(F.count_distinct("latitude", "longitude").alias("locations"))
        .filter(F.col("locations") > 1)
        .select("mmsi", "timestamp")
    )

    data = (
        # Removes duplicates
        data
        .join(bad_duplicate_times, ["mmsi", "timestamp"], "left_anti")
        .dropDuplicates(["mmsi", "timestamp", "latitude", "longitude"])
    )

    vessel_window = Window.partitionBy("mmsi").orderBy("timestamp")

    # Collects previous and next AIS points for each vessel
    with_neighbors = (
        data
        .repartition("mmsi")
        .withColumn("previous_timestamp", F.lag("timestamp").over(vessel_window))
        .withColumn("previous_latitude", F.lag("latitude").over(vessel_window))
        .withColumn("previous_longitude", F.lag("longitude").over(vessel_window))
        .withColumn("next_timestamp", F.lead("timestamp").over(vessel_window))
        .withColumn("next_latitude", F.lead("latitude").over(vessel_window))
        .withColumn("next_longitude", F.lead("longitude").over(vessel_window))
    )

    cleaned = (
        with_neighbors
        # Calculates time gaps
        .withColumn(
            "previous_hours",
            (F.unix_timestamp("timestamp") - F.unix_timestamp("previous_timestamp")) / 3600,
        )
        .withColumn(
            "next_hours",
            (F.unix_timestamp("next_timestamp") - F.unix_timestamp("timestamp")) / 3600,
        )
        # Calculates distances to neighboring points
        .withColumn(
            "previous_distance_nm",
            haversine_nm(
                F.col("previous_latitude"),
                F.col("previous_longitude"),
                F.col("latitude"),
                F.col("longitude"),
            ),
        )
        .withColumn(
            "next_distance_nm",
            haversine_nm(
                F.col("latitude"),
                F.col("longitude"),
                F.col("next_latitude"),
                F.col("next_longitude"),
            ),
        )
        # Converts distance and time to speed
        .withColumn(
            "previous_speed_knots",
            F.when(F.col("previous_hours") > 0, F.col("previous_distance_nm") / F.col("previous_hours")),
        )
        .withColumn(
            "next_speed_knots",
            F.when(F.col("next_hours") > 0, F.col("next_distance_nm") / F.col("next_hours")),
        )
        # Marks impossible isolated GPS jumps
        .withColumn(
            "is_middle_spike",
            (F.col("previous_speed_knots") > GPS_SPEED_LIMIT_KNOTS)
            & (F.col("next_speed_knots") > GPS_SPEED_LIMIT_KNOTS),
        )
        # Marks impossible first or last points
        .withColumn(
            "is_edge_spike",
            (
                F.col("previous_speed_knots").isNull()
                & (F.col("next_speed_knots") > GPS_EDGE_SPEED_LIMIT_KNOTS)
            )
            | (
                F.col("next_speed_knots").isNull()
                & (F.col("previous_speed_knots") > GPS_EDGE_SPEED_LIMIT_KNOTS)
            ),
        )
        # Keeps only valid track points
        .filter(~F.col("is_middle_spike"))
        .filter(~F.col("is_edge_spike"))
        .select(
            "mmsi",
            "timestamp",
            "latitude",
            "longitude",
            "navigational_status",
            "sog",
            "cog",
            "heading",
            "name",
            "ship_type",
            "width",
            "length",
            "draught",
        )
    )

    cleaned.write.mode("overwrite").parquet(str(output_path))
    finish_stage(output_path)
    print(f"GPS-cleaned output: {output_path}")
