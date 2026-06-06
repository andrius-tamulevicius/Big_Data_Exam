from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config import CENTER_LATITUDE, CENTER_LONGITUDE, RADIUS_NM
from src.scripts.calculate_distance import bounding_box, haversine_nm
from src.scripts.manage_stages import finish_stage, prepare_stage


def filter_exact_area(spark: SparkSession, input_path: Path, output_path: Path) -> None:
    if not prepare_stage(output_path):
        return

    # Creates a box for the final area
    min_latitude, max_latitude, min_longitude, max_longitude = bounding_box(
        CENTER_LATITUDE,
        CENTER_LONGITUDE,
        RADIUS_NM,
    )

    filtered = (
        spark.read.parquet(str(input_path))
        # Creates the bounding box around the starting point
        .filter(F.col("latitude").between(min_latitude, max_latitude))
        .filter(F.col("longitude").between(min_longitude, max_longitude))
        .withColumn(
            "distance_from_center_nm",
            haversine_nm(
                F.col("latitude"),
                F.col("longitude"),
                F.lit(CENTER_LATITUDE),
                F.lit(CENTER_LONGITUDE),
            ),
        )
        .filter(F.col("distance_from_center_nm") <= RADIUS_NM)
        .drop("distance_from_center_nm")
    )

    filtered.write.mode("overwrite").parquet(str(output_path))
    finish_stage(output_path)
    print(f"Exact area output: {output_path}")
