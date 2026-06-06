from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config import (
    COLLISION_DISTANCE_METERS,
    COLLISION_MIN_APPROACH_METERS,
    COLLISION_MIN_BEFORE_POINTS,
    COLLISION_MIN_MOVED_METERS,
    COLLISION_MIN_SPEED_DROP,
    EXCLUDED_SHIP_TYPES,
    MOVING_DISTANCE_METERS,
    MOVING_SPEED_KNOTS,
    MIN_BEFORE_POINTS,
    STATIONARY_DISTANCE_METERS,
    STATIONARY_SPEED_KNOTS,
    VALIDATION_WINDOW_MINUTES,
)
from src.scripts.calculate_distance import haversine_nm
from src.scripts.manage_stages import finish_stage, prepare_stage


def candidate_vessels(candidates):
    first = candidates.select(
        "candidate_id",
        "collision_time",
        F.lit("1").alias("vessel_side"),
        F.col("mmsi_1").alias("mmsi"),
    )
    second = candidates.select(
        "candidate_id",
        "collision_time",
        F.lit("2").alias("vessel_side"),
        F.col("mmsi_2").alias("mmsi"),
    )
    return first.unionByName(second)


def before_stats(vessels, tracks):
    joined = (
        vessels.alias("v")
        .join(
            tracks.alias("t"),
            (F.col("v.mmsi") == F.col("t.mmsi"))
            & (F.col("t.timestamp") >= F.col("v.collision_time") - F.expr(f"INTERVAL {VALIDATION_WINDOW_MINUTES} MINUTES"))
            & (F.col("t.timestamp") <= F.col("v.collision_time")),
        )
    )

    stats = (
        joined
        .groupBy(F.col("v.candidate_id").alias("candidate_id"), F.col("v.vessel_side").alias("vessel_side"))
        .agg(
            F.count("*").alias("before_point_count"),
            F.percentile_approx(F.col("t.sog"), F.lit(0.5)).alias("before_median_sog"),
            F.min_by(
                F.struct(
                    F.col("t.latitude").alias("latitude"),
                    F.col("t.longitude").alias("longitude"),
                ),
                F.col("t.timestamp"),
            ).alias("first_point"),
            F.max_by(
                F.struct(
                    F.col("t.latitude").alias("latitude"),
                    F.col("t.longitude").alias("longitude"),
                ),
                F.col("t.timestamp"),
            ).alias("last_point"),
        )
        .withColumn(
            "before_moved_meters",
            haversine_nm(
                F.col("first_point.latitude"),
                F.col("first_point.longitude"),
                F.col("last_point.latitude"),
                F.col("last_point.longitude"),
            ) * 1852,
        )
        .withColumn("first_latitude", F.col("first_point.latitude"))
        .withColumn("first_longitude", F.col("first_point.longitude"))
        .withColumn("last_latitude", F.col("last_point.latitude"))
        .withColumn("last_longitude", F.col("last_point.longitude"))
        .drop("first_point", "last_point")
    )

    return stats


def after_stats(vessels, tracks):
    joined = (
        vessels.alias("v")
        .join(
            tracks.alias("t"),
            (F.col("v.mmsi") == F.col("t.mmsi"))
            & (F.col("t.timestamp") > F.col("v.collision_time"))
            & (F.col("t.timestamp") <= F.col("v.collision_time") + F.expr(f"INTERVAL {VALIDATION_WINDOW_MINUTES} MINUTES")),
            "left",
        )
    )

    stats = (
        joined
        .groupBy(F.col("v.candidate_id").alias("candidate_id"), F.col("v.vessel_side").alias("vessel_side"))
        .agg(
            F.count("t.timestamp").alias("after_point_count"),
            F.percentile_approx(F.col("t.sog"), F.lit(0.5)).alias("after_median_sog"),
        )
    )

    return stats


def select_side_stats(stats, side: str, prefix: str):
    return (
        stats
        .filter(F.col("vessel_side") == side)
        .select(
            "candidate_id",
            *[
                F.col(column).alias(f"{prefix}_{column}")
                for column in stats.columns
                if column not in ["candidate_id", "vessel_side"]
            ],
        )
    )


def vessel_types(tracks):
    return (
        tracks
        .filter(F.col("ship_type").isNotNull())
        .filter(F.col("ship_type") != "Undefined")
        .groupBy("mmsi")
        .agg(F.max_by("ship_type", "timestamp").alias("ship_type"))
    )


def filter_collision_candidates(spark: SparkSession, candidates_path: Path, tracks_path: Path, output_path: Path) -> None:
    if not prepare_stage(output_path):
        return

    candidates = spark.read.parquet(str(candidates_path))
    tracks = spark.read.parquet(str(tracks_path)).select("mmsi", "timestamp", "latitude", "longitude", "sog", "ship_type")
    types = vessel_types(tracks)
    type_1 = types.select(F.col("mmsi").alias("mmsi_1"), F.col("ship_type").alias("vessel_1_ship_type"))
    type_2 = types.select(F.col("mmsi").alias("mmsi_2"), F.col("ship_type").alias("vessel_2_ship_type"))
    candidates = candidates.join(type_1, "mmsi_1", "left").join(type_2, "mmsi_2", "left")
    vessels = candidate_vessels(candidates)

    before = before_stats(vessels, tracks)
    after = after_stats(vessels, tracks)

    before_1 = select_side_stats(before, "1", "vessel_1")
    before_2 = select_side_stats(before, "2", "vessel_2")
    after_1 = select_side_stats(after, "1", "vessel_1")
    after_2 = select_side_stats(after, "2", "vessel_2")

    scored = (
        candidates
        .join(before_1, "candidate_id")
        .join(before_2, "candidate_id")
        .join(after_1, "candidate_id")
        .join(after_2, "candidate_id")
        .withColumn(
            "vessel_1_was_moving",
            (F.col("vessel_1_before_median_sog") >= MOVING_SPEED_KNOTS)
            | (F.col("vessel_1_before_moved_meters") >= MOVING_DISTANCE_METERS),
        )
        .withColumn(
            "vessel_2_was_moving",
            (F.col("vessel_2_before_median_sog") >= MOVING_SPEED_KNOTS)
            | (F.col("vessel_2_before_moved_meters") >= MOVING_DISTANCE_METERS),
        )
        .withColumn(
            "both_were_stationary",
            (F.col("vessel_1_before_median_sog") < STATIONARY_SPEED_KNOTS)
            & (F.col("vessel_2_before_median_sog") < STATIONARY_SPEED_KNOTS)
            & (F.col("vessel_1_before_moved_meters") < STATIONARY_DISTANCE_METERS)
            & (F.col("vessel_2_before_moved_meters") < STATIONARY_DISTANCE_METERS),
        )
        .filter(F.col("vessel_1_was_moving"))
        .filter(F.col("vessel_2_was_moving"))
        .filter(F.col("vessel_1_before_point_count") >= MIN_BEFORE_POINTS)
        .filter(F.col("vessel_2_before_point_count") >= MIN_BEFORE_POINTS)
        .filter(F.col("vessel_1_before_median_sog") >= MOVING_SPEED_KNOTS)
        .filter(F.col("vessel_2_before_median_sog") >= MOVING_SPEED_KNOTS)
        .filter(F.col("vessel_1_before_moved_meters") >= MOVING_DISTANCE_METERS)
        .filter(F.col("vessel_2_before_moved_meters") >= MOVING_DISTANCE_METERS)
        .filter(~F.col("both_were_stationary"))
        .withColumn(
            "vessel_1_after_speed",
            F.col("vessel_1_after_median_sog"),
        )
        .withColumn(
            "vessel_2_after_speed",
            F.col("vessel_2_after_median_sog"),
        )
        .withColumn(
            "vessel_1_speed_drop_pct",
            F.when(
                F.col("vessel_1_before_median_sog") > 0,
                (F.col("vessel_1_before_median_sog") - F.col("vessel_1_after_speed")) / F.col("vessel_1_before_median_sog"),
            ),
        )
        .withColumn(
            "vessel_2_speed_drop_pct",
            F.when(
                F.col("vessel_2_before_median_sog") > 0,
                (F.col("vessel_2_before_median_sog") - F.col("vessel_2_after_speed")) / F.col("vessel_2_before_median_sog"),
            ),
        )
        .withColumn(
            "combined_speed_drop_pct",
            F.coalesce(F.col("vessel_1_speed_drop_pct"), F.lit(0)) + F.coalesce(F.col("vessel_2_speed_drop_pct"), F.lit(0)),
        )
        .withColumn(
            "pre_event_pair_distance_meters",
            haversine_nm(
                F.col("vessel_1_first_latitude"),
                F.col("vessel_1_first_longitude"),
                F.col("vessel_2_first_latitude"),
                F.col("vessel_2_first_longitude"),
            ) * 1852,
        )
        .withColumn("minimum_distance_meters", F.col("minimum_distance_nm") * 1852)
        .withColumn("approach_distance_meters", F.col("pre_event_pair_distance_meters") - F.col("minimum_distance_meters"))
        .withColumn("vessels_approached", F.col("approach_distance_meters") > 0)
        .withColumn("vessel_1_is_excluded_type", F.col("vessel_1_ship_type").isin(EXCLUDED_SHIP_TYPES))
        .withColumn("vessel_2_is_excluded_type", F.col("vessel_2_ship_type").isin(EXCLUDED_SHIP_TYPES))
        .filter(F.col("minimum_distance_meters") <= COLLISION_DISTANCE_METERS)
        .filter(F.col("combined_speed_drop_pct") >= COLLISION_MIN_SPEED_DROP)
        .filter(F.col("vessel_1_before_point_count") >= COLLISION_MIN_BEFORE_POINTS)
        .filter(F.col("vessel_2_before_point_count") >= COLLISION_MIN_BEFORE_POINTS)
        .filter(F.col("vessel_1_before_moved_meters") >= COLLISION_MIN_MOVED_METERS)
        .filter(F.col("vessel_2_before_moved_meters") >= COLLISION_MIN_MOVED_METERS)
        .filter(F.col("approach_distance_meters") >= COLLISION_MIN_APPROACH_METERS)
        .filter(~F.coalesce(F.col("vessel_1_is_excluded_type"), F.lit(False)))
        .filter(~F.coalesce(F.col("vessel_2_is_excluded_type"), F.lit(False)))
    )

    scored.write.mode("overwrite").parquet(str(output_path))
    finish_stage(output_path)
    print(f"Validated candidates output: {output_path}")
