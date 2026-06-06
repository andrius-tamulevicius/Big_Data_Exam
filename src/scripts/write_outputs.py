import json
from pathlib import Path

import folium
from branca.element import Element
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config import VALIDATION_WINDOW_MINUTES
from src.scripts.manage_stages import finish_stage, prepare_stage


def write_final_outputs(spark: SparkSession, candidates_path: Path, tracks_path: Path, result_path: Path, map_path: Path) -> None:
    if not prepare_stage(result_path):
        return

    candidates = spark.read.parquet(str(candidates_path))
    rows = candidates.orderBy("collision_time").collect()

    result_path.parent.mkdir(parents=True, exist_ok=True)
    map_path.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        result_path.write_text(json.dumps({"collision_found": False}, indent=2), encoding="utf-8")
        finish_stage(result_path)
        return

    collisions = []

    for row in rows:
        collision = row.asDict()
        collision["collision_found"] = True
        collision["collision_time"] = str(collision["collision_time"])
        collisions.append(collision)

    result = collisions[0]

    if len(collisions) > 1:
        result = {
            "collision_found": True,
            "collision_count": len(collisions),
            "collisions": collisions,
        }

    result_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")

    collision = collisions[0]
    write_map(spark, tracks_path, collision, map_path)
    finish_stage(result_path)

    print("Final collision result")
    print(f"Collision candidates found: {len(collisions)}")
    print(f"MMSI 1: {collision['mmsi_1']}")
    print(f"Name 1: {collision['name_1']}")
    print(f"MMSI 2: {collision['mmsi_2']}")
    print(f"Name 2: {collision['name_2']}")
    print(f"Collision time: {collision['collision_time']}")
    print(f"Collision coordinates: {collision['collision_latitude']}, {collision['collision_longitude']}")
    print(f"Minimum distance nm: {collision['minimum_distance_nm']}")


def write_map(spark: SparkSession, tracks_path: Path, best: dict, map_path: Path) -> None:
    tracks = (
        spark.read.parquet(str(tracks_path))
        .filter(F.col("mmsi").isin([best["mmsi_1"], best["mmsi_2"]]))
        .filter(F.col("timestamp") >= F.to_timestamp(F.lit(best["collision_time"])) - F.expr(f"INTERVAL {VALIDATION_WINDOW_MINUTES} MINUTES"))
        .filter(F.col("timestamp") <= F.to_timestamp(F.lit(best["collision_time"])) + F.expr(f"INTERVAL {VALIDATION_WINDOW_MINUTES} MINUTES"))
        .select("mmsi", "timestamp", "latitude", "longitude", "name")
        .orderBy("mmsi", "timestamp")
        .collect()
    )

    map_object = folium.Map(
        location=[best["collision_latitude"], best["collision_longitude"]],
        zoom_start=12,
        tiles="OpenStreetMap",
    )

    colors = {
        best["mmsi_1"]: "blue",
        best["mmsi_2"]: "red",
    }
    labels = {
        best["mmsi_1"]: f"{best['name_1']} ({best['mmsi_1']})",
        best["mmsi_2"]: f"{best['name_2']} ({best['mmsi_2']})",
    }

    for mmsi in [best["mmsi_1"], best["mmsi_2"]]:
        vessel_rows = [row for row in tracks if row["mmsi"] == mmsi]
        points = [[row["latitude"], row["longitude"]] for row in vessel_rows]

        if points:
            folium.PolyLine(points, color=colors[mmsi], weight=4, tooltip=labels[mmsi]).add_to(map_object)
            folium.Marker(points[0], popup=f"Start<br>{labels[mmsi]}<br>{vessel_rows[0]['timestamp']}", icon=folium.Icon(color=colors[mmsi])).add_to(map_object)
            folium.Marker(points[-1], popup=f"End<br>{labels[mmsi]}<br>{vessel_rows[-1]['timestamp']}", icon=folium.Icon(color=colors[mmsi])).add_to(map_object)

    folium.Marker(
        [best["collision_latitude"], best["collision_longitude"]],
        popup=f"Collision<br>{best['collision_time']}<br>{labels[best['mmsi_1']]}<br>{labels[best['mmsi_2']]}",
        icon=folium.Icon(color="black", icon="warning-sign"),
    ).add_to(map_object)

    legend = f"""
    <div style="position: fixed; bottom: 24px; left: 24px; z-index: 9999; background: white; padding: 10px 12px; border: 1px solid #999; font-size: 13px;">
        <b>Trajectories</b><br>
        <span style="color: blue;">&#9632;</span> {labels[best["mmsi_1"]]}<br>
        <span style="color: red;">&#9632;</span> {labels[best["mmsi_2"]]}<br>
        <span style="color: black;">&#9679;</span> Collision point
    </div>
    """
    map_object.get_root().html.add_child(Element(legend))

    map_object.save(str(map_path))
    print(f"Trajectory map: {map_path}")
