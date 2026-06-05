import math

from pyspark.sql import functions as F


EARTH_RADIUS_NM = 3440


def haversine_nm(lat1, lon1, lat2, lon2):
    lat1_rad = F.radians(lat1)
    lat2_rad = F.radians(lat2)
    lat_delta = F.radians(lat2 - lat1)
    lon_delta = F.radians(lon2 - lon1)
    a = F.pow(F.sin(lat_delta / 2), 2) + F.cos(lat1_rad) * F.cos(lat2_rad) * F.pow(F.sin(lon_delta / 2), 2)
    return 2 * F.lit(EARTH_RADIUS_NM) * F.asin(F.sqrt(a))


def bounding_box(center_latitude: float, center_longitude: float, radius_nm: float):
    latitude_delta = radius_nm / 60.0
    longitude_delta = radius_nm / (60.0 * math.cos(math.radians(center_latitude)))
    return (
        center_latitude - latitude_delta,
        center_latitude + latitude_delta,
        center_longitude - longitude_delta,
        center_longitude + longitude_delta,
    )


def python_haversine_nm(lat1, lon1, lat2, lon2):
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    lat_delta = math.radians(lat2 - lat1)
    lon_delta = math.radians(lon2 - lon1)
    a = math.sin(lat_delta / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(lon_delta / 2) ** 2
    return 2 * EARTH_RADIUS_NM * math.asin(math.sqrt(a))
