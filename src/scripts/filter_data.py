from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config import (
    BROAD_RADIUS_NM,
    CENTER_LATITUDE,
    CENTER_LONGITUDE,
)
from src.scripts.calculate_distance import bounding_box, haversine_nm


INVALID_MMSI_VALUES = ["123456789"]

VALID_MIDS = [
    "201", "202", "203", "204", "205", "206", "207", "208", "209", "210",
    "211", "212", "213", "214", "215", "216", "218", "219", "220", "224",
    "225", "226", "227", "228", "229", "230", "231", "232", "233", "234",
    "235", "236", "237", "238", "239", "240", "241", "242", "243", "244",
    "245", "246", "247", "248", "249", "250", "251", "252", "253", "254",
    "255", "256", "257", "258", "259", "261", "262", "263", "264", "265",
    "266", "267", "268", "269", "270", "271", "272", "273", "274", "275",
    "276", "277", "278", "279", "301", "303", "304", "305", "306", "307",
    "308", "309", "310", "311", "312", "314", "316", "319", "321", "323",
    "325", "327", "329", "330", "331", "332", "334", "336", "338", "339",
    "341", "343", "345", "347", "348", "350", "351", "352", "353", "354",
    "355", "356", "357", "358", "359", "361", "362", "364", "366", "367",
    "368", "369", "370", "371", "372", "373", "374", "375", "376", "377",
    "378", "379", "401", "403", "405", "408", "410", "412", "413", "414",
    "416", "417", "419", "422", "423", "425", "428", "431", "432", "434",
    "436", "437", "438", "440", "441", "443", "445", "447", "450", "451",
    "453", "455", "457", "459", "461", "463", "466", "468", "470", "471",
    "472", "473", "475", "477", "478", "501", "503", "506", "508", "510",
    "511", "512", "514", "515", "516", "518", "520", "523", "525", "529",
    "531", "533", "536", "538", "540", "542", "544", "546", "548", "550",
    "553", "555", "557", "559", "561", "563", "564", "565", "566", "567",
    "570", "572", "574", "576", "577", "578", "601", "603", "605", "607",
    "608", "609", "610", "611", "612", "613", "615", "616", "617", "618",
    "619", "620", "621", "622", "624", "625", "626", "627", "629", "630",
    "631", "632", "633", "634", "635", "636", "637", "638", "642", "644",
    "645", "647", "649", "650", "654", "655", "656", "657", "659", "660",
    "661", "662", "663", "664", "665", "666", "667", "668", "669", "670",
    "671", "672", "674", "675", "676", "677", "678", "679", "701", "710",
    "720", "725", "730", "735", "740", "745", "750", "755", "760", "765",
    "770", "775",
]


def read_filter_daily_csv(spark: SparkSession, csv_path: Path):
    # Reads the raw CSV
    data = spark.read.option("header", True).csv(str(csv_path))

    # Parses the timestamp
    timestamp_text = F.trim(F.col("# Timestamp"))
    parsed_timestamp = F.coalesce(
        F.to_timestamp(timestamp_text, "dd/MM/yyyy HH:mm:ss"),
        F.to_timestamp(timestamp_text, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(timestamp_text, "yyyy-MM-dd'T'HH:mm:ss"),
    )

    min_latitude, max_latitude, min_longitude, max_longitude = bounding_box(
        CENTER_LATITUDE,
        CENTER_LONGITUDE,
        BROAD_RADIUS_NM,
    )

    filtered = (
        data
        # Renames and collects the columns
        .withColumn("mmsi", F.trim(F.col("MMSI")))
        .withColumn("timestamp", parsed_timestamp)
        .withColumn("latitude", F.col("Latitude").cast("double"))
        .withColumn("longitude", F.col("Longitude").cast("double"))
        .withColumn("navigational_status", F.trim(F.col("Navigational status")))
        .withColumn("sog", F.col("SOG").cast("double"))
        .withColumn("cog", F.col("COG").cast("double"))
        .withColumn("heading", F.col("Heading").cast("double"))
        .withColumn("name", F.trim(F.col("Name")))
        .withColumn("ship_type", F.trim(F.col("Ship type")))
        .withColumn("width", F.col("Width").cast("double"))
        .withColumn("length", F.col("Length").cast("double"))
        .withColumn("draught", F.col("Draught").cast("double"))
        # Removes invalid vessel IDs and bad positions
        .filter(F.col("mmsi").rlike("^[0-9]{9}$"))
        .filter(~F.col("mmsi").isin(INVALID_MMSI_VALUES))
        .filter(~F.col("mmsi").rlike(r"^([0-9])\1{8}$"))
        .filter(F.substring(F.col("mmsi"), 1, 3).isin(VALID_MIDS))
        .filter(F.col("timestamp").isNotNull())
        .filter(F.col("latitude").between(-90, 90))
        .filter(F.col("longitude").between(-180, 180))
        # Applies a broad area filter. Applying a broad filter first removes an extremely large portion of the data
        # and makes it easier for future handling
        .filter(F.col("latitude").between(min_latitude, max_latitude))
        .filter(F.col("longitude").between(min_longitude, max_longitude))
        # Applies the exact distance from the starting point
        .withColumn(
            "distance_from_center_nm",
            haversine_nm(
                F.col("latitude"),
                F.col("longitude"),
                F.lit(CENTER_LATITUDE),
                F.lit(CENTER_LONGITUDE),
            ),
        )
        .filter(F.col("distance_from_center_nm") <= BROAD_RADIUS_NM)
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

    return filtered
