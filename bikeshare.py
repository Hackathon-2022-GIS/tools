#!/bin/python3
import csv
import glob
import mysql.connector
from mysql.connector.errorcode import ER_DUP_ENTRY

trips_create_sql = """CREATE TABLE IF NOT EXISTS trips (
  ride_id CHAR(16) NOT NULL PRIMARY KEY CLUSTERED,
  rideable_type ENUM('classic_bike','electric_bike','docked_bike') NOT NULL,
  started_at TIMESTAMP NOT NULL,
  ended_at TIMESTAMP NOT NULL,
  start_station_id INT,
  end_station_id INT,
  member_casual ENUM('member','casual') NOT NULL
)
"""

stations_create_sql = """CREATE TABLE IF NOT EXISTS stations (
  station_id INT NOT NULL PRIMARY KEY,
  station_name VARCHAR(200) NOT NULL,
  station_location GEOMETRY NOT NULL
)
"""


def create_schema(c: mysql.connector.connection.MySQLConnection) -> None:
    cur = c.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS bikeshare")
    c.database = "bikeshare"
    cur.execute(trips_create_sql)
    cur.execute(stations_create_sql)
    cur.close()


def load_data(c: mysql.connector.connection.MySQLConnection) -> None:
    # Data is from https://s3.amazonaws.com/capitalbikeshare-data/YYYYMM-capitalbikeshare-tripdata.zip
    # Replace YYYYMM with Year and Month.
    # See also https://docs.pingcap.com/tidb/dev/import-example-data
    # Note that older data files lack coordinates
    csv_glob = "/home/dvaneeden/bikeshare-data/*.csv"
    for csvpath in glob.glob(csv_glob):
        print(f"Loading data from {csvpath}")
        with open(csvpath, newline="") as csvdata:
            reader = csv.DictReader(csvdata)
            rownr = 0
            for row in reader:
                cur = c.cursor()

                # Ignore files with entries that lack lat/lon
                if not "end_lng" in row:
                    break

                # Add start station
                if (
                    len(row["start_station_id"]) > 0
                    and len(row["start_lat"]) > 0
                    and len(row["start_lng"])
                ):
                    wkt = f"POINT({row['start_lng']} {row['start_lat']})"
                    cur.execute(
                        "INSERT IGNORE INTO stations VALUES(%s, %s, ST_GeomFromText(%s))",
                        (row["start_station_id"], row["start_station_name"], wkt),
                    )

                # Add end station
                if (
                    len(row["end_station_id"]) > 0
                    and len(row["end_lat"]) > 0
                    and len(row["end_lng"])
                ):
                    wkt = f"POINT({row['end_lng']} {row['end_lat']})"
                    cur.execute(
                        "INSERT IGNORE INTO stations VALUES(%s, %s, ST_GeomFromText(%s))",
                        (row["end_station_id"], row["end_station_name"], wkt),
                    )

                # Add trip
                cur.execute(
                    "INSERT IGNORE INTO trips VALUES(%s, %s, %s, %s, %s, %s, %s)",
                    (
                        row["ride_id"],
                        row["rideable_type"],
                        row["started_at"],
                        row["ended_at"],
                        row["start_station_id"] or None,
                        row["end_station_id"] or None,
                        row["member_casual"],
                    ),
                )
                c.commit()


if __name__ == "__main__":
    config = {"host": "127.0.0.1", "port": 4000, "user": "root"}
    c = mysql.connector.connect(**config)
    create_schema(c)
    load_data(c)
    c.close()
