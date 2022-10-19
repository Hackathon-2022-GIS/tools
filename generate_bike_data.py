#!/bin/python3
import random
import mysql.connector

bikes_create_sql = """CREATE TABLE IF NOT EXISTS bikes (
  bike_id bigint unsigned PRIMARY KEY AUTO_RANDOM,
  battery_pct tinyint unsigned NOT NULL DEFAULT 0,
  status ENUM('reserved', 'in_use', 'docked', 'broken') NOT NULL DEFAULT 'docked',
  station_id INT COMMENT 'onroute if null')
"""

def populate_bikes(c: mysql.connector.connection.MySQLConnection) -> None:
    cur = c.cursor()
    cur.execute(bikes_create_sql)
    cur.execute('''SELECT station_id FROM stations''')
    stations = [x[0] for x in cur.fetchall()]
    bikecount=1000
    while bikecount:
        battery_pct = random.randint(0, 100)
        status = random.choices(['reserved','in_use','docked','broken'], [0.2, 0.2, 0.8, 0.1])[0]
        station_id=None
        if random.random() > 0.3:
            station_id=random.choice(stations)
        cur.execute("INSERT INTO bikes(battery_pct, status, station_id) VALUES (%s,%s,%s)",
                    (battery_pct, status, station_id))
        bikecount -=1
        if bikecount%100 == 0:
            print(bikecount)
            c.commit()
    c.commit()
    cur.close()

if __name__ == "__main__":
    config = {"host": "127.0.0.1", "port": 4000, "user": "root", "password": "xxxxxxx", "database": "bikeshare"}
    c = mysql.connector.connect(**config)
    populate_bikes(c)
    c.close()
