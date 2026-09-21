CREATE_TRIPS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS trips (
trip_id INTEGER NOT NULL,
start_time TIMESTAMP NOT NULL,
end_time TIMESTAMP NOT NULL,
bikeid INTEGER NOT NULL,
tripduration DECIMAL(16,2) NOT NULL,
from_station_id INTEGER NOT NULL,
from_station_name VARCHAR(100) NOT NULL,
to_station_id INTEGER NOT NULL,
to_station_name VARCHAR(100) NOT NULL,
usertype VARCHAR(20),
gender VARCHAR(6),
birthyear INTEGER,
PRIMARY KEY(trip_id));
"""

CREATE_STATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stations (
id INTEGER NOT NULL,
name VARCHAR(250) NOT NULL,
city VARCHAR(100) NOT NULL,
latitude DECIMAL(9, 6) NOT NULL,
longitude DECIMAL(9, 6) NOT NULL,
dpcapacity INTEGER NOT NULL,
online_date TIMESTAMP NOT NULL,
PRIMARY KEY(id));
"""

COPY_CSV_SQL = """
COPY {} FROM STDIN WITH (FORMAT csv, HEADER true)
"""

COPY_TRIPS_SQL = COPY_CSV_SQL.format("trips")

COPY_STATIONS_SQL = COPY_CSV_SQL.format("stations")

LOCATION_TRAFFIC_SQL_DROP = """
DROP TABLE IF EXISTS station_traffic;
"""
LOCATION_TRAFFIC_SQL_CREATE = """
CREATE TABLE station_traffic AS
    SELECT
    DISTINCT(t.from_station_id) AS station_id,
    t.from_station_name AS station_name,
    num_departures,
    num_arrivals
    FROM trips t
    JOIN (
        SELECT
        from_station_id,
        COUNT(from_station_id) AS num_departures
        FROM trips
        GROUP BY from_station_id
    ) AS fs ON t.from_station_id = fs.from_station_id
    JOIN (
        SELECT
        to_station_id,
        COUNT(to_station_id) AS num_arrivals
        FROM trips
        GROUP BY to_station_id
    ) AS ts ON t.from_station_id = ts.to_station_id
"""
