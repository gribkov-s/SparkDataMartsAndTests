
DROP DATABASE IF EXISTS otus;
CREATE DATABASE otus;


CREATE TABLE taxi_trips_by_distance_groups (
distance_group_id INT,
distance_group VARCHAR,
trips_qty INT,
avg_distance DECIMAL(10, 1),
avg_trip_time DECIMAL(10, 1),
avg_amount_per_mile DECIMAL(10, 2),
min_amount DECIMAL(10, 2),
max_amount DECIMAL(10, 2)
);

