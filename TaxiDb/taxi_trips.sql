CREATE TABLE taxi_trips (
    id INT IDENTITY PRIMARY KEY,
    tpep_pickup_datetime DATETIME NOT NULL,
    tpep_dropoff_datetime DATETIME NOT NULL,
    passenger_count INT NOT NULL,
    trip_distance FLOAT NOT NULL,
    store_and_fwd_flag NVARCHAR(3) NOT NULL,
    pu_location_id INT NOT NULL,
    do_location_id INT NOT NULL,
    fare_amount DECIMAL(10, 2) NOT NULL,
    tip_amount DECIMAL(10, 2) NOT NULL,

    CONSTRAINT UQ_TaxiTrip UNIQUE (
        tpep_pickup_datetime, 
        tpep_dropoff_datetime, 
        passenger_count
    )
);
