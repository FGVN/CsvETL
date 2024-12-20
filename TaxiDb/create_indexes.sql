CREATE INDEX IDX_PULocationID_TipAmount ON taxi_trips (pu_location_id, tip_amount);
CREATE INDEX IDX_TripDistance ON taxi_trips (trip_distance DESC);
CREATE INDEX IDX_PickupDropoffDuration ON taxi_trips (tpep_pickup_datetime, tpep_dropoff_datetime);
CREATE INDEX IDX_PULocationID ON taxi_trips (pu_location_id);

