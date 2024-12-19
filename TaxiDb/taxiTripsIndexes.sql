CREATE INDEX IDX_PULocationID_TipAmount ON TaxiTrips (PULocationID, TipAmount);
CREATE INDEX IDX_TripDistance ON TaxiTrips (TripDistance DESC);
CREATE INDEX IDX_PickupDropoffDuration ON TaxiTrips (PickupDateTime, DropoffDateTime);