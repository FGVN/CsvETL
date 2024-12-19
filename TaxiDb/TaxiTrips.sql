CREATE TABLE TaxiTrips (
    Id INT IDENTITY PRIMARY KEY,
    PickupDateTime DATETIME NOT NULL,
    DropoffDateTime DATETIME NOT NULL,
    PassengerCount INT NOT NULL,
    TripDistance FLOAT NOT NULL,
    StoreAndFwdFlag NVARCHAR(3) NOT NULL,
    PULocationID INT NOT NULL,
    DOLocationID INT NOT NULL,
    FareAmount DECIMAL(10, 2) NOT NULL,
    TipAmount DECIMAL(10, 2) NOT NULL
);
