WITH CTE AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY PickupDateTime, DropoffDateTime, PassengerCount ORDER BY Id) AS RowNum
    FROM TaxiTrips
)
DELETE FROM CTE
WHERE RowNum > 1;
