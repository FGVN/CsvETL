using System.ComponentModel.DataAnnotations.Schema;

namespace CsvETL.Models;

[Table("TaxiTrips")]
public class TaxiTrip : ISpecificPreprocessing
{
    [Column("tpep_pickup_datetime")]
    public DateTime TpepPickupDatetime { get; set; }

    [Column("tpep_dropoff_datetime")]
    public DateTime TpepDropoffDatetime { get; set; }

    [Column("passenger_count")]
    public int PassengerCount { get; set; }

    [Column("trip_distance")]
    public decimal TripDistance { get; set; }

    [Column("store_and_fwd_flag")]
    public string StoreAndFwdFlag { get; set; }

    [Column("PULocationID")]
    public int PULocationID { get; set; }

    [Column("DOLocationID")]
    public int DOLocationID { get; set; }

    [Column("fare_amount")]
    public decimal FareAmount { get; set; }

    [Column("tip_amount")]
    public decimal TipAmount { get; set; }

    public Dictionary<string, Func<object, object>> GetPreprocessingRules()
    {
        return new Dictionary<string, Func<object, object>>
        {
            { nameof(StoreAndFwdFlag), value =>
                value is string str ? (str.Trim().ToUpper() == "Y" ? "Yes" : str.Trim().ToUpper() == "N" ? "No" : str) : value },

            { nameof(TpepPickupDatetime), value =>
                value is DateTime date ? TimeZoneInfo.ConvertTimeToUtc(date, TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time")) : value },
            { nameof(TpepDropoffDatetime), value =>
                value is DateTime date ? TimeZoneInfo.ConvertTimeToUtc(date, TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time")) : value }
        };
    }
}

