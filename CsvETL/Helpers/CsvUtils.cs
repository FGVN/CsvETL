using CsvETL.Helpers;
using CsvETL.Models;
using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;

public class CsvHelperService
{
    public static async Task CreateCsvFileAsync<T>(string filePath, IEnumerable<T> records)
    {
        using var writer = new StreamWriter(filePath);
        using var csv = new CsvWriter(writer, new CsvConfiguration(CultureInfo.InvariantCulture));
        csv.Context.RegisterClassMap(CsvMappingUtils.CreateMap<T>());
        await csv.WriteRecordsAsync(records);
        Console.WriteLine($"CSV file '{filePath}' has been created.");
    }

    public static async Task ProcessCsvFileAsync(string filePath)
    {
        var uniqueRecords = new HashSet<string>();
        var duplicateRecords = new List<TaxiTrip>();
        var validRecords = new List<TaxiTrip>();
        var faultyRecords = new List<string>();

        var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            MissingFieldFound = null,
            BadDataFound = null,
        };

        using (var reader = new StreamReader(filePath))
        using (var csv = new CsvReader(reader, csvConfig))
        {
            csv.Context.RegisterClassMap(CsvMappingUtils.CreateMap<TaxiTrip>());

            Console.WriteLine("Processing records...");

            while (await csv.ReadAsync())
            {
                try
                {
                    var record = csv.GetRecord<TaxiTrip>();
                    PreprocessingUtils<TaxiTrip>.PreprocessRecord(record);

                    string compositeKey = $"{record.TpepPickupDatetime}-{record.TpepDropoffDatetime}-{record.PassengerCount}";

                    if (!uniqueRecords.Add(compositeKey))
                    {
                        duplicateRecords.Add(record);
                    }
                    else
                    {
                        validRecords.Add(record);
                    }
                }
                catch (Exception ex)
                {
                    faultyRecords.Add($"Error: {ex.Message}, Record: {csv.Parser.RawRecord}");
                }
            }
        }

        if (duplicateRecords.Any())
        {
            Console.WriteLine("Found duplicates. Saving them to 'duplicates.csv'...");
            await CreateCsvFileAsync("duplicates.csv", duplicateRecords);
        }

        if (faultyRecords.Any())
        {
            Console.WriteLine("Found faulty records. Saving them to 'trouble_records.csv'...");
            await File.WriteAllLinesAsync("trouble_records.csv", faultyRecords);
        }

        Console.WriteLine("Processing completed.");
    }

}
