using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;

namespace CsvETL.Helpers;

public class CsvHelperService
{
    public static async Task CreateCsvFileAsync<T>(string filePath, IEnumerable<T> records)
    {
        string absoluteFilePath = Path.GetFullPath(filePath);
        string directory = Path.GetDirectoryName(absoluteFilePath!)!;
        if (!Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory!);
        }
        using var writer = new StreamWriter(absoluteFilePath);
        using var csv = new CsvWriter(writer, new CsvConfiguration(CultureInfo.InvariantCulture));
        csv.Context.RegisterClassMap(CsvMappingHelper.CreateMap<T>()!);
        await csv.WriteRecordsAsync(records);
        Console.WriteLine($"CSV file '{absoluteFilePath}' has been created.");
    }

    public static async Task<List<T>> GetRecords<T>(string filePath)
    {
        var records = new List<T>();

        using var reader = new StreamReader(filePath);
        using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture));
        csv.Context.RegisterClassMap(CsvMappingHelper.CreateMap<T>()!);

        await foreach (var record in csv.GetRecordsAsync<T>())
        {
            records.Add(record);
        }

        return records;
    }
}

