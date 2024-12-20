using CsvETL.Models;
using CsvHelper.Configuration;
using CsvHelper;
using System.Globalization;
using System.Data;
using CsvETL.Data;
using CsvETL.Helpers;

namespace CsvETL.Processors;

class Processing
{
    public static async Task ReadBatchAndSave<T>(
        string filePath,
        string connectionString,
        int batchSize = ProcessingConfig.DEFAULT_BATCH_SIZE,
        string duplicatesFilepath = ProcessingConfig.DUPLICATES_FILENAME)
        where T : class, ISpecificPreprocessing, new()
    {
        var savedDups = await CsvHelperService.GetRecords<T>(duplicatesFilepath);
        var duplicates = new HashSet<string>(savedDups.Select(x => AttributeHelper.GenerateCompositeKey(x)));

        var validRecords = new List<T>();
        var faultyRecords = new List<string>();

        var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            MissingFieldFound = null,
            BadDataFound = null,
        };

        using var reader = new StreamReader(filePath);
        using var csv = new CsvReader(reader, csvConfig);
        csv.Context.RegisterClassMap(CsvMappingHelper.CreateMap<T>()!);

        while (csv.Read())
        {
            try
            {
                var record = csv.GetRecord<T>();

                var preprocessingRules = record.GetPreprocessingRules();

                foreach (var rule in preprocessingRules)
                {
                    var property = typeof(T).GetProperty(rule.Key);
                    if (property != null)
                    {
                        var value = property.GetValue(record);
                        property.SetValue(record, rule.Value(value!));
                    }
                }

                var compositeKey = AttributeHelper.GenerateCompositeKey(record);

                if (duplicates.Contains(compositeKey))
                    continue;

                duplicates.Add(compositeKey);
                validRecords.Add(record);

                if (validRecords.Count >= batchSize)
                {
                    await InsertRecordsAsync(validRecords, connectionString, batchSize);
                    validRecords.Clear();
                }
            }
            catch (Exception ex)
            {
                faultyRecords.Add($"Error: {ex.Message}, Record: {csv.Parser.RawRecord}");
            }
        }

        if (validRecords.Any())
        {
            await InsertRecordsAsync(validRecords, connectionString, batchSize);
        }

        Console.WriteLine("Sql load completed.");
    }

    private static async Task InsertRecordsAsync<T>(List<T> records, string connectionString, int batchSize)
    {
        using var inserter = new SqlServerService(connectionString);
        await inserter.InsertUsingBulkCopyAsync(records, batchSize);
    }
}