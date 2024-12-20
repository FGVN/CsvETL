using CsvETL.Exceptions;
using CsvETL.Helpers;
using CsvETL.Models;
using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;

namespace CsvETL.Processors;

class Preprocessing
{
    public static async Task ProcessDuplicatesAndFautly<T>(string filePath)
    {
        var processedRecords = new HashSet<string>(); 
        var duplicateRecords = new List<T>();   
        var faultyRecords = new List<string>();        

        var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            MissingFieldFound = null,
            BadDataFound = null,
        };

        using (var reader = new StreamReader(filePath))
        using (var csv = new CsvReader(reader, csvConfig))
        {
            csv.Context.RegisterClassMap(CsvMappingHelper.CreateMap<T>()!);

            Console.WriteLine("Preprocessing records...");

            while (await csv.ReadAsync())
            {
                try
                {
                    var record = csv.GetRecord<T>();
                    PreprocessRecord(record);

                    string compositeKey = AttributeHelper.GenerateCompositeKey(record);

                    if (!processedRecords.Add(compositeKey))
                    {
                        duplicateRecords.Add(record);
                    }

                }
                catch (Exception ex)
                {
                    faultyRecords.Add($"Error: {ex.Message}, Record: {csv.Parser.RawRecord}");
                }
            }

            if (duplicateRecords.Any())
            {
                Console.WriteLine($"Found duplicates. Saving them to '{ProcessingConfig.DUPLICATES_FILENAME}'...");
                await CsvHelperService.CreateCsvFileAsync(ProcessingConfig.DUPLICATES_FILENAME, duplicateRecords);
            }

            if (faultyRecords.Any())
            {
                Console.WriteLine($"Found faulty records. Saving logs to '{ProcessingConfig.FAULTY_FILENAME}'...");
                await File.WriteAllLinesAsync(ProcessingConfig.FAULTY_FILENAME, faultyRecords);
            }
        }

        Console.WriteLine("Preprocessing completed.");
    }


    public static void ValidateCsvHeaders<T>(string filePath)
    {
        Dictionary<string, string> _propertyToColumnMapping = AttributeHelper.GetColumnMappings<T>();
        if (!File.Exists(filePath))
        {
            throw new FileNotFoundException($"The file {filePath} does not exist.");
        }

        using var reader = new StreamReader(filePath);
        string? headerLine = reader.ReadLine();

        if (string.IsNullOrEmpty(headerLine))
        {
            throw new InvalidDataException("The CSV file is empty or does not contain a header row.");
        }

        var fileColumns = headerLine.Split(',').Select(c => c.Trim()).ToList();
        var requiredColumns = _propertyToColumnMapping.Values.ToList();

        var missingFields = requiredColumns.Except(fileColumns).ToList();
        if (missingFields.Any())
        {
            throw new MissingFieldsException($"The following required fields are missing in the CSV file: {string.Join(", ", missingFields)}");
        }

        var extraFields = fileColumns.Except(requiredColumns).ToList();
        if (extraFields.Any())
        {
            Console.WriteLine($"The following extra fields were found in the CSV file: {string.Join(", ", extraFields)}");
        }

        Console.WriteLine("CSV validation passed. All required fields are present.");
    }

    public static void PreprocessRecord<T>(T record)
    {
        if (record is ISpecificPreprocessing preprocessingInstance)
        {
            var rules = preprocessingInstance.GetPreprocessingRules();

            foreach (var rule in rules)
            {
                var property = typeof(T).GetProperty(rule.Key);
                if (property != null)
                {
                    var value = property.GetValue(record);

                    var processedValue = rule.Value(value!);

                    property.SetValue(record, processedValue);
                }
            }
        }
    }
}

