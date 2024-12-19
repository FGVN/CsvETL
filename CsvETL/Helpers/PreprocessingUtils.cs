using CsvETL.Models;
using CsvToDbHelpers;

namespace CsvETL.Helpers;

internal class PreprocessingUtils<T> where T : class
{

    private readonly Dictionary<string, string?> _propertyToColumnMapping;

    public PreprocessingUtils()
    {
        _propertyToColumnMapping = AttributeHelper.GetColumnMappings<T>();
    }

    public void ValidateCsvHeaders(string filePath)
    {
        if (!File.Exists(filePath))
        {
            throw new FileNotFoundException($"The file {filePath} does not exist.");
        }

        using var reader = new StreamReader(filePath);
        string headerLine = reader.ReadLine();

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

    /// <summary>
    /// Preprocesses the record (e.g., trims text fields, converts store_and_fwd_flag).
    /// </summary>
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

                    var processedValue = rule.Value(value);

                    property.SetValue(record, processedValue);
                }
            }
        }
    }
}
