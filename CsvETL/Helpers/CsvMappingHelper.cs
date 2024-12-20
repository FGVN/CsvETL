using CsvHelper.Configuration;

namespace CsvETL.Helpers;

public static class CsvMappingHelper
{
    public static ClassMap<T>? CreateMap<T>()
    {
        var columnMappings = AttributeHelper.GetColumnMappings<T>();

        if (columnMappings == null || !columnMappings.Any())
        {
            Console.WriteLine("No column mappings found.");
            return null;
        }

        var map = new DefaultClassMap<T>();
        foreach (var kvp in columnMappings)
        {
            map.Map(typeof(T), typeof(T).GetProperty(kvp.Key)!).Name(kvp.Value);
        }

        return map;
    }

}
