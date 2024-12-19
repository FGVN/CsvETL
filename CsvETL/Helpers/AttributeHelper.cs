using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;

namespace CsvToDbHelpers;

public static class AttributeHelper
{
    public static string GetTableName<T>()
    {
        var tableAttribute = typeof(T).GetCustomAttribute<TableAttribute>();
        if (tableAttribute == null)
        {
            throw new InvalidOperationException($"Type {typeof(T).Name} does not have a TableAttribute defined.");
        }
        return tableAttribute.Name;
    }

    public static Dictionary<string, string?> GetColumnMappings<T>()
    {
        return typeof(T).GetProperties()
            .Where(prop => prop.GetCustomAttribute<ColumnAttribute>() != null)
            .ToDictionary(
                prop => prop.Name, 
                prop => prop.GetCustomAttribute<ColumnAttribute>()?.Name 
            );
    }

    public static void ValidateCsvHeaders<T>(IEnumerable<string> csvHeaders)
    {
        var columnMappings = GetColumnMappings<T>();
        var missingColumns = columnMappings.Values.Except(csvHeaders, StringComparer.OrdinalIgnoreCase).ToList();
        var extraColumns = csvHeaders.Except(columnMappings.Values, StringComparer.OrdinalIgnoreCase).ToList();

        if (missingColumns.Any())
        {
            throw new InvalidOperationException($"The following columns are missing in the CSV file: {string.Join(", ", missingColumns)}");
        }

        if (extraColumns.Any())
        {
            Console.WriteLine($"Warning: The following extra columns are present in the CSV file but are not mapped: {string.Join(", ", extraColumns)}");
        }
    }
}

