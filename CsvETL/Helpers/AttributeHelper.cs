using CsvETL.Attributes;
using CsvETL.Models;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;

namespace CsvETL.Helpers;

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

    public static Dictionary<string, string> GetColumnMappings<T>()
    {
        return typeof(T).GetProperties()
            .Where(prop => prop.GetCustomAttribute<ColumnAttribute>() != null)
            .ToDictionary(
                prop => prop.Name, 
                prop => prop.GetCustomAttribute<ColumnAttribute>()!.Name! 
            );
    }

    public static string GenerateCompositeKey<T>(T record)
    {
        var compositeKeyParts = typeof(T).GetProperties()
            .Where(prop => Attribute.IsDefined(prop, typeof(CompositePartAttribute)))
            .Select(prop => prop.GetValue(record)?.ToString() ?? string.Empty);

        return string.Join("-", compositeKeyParts);
    }
}

