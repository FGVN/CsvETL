using CsvToDbHelpers;
using System.Data;
using System.Data.SqlClient;

public class BulkInserter
{
    private readonly string _connectionString;

    public BulkInserter(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async Task InsertUsingBulkCopyAsync<T>(IEnumerable<T> data, int batchSize = 1000)
    {
        var tableName = AttributeHelper.GetTableName<T>();
        var columnMappings = AttributeHelper.GetColumnMappings<T>();

        using var connection = new SqlConnection(_connectionString);
        using var bulkCopy = new SqlBulkCopy(connection)
        {
            DestinationTableName = tableName,
            BatchSize = batchSize
        };

        foreach (var mapping in columnMappings)
        {
            bulkCopy.ColumnMappings.Add(mapping.Key, mapping.Value);
        }

        await connection.OpenAsync();

        try
        {
            var batch = new List<T>(batchSize);
            foreach (var item in data)
            {
                batch.Add(item);

                if (batch.Count == batchSize)
                {
                    var dataTable = ConvertToDataTable(batch, columnMappings);
                    await bulkCopy.WriteToServerAsync(dataTable);
                    batch.Clear(); 
                }
            }

            if (batch.Any())
            {
                var dataTable = ConvertToDataTable(batch, columnMappings);
                await bulkCopy.WriteToServerAsync(dataTable);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred: {ex.Message}");
            throw;
        }
    }


    private static DataTable ConvertToDataTable<T>(IEnumerable<T> data, Dictionary<string, string> columnMappings)
    {
        var dataTable = new DataTable();

        foreach (var mapping in columnMappings)
        {
            dataTable.Columns.Add(mapping.Value);
        }

        foreach (var item in data)
        {
            var row = dataTable.NewRow();
            foreach (var mapping in columnMappings)
            {
                var property = typeof(T).GetProperty(mapping.Key);
                row[mapping.Value] = property?.GetValue(item) ?? DBNull.Value;
            }
            dataTable.Rows.Add(row);
        }

        return dataTable;
    }
}
