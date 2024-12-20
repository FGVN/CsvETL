using System.Data;
using Microsoft.Data.SqlClient;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;
using CsvETL.Helpers;
using CsvETL.Attributes;

namespace CsvETL.Data;

public class SqlServerService : IDisposable
{
    private readonly string _connectionString;
    private SqlConnection _connection;
    private SqlBulkCopy _bulkCopy;
    private bool _disposed;

    public SqlServerService(string connectionString)
    {
        _connectionString = connectionString;
        _connection = new SqlConnection(_connectionString);
        _bulkCopy = new SqlBulkCopy(_connection)
        {
            BatchSize = ProcessingConfig.DEFAULT_BATCH_SIZE  
        };
        _disposed = false;
    }


    public async Task InsertUsingBulkCopyAsync<T>(IEnumerable<T> data, int batchSize = ProcessingConfig.DEFAULT_BATCH_SIZE)
    {
        var tableName = AttributeHelper.GetTableName<T>();
        var columnMappings = AttributeHelper.GetColumnMappings<T>();

        using var connection = new SqlConnection(_connectionString);
        using var bulkCopy = new SqlBulkCopy(connection)
        {
            DestinationTableName = tableName,
            BatchSize = batchSize
        };

        for (int i = 0; i < columnMappings.Count(); i++)
        {
            bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(i, i + 1));
        }
        //foreach (var mapping in columnMappings)
        //{
        //    bulkCopy.ColumnMappings.Add(mapping.Value, mapping.Key);
        //}

        await connection.OpenAsync();

        var existingRecords = await CheckForExistingRecords(data, connection);

        data = data.Except(existingRecords);

        Console.WriteLine($"Batch had {existingRecords.Count()} same records as the db");

        try
        {
            var batch = new List<T>(batchSize);
            foreach (var item in data)
            {
                batch.Add(item);

                if (batch.Count == batchSize)
                {
                    var dataTable = ConvertToDataTable(data, columnMappings);
                    await bulkCopy.WriteToServerAsync(dataTable);
                    batch.Clear(); 
                }
            }

            if (batch.Any())
            {
                var dataTable = ConvertToDataTable(data, columnMappings);
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

    private async Task<IEnumerable<T>> CheckForExistingRecords<T>(IEnumerable<T> batch, SqlConnection connection)
    {
        var existingRecords = new List<T>();

        var compositeKeyProperties = typeof(T).GetProperties()
            .Where(p => Attribute.IsDefined(p, typeof(CompositePartAttribute)))
            .ToList();

        if (!compositeKeyProperties.Any())
        {
            throw new InvalidOperationException($"No properties marked with {nameof(CompositePartAttribute)} found on type {typeof(T).Name}");
        }

        var batchGroups = batch
            .Select((item, index) => new { item, index })
            .GroupBy(x => x.index / 10) 
            .Select(group => group.Select(x => x.item).ToList())
            .ToList();

        using SqlTransaction transaction = connection.BeginTransaction();

        try
        {
            foreach (var group in batchGroups)
            {
                var whereClauses = new List<string>();
                var parametersList = new List<SqlParameter>();

                for (int i = 0; i < group.Count; i++)
                {
                    var item = group[i];
                    var whereClause = string.Join(" AND ", compositeKeyProperties.Select(p => $"[{p.GetCustomAttribute<ColumnAttribute>()?.Name ?? p.Name}] = @{p.Name}_{i}"));
                    whereClauses.Add($"({whereClause})");

                    var parameters = compositeKeyProperties
                        .Select(p => new SqlParameter($"@{p.Name}_{i}", p.GetValue(item) ?? DBNull.Value))
                        .ToArray();
                    parametersList.AddRange(parameters);
                }

                var query = $"SELECT COUNT(1) FROM [{typeof(T).GetCustomAttribute<TableAttribute>()?.Name ?? typeof(T).Name}] WHERE " + string.Join(" OR ", whereClauses);

                using var command = new SqlCommand(query, connection, transaction);
                command.Parameters.AddRange(parametersList.ToArray());

                var count = (await command.ExecuteScalarAsync()) as int? ?? 0;
                if (count > 0)
                {
                    existingRecords.AddRange(group); 
                }
            }

            await transaction.CommitAsync();
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync();
            Console.WriteLine($"An error occurred during checking for existing records: {ex.Message}");
            throw;
        }

        return existingRecords;
    }



    public void Dispose()
    {
        if (!_disposed)
        {
            _connection?.Dispose();
            _bulkCopy?.Close();
            _disposed = true;
        }
    }

    ~SqlServerService()
    {
        Dispose();
    }
}
