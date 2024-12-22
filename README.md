# CsvETL

## Usage
```cli
dotnet run <csv_file_path> [--connectionString <connection_string>] [--batchSize <batch_size>]
```
### Example Usage
```cli
dotnet run "data/sample-cab-data.csv" --connectionString "Server=myserver;Database=TaxiDb;User Id=myuser;Password=mypassword;" --batchSize 500
```

## The column count from the provided dataset: 29840

# Assumptions made
 - The input data may be randomly sorted.
 - The CSV file may contain additional columns, they are not used in processing and shown in output.
 - The CSV file may not have all columns - then the error is thrown
 - Duplicate records may exist, and faulty records may be present due to data inconsistencies.
 - If records contain errors or are malformed, they will be logged separately into .txt file and not inserted into the database.

# Handling Large Files (10GB and Beyond):
- If the CSV file is extremely large, such as 10GB, the duplicate handling process should be optimized to avoid bloating the RAM with duplicates.
- The program processes the file in batches to ensure memory efficiency.
- The connection handling and fail-safe mechanisms (such as retrying in case of connection loss) should be added to improve reliability.
- The database should be distributed when dealing with such large amounts of data
