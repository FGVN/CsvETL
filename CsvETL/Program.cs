using CsvETL.Models;
using CsvETL.Processors;
using CsvETL.Exceptions;

class Program
{
    static async Task Main(string[] args)
    {
        try
        {

            string connectionString = "Server=localhost;Database=TaxiDb;Integrated Security=True;TrustServerCertificate=True;";
            int batchSize = ProcessingConfig.DEFAULT_BATCH_SIZE;

            if (args.Length == 0)
            {
                Console.WriteLine("Usage: dotnet run <csv_file_path> [--connectionString <connection_string>] [--batchSize <batch_size>]");
                return;
            }

            for (int i = 1; i < args.Length; i++)
            {
                if (args[i] == "--connectionString" && i + 1 < args.Length)
                {
                    connectionString = args[i + 1]; 
                    i++;  
                }
                else if (args[i] == "--batchSize" && i + 1 < args.Length && int.TryParse(args[i + 1], out int parsedBatchSize))
                {
                    batchSize = parsedBatchSize;  
                    i++;  
                }
            }


            string filePath = args[0];

            Preprocessing.ValidateCsvHeaders<TaxiTrip>(filePath);

            await Preprocessing.ProcessDuplicatesAndFautly<TaxiTrip>(filePath);

            await Processing.ReadBatchAndSave<TaxiTrip>(filePath, connectionString, batchSize);
        }
        catch (MissingFieldsException ex)
        {
            Console.WriteLine($"Validation failed: {ex.Message}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred: {ex.Message}");
        }
    }
}
