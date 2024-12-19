using CsvETL.Models;
using CsvETL.Helpers;

class Program
{
    static async Task Main(string[] args)
    {
        try
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Usage: dotnet run <csv_file_path>");
                return;
            }

            string filePath = args[0];

            PreprocessingUtils<TaxiTrip>.PreprocessRecord(filePath);

            await CsvHelperService.ProcessCsvFileAsync(filePath);
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
