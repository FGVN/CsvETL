namespace CsvETL.Models;

public interface ISpecificPreprocessing
{
    Dictionary<string, Func<object, object>> GetPreprocessingRules();
}

