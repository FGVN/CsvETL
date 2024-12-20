namespace CsvETL.Exceptions;

public class MissingFieldsException : Exception
{
    public MissingFieldsException(string message) : base(message)
    {
    }
}
