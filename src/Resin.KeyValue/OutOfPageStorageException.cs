namespace Resin.KeyValue
{
    public class OutOfPageStorageException : Exception
    {
        public OutOfPageStorageException() : base()
        {
        }

        public OutOfPageStorageException(string? message) : base(message)
        {
        }
    }
}
